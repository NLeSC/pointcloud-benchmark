#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging, math, numpy
from pointcloud import dbops, monetdbops, lasops, morton
from pointcloud.AbstractLoader import AbstractLoader as ALoader
from pointcloud.monetdb.CommonMonetDB import CommonMonetDB

MAX_FILES = 1999

class LoaderBinary(ALoader, CommonMonetDB):
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        ALoader.__init__(self, configuration)
        self.setVariables(configuration)
    
    def initialize(self):
        if self.partitioning and not self.imprints:
            raise Exception('Partitioning without imprints is not supported!')        

        if self.createDB:
            logging.info('Creating DB ' + self.dbName)
            # Drop previous DB if exist and create a new one
            os.system('monetdb stop ' + self.dbName)
            os.system('monetdb destroy ' + self.dbName + ' -f')
            os.system('monetdb create ' + self.dbName)
            os.system('monetdb release ' + self.dbName)
        
            connection = self.getConnection()
            cursor = connection.cursor()
            
#            monetdbops.mogrifyExecute(cursor, """CREATE FUNCTION GetX(morton BIGINT, scaleX DOUBLE, globalOffset BIGINT) RETURNS DOUBLE external name geom."GetX";""")
#            monetdbops.mogrifyExecute(cursor, """CREATE FUNCTION GetY(morton BIGINT, scaleY DOUBLE, globalOffset BIGINT) RETURNS DOUBLE external name geom."GetY";""")
        
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)        
        (self.inputFiles, inputFilesBoundingCube, _, _, boundingCube, scales) = lasops.getPCFolderDetails(self.inputFolder, numProc = self.numProcessesLoad)
        (self.minX, self.minY, self.minZ, self.maxX, self.maxY, self.maxZ) = boundingCube
        (self.scaleX, self.scaleY, _) = scales
        
        if not self.imprints:
            # If we want to create a final indexed table we need to put the 
            # points in a temporal table
            self.tempFlatTable = 'TEMP_' + self.flatTable
            ftName = self.tempFlatTable
        else:
            ftName = self.flatTable
        
        connection = self.getConnection()
        cursor = connection.cursor()
        if self.partitioning:
            # Get initial number of tiles in X and Y
            rangeX = self.maxX - self.minX
            rangeY = self.maxY - self.minY
            # We scale the number of tiles in each dimension according to the ration between the axis extents
            nX = int(math.ceil(math.sqrt(self.numPartitions) * (float(rangeX)/float(rangeY))))
            nY = int(math.ceil(math.sqrt(self.numPartitions) * (float(rangeY)/float(rangeX))))
            
            # Previous nX*nY will for sure be higher than desired number of partitions
            # Now we refine nX and nY to have a number of partitions as close to the desrired by the user
            correctNP = False
            if nX >= nY:
                modX = True
            else:
                modX = False
            while not correctNP:
                nP = nX * nY
                if nP == self.numPartitions:
                    correctNP = True
                elif nP > self.numPartitions:
                    if modX:
                        nX = nX - 1
                        modX = False
                    else:
                        nY = nY - 1
                        modX = True
                else:
                    correctNP = True
                    if modX:
                        nY = nY + 1
                    else:
                        nX = nX + 1
                    
            
            self.tilesFiles = {}
            for i in range(len(self.inputFiles)):
                (fminX, fminY, _, fmaxX, fmaxY, _) = inputFilesBoundingCube[i]
                pX = fminX + ((fmaxX - fminX) / 2.)
                pY = fminY + ((fmaxY - fminY) / 2.)
                m = morton.EncodeMorton2D(*self.getTileIndex(pX, pY, self.minX, self.minY, self.maxX, self.maxY, nX, nY))
                if m not in self.tilesFiles:
                    self.tilesFiles[m] = []
                self.tilesFiles[m].append(self.inputFiles[i])
            
            logging.info('Real number of partitions is ' + str(len(self.tilesFiles)))
            
            monetdbops.mogrifyExecute(cursor, "CREATE MERGE TABLE " + ftName + " (" + (', '.join(self.getDBColumns())) + ")")
            for m in sorted(self.tilesFiles):
                monetdbops.mogrifyExecute(cursor, "CREATE TABLE " + ftName + str(m) + " (" + (', '.join(self.getDBColumns())) + ")")
        else:
            monetdbops.mogrifyExecute(cursor, "CREATE TABLE " + ftName + " (" + (', '.join(self.getDBColumns())) + ")")
                
        #  Create the meta-data table
        monetdbops.mogrifyExecute(cursor, "CREATE TABLE " + self.metaTable + " (tablename text, srid integer, minx DOUBLE PRECISION, miny DOUBLE PRECISION, maxx DOUBLE PRECISION, maxy DOUBLE PRECISION, scalex DOUBLE PRECISION, scaley DOUBLE PRECISION)")
        # Close connection
        connection.close()    
        
    def getTileIndex(self, pX, pY, minX, minY, maxX, maxY, axisTilesX, axisTilesY):
        xpos = int((pX - minX) * axisTilesX / (maxX - minX))
        ypos = int((pY - minY) * axisTilesY / (maxY - minY))
        if xpos == axisTilesX: # If it is in the edge of the box (in the maximum side) we need to put in the last tile
            xpos -= 1
        if ypos == axisTilesY:
            ypos -= 1
        return (xpos, ypos)

    def getDBColumns(self):
        cols = []
        for c in self.columns:
            if c not in self.DM_FLAT:
                raise Exception('Wrong column!' + c)
            cols.append(self.DM_FLAT[c][0] + ' ' + self.DM_FLAT[c][1])
        return cols
    
    def close(self):
        # Restart DB to flush data in memory to disk
        logging.info('Restarting DB')
        os.system('monetdb stop ' + self.dbName)
        os.system('monetdb start ' + self.dbName)
        
        connection = self.getConnection()
        cursor = connection.cursor()
        
        if not self.imprints:
            ftName = self.tempFlatTable
        else:
            ftName = self.flatTable
        
        # We set the table to read only
        if self.partitioning:
            for m in sorted(self.tilesFiles):
                partitionName = ftName + str(m)
                monetdbops.mogrifyExecute(cursor, "alter table " + partitionName + " set read only")
        else:
            monetdbops.mogrifyExecute(cursor, "alter table " + ftName + " set read only")
        
        if self.imprints:
            if self.partitioning:
                # Create imprints index
                logging.info('Creating imprints for different partitions and columns')
                for m in sorted(self.tilesFiles):
                    partitionName = ftName + str(m)
                    for c in self.columns:
                        colName = self.DM_FLAT[c][0]
                        if c == 'x':
                            minDim = self.minX - 10
                            maxDim = self.minX - 9
                        elif c == 'y':
                            minDim = self.minY - 10
                            maxDim = self.minY - 9
                        elif c == 'z':
                            minDim = self.minZ - 10
                            maxDim = self.minZ - 9
                        else:
                            minDim = 0
                            maxDim = 1
                        monetdbops.mogrifyExecute(cursor, "select " + colName + " from " + partitionName + " where " + colName + " between %s and %s", [minDim, maxDim])
                    monetdbops.mogrifyExecute(cursor, "analyze sys." + partitionName + " (" + dbops.getSelectCols(self.columns, self.DM_FLAT) + ") minmax")
            else:
                logging.info('Creating imprints')
                for c in self.columns:
                    colName = self.DM_FLAT[c][0]
                    if c == 'x':
                        minDim = self.minX - 10
                        maxDim = self.minX - 9
                    elif c == 'y':
                        minDim = self.minY - 10
                        maxDim = self.minY - 9
                    elif c == 'z':
                        minDim = self.minZ - 10
                        maxDim = self.minZ - 9
                    else:
                        minDim = 0
                        maxDim = 1
                    monetdbops.mogrifyExecute(cursor, "select " + colName + " from " + self.flatTable + " where " + colName + " between %s and %s", [minDim, maxDim])
                monetdbops.mogrifyExecute(cursor, "analyze sys." + self.flatTable + " (" + dbops.getSelectCols(self.columns, self.DM_FLAT) + ") minmax")
        else:
            if self.partitioning:
                for m in sorted(self.tilesFiles):
                    partitionName = ftName + str(m)
                    newPartitionName = self.flatTable + str(m)
                    monetdbops.mogrifyExecute(cursor, 'CREATE TABLE ' + newPartitionName + ' AS SELECT * FROM ' + partitionName + ' ORDER BY ' + dbops.getSelectCols(self.index, self.DM_FLAT) + ' WITH DATA')
                    monetdbops.mogrifyExecute(cursor, "alter table " + newPartitionName + " set read only")
                    monetdbops.mogrifyExecute(cursor, 'DROP TABLE ' + partitionName)
            else:
                monetdbops.mogrifyExecute(cursor, 'CREATE TABLE ' + self.flatTable + ' AS SELECT * FROM ' + self.tempFlatTable + ' ORDER BY ' + dbops.getSelectCols(self.index, self.DM_FLAT) + ' WITH DATA')
                monetdbops.mogrifyExecute(cursor, "alter table " + self.flatTable + " set read only")
                monetdbops.mogrifyExecute(cursor, 'DROP TABLE ' + self.tempFlatTable)
            
        if self.partitioning:
            for m in sorted(self.tilesFiles):
                partitionName = self.flatTable + str(m)
                monetdbops.mogrifyExecute(cursor, "ALTER TABLE " + self.flatTable + " add table " + partitionName)
        
        connection.close()
 
    def size(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        sizes = monetdbops.getSizes(cursor)
        cursor.close()
        connection.close()
        
        for i in range(len(sizes)):
            if sizes[i] != None:
                sizes[i] = '%.3f MB' % sizes[i]
        (size_indexes, size_ex_indexes, size_total) = sizes
        return ' Size indexes= ' + str(size_indexes) + '. Size excluding indexes= ' + str(size_ex_indexes) + '. Size total= ' + str(size_total)
    
    def getNumPoints(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute('select count(*) from ' + self.flatTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n
    
    def process(self):
        logging.info('Starting data loading (parallel by las2col) from ' + self.inputFolder + ' to ' + self.dbName)
        return self.processSingle([self.inputFolder, ], self.processInputFolder)
    
    def processInputFolder(self, index, inputFolder):
        # Create connection
        connection = self.getConnection()
        cursor = connection.cursor()    
        
        # Add the meta-data to the meta table
        metaArgs = (self.flatTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        monetdbops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" , metaArgs)

        l2colCols = []
        for c in self.columns:
            l2colCols.append(self.DM_LAS2COL[c])

        if self.partitioning:
            for m in sorted(self.tilesFiles):
                # Split the list of input files in bunches of maximum MAX_FILES files
                inputFilesLists = numpy.array_split(self.tilesFiles[m], int(math.ceil(float(len(self.tilesFiles[m]))/float(MAX_FILES))))
                for i in range(len(inputFilesLists)):
                    # Create the file with the list of PC files
                    listFile =  self.tempDir + '/' + str(m) + '_' + str(i) + '_listFile'
                    outputFile = open(listFile, 'w')
                    for f in inputFilesLists[i]:
                        outputFile.write(f + '\n')
                    outputFile.close()
                    
                    # Generate the command for the NLeSC Binary converter
                    inputArg = '-f ' + listFile
                    tempFile =  self.tempDir + '/' + str(m) + '_' + str(i) + '_tempFile'    
                    c = 'las2col ' + inputArg + ' ' + tempFile + ' --parse ' + ''.join(l2colCols) + ' --num_read_threads ' + str(self.numProcessesLoad)
                    if 'k' in self.columns:
                        c += ' --moffset ' + str(int(self.minX / self.scaleX)) + ','+ str(int(self.minY / self.scaleY)) + ' --check ' + str(self.scaleX) + ',' + str(self.scaleY)
                    # Execute the converter
                    logging.info(c)
                    os.system(c)
                
                    #The different binary files have a pre-defined name 
                    bs = []
                    for col in l2colCols:
                        bs.append("'" + tempFile + "_col_" + col + ".dat'")
                    
                    if not self.imprints:
                        ftName = self.tempFlatTable
                    else:
                        ftName = self.flatTable
                        
                    monetdbops.mogrifyExecute(cursor, "COPY BINARY INTO " + ftName + str(m) + " from (" + ','.join(bs) + ")")
        else:     
    
            # Split the list of input files in bunches of maximum MAX_FILES files
            inputFilesLists = numpy.array_split(self.inputFiles, int(math.ceil(float(len(self.inputFiles))/float(MAX_FILES))))
            
            for i in range(len(inputFilesLists)):
                # Create the file with the list of PC files
                listFile =  self.tempDir + '/' + str(i) + '_listFile'
                outputFile = open(listFile, 'w')
                for f in inputFilesLists[i]:
                    outputFile.write(f + '\n')
                outputFile.close()
                
                # Generate the command for the NLeSC Binary converter
                inputArg = '-f ' + listFile
                tempFile =  self.tempDir + '/' + str(i) + '_tempFile'    
                c = 'las2col ' + inputArg + ' ' + tempFile + ' --parse ' + ''.join(l2colCols) + ' --num_read_threads ' + str(self.numProcessesLoad)
                if 'k' in self.columns:
                    c += ' --moffset ' + str(int(self.minX / self.scaleX)) + ','+ str(int(self.minY / self.scaleY)) + ' --check ' + str(self.scaleX) + ',' + str(self.scaleY)
                # Execute the converter
                logging.info(c)
                os.system(c)
                
                # The different binary files have a pre-defined name 
                bs = []
                for col in l2colCols:
                    bs.append("'" + tempFile + "_col_" + col + ".dat'")
                
                if not self.imprints:
                    ftName = self.tempFlatTable
                else:
                    ftName = self.flatTable
                
                monetdbops.mogrifyExecute(cursor, "COPY BINARY INTO " + ftName + " from (" + ','.join(bs) + ")")
        connection.close()
