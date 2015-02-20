#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging, math, numpy
from pointcloud import utils
from pointcloud.AbstractLoader import AbstractLoader as ALoader
from pointcloud.monetdb.CommonMonetDB import CommonMonetDB
from pointcloud import dbops

TIME_FORMAT = "%Y_%m_%d_%H_%M_%S"

class LoaderBinary(ALoader, CommonMonetDB):
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        ALoader.__init__(self, configuration)
        self.setVariables(configuration)
        self.numPartitions = 0
        self.useLOF = True
    
    def connect(self, superUser = False):
        return self.connection()     
    
    def initialize(self):
        if self.numProcessesLoad > 1:
            raise Exception('Parallel loading is done automatically in las2col!')
        if not self.imprints and self.partitioning:
            raise Exception('Partitioning without imprints is not supported!')
            
        if self.cDB:
            os.system('monetdb stop ' + self.dbName)
            os.system('monetdb destroy ' + self.dbName + ' -f')
            os.system('monetdb create ' + self.dbName)
            os.system('monetdb release ' + self.dbName)
        
        connection = self.connect()
        cursor = connection.cursor()
        
        if not self.imprints:
            self.tempFlatTable = 'TEMP_' + self.flatTable
            ftName = self.tempFlatTable
        else:
            ftName = self.flatTable
        
        if self.partitioning:
            self.mogrifyExecute(cursor, "CREATE MERGE TABLE " + ftName + " (" + (', '.join(self.getFlatTableCols())) + ")")
        else:
            self.mogrifyExecute(cursor, "CREATE TABLE " + ftName + " (" + (', '.join(self.getFlatTableCols())) + ")")
        
        connection.commit()
        connection.close()    
    
    def getFlatTableCols(self):
        cols = []
        for c in self.columns:
            if c not in self.colsData:
                raise Exception('Wrong column!' + c)
            cols.append(self.colsData[c][0] + ' ' + self.colsData[c][1])
        return cols
    
    def close(self):
        # Restart DB to flush data in memory to disk
        if self.restartDB:
            logging.info('Restarting DB...')
            os.system('monetdb stop ' + self.dbName)
            os.system('monetdb start ' + self.dbName)
        
        connection = self.connect()
        cursor = connection.cursor()
        
        if not self.imprints:
            ftName = self.tempFlatTable
        else:
            ftName = self.flatTable
            
        if self.partitioning:
            for i in range(self.numPartitions):
                partitionName = ftName + str(i)
                self.mogrifyExecute(cursor, "alter table " + partitionName + " set read only")
                connection.commit()
        else:
            self.mogrifyExecute(cursor, "alter table " + ftName + " set read only")
            connection.commit()
        
        if self.imprints:
            if self.partitioning:
                # Create imprints index by sample query in center of extent
                logging.info('Creating imprints for different partitions and columns...')
                for c in self.columns:
                    colName = self.colsData[c][0]
                    for i in range(self.numPartitions):
                        partitionName = self.flatTable + str(i)
                        self.mogrifyExecute(cursor, "select " + colName + " from " + partitionName + " where " + colName + " between 0 and 1")
                        connection.commit()
                #TODO create 2 processes, one for x and one for y
            else:
                logging.info('Creating imprints...')
                query = "select * from " + self.flatTable + " where x between 0 and 1 and y between 0 and 1"
                self.mogrifyExecute(cursor, query)
                connection.commit()
                
        else:
            if self.partitioning:
                for i in range(self.numPartitions):
                    partitionName = self.tempFlatTable + str(i)
                    newPartitionName = self.flatTable + str(i)
                    self.mogrifyExecute(cursor, 'CREATE TABLE ' + newPartitionName + ' AS SELECT * FROM ' + partitionName + ' ORDER BY ' + dbops.getSelectCols(self.index, self.colsData) + ' WITH DATA')
                    self.mogrifyExecute(cursor, "alter table " + newPartitionName + " set read only")
                    self.mogrifyExecute(cursor, 'DROP TABLE ' + partitionName)
                    connection.commit()
            else:
                self.mogrifyExecute(cursor, 'CREATE TABLE ' + self.flatTable + ' AS SELECT * FROM ' + self.tempFlatTable + ' ORDER BY ' + dbops.getSelectCols(self.index, self.colsData) + ' WITH DATA')
                self.mogrifyExecute(cursor, "alter table " + self.flatTable + " set read only")
                self.mogrifyExecute(cursor, 'DROP TABLE ' + self.tempFlatTable)
                connection.commit()
            
        if self.partitioning:
            for i in range(self.numPartitions):
                partitionName = ftName + str(i)
                self.mogrifyExecute(cursor, "ALTER TABLE " + ftName + " add table " + partitionName)
                connection.commit()
        
        connection.close()
 
    def size(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute("""select cast(sum(imprints) AS double)/(1024.*1024.), cast(sum(columnsize) as double)/(1024.*1024.), (cast(sum(imprints) AS double)/(1024.*1024.) + cast(sum(columnsize) as double)/(1024.*1024.)) from storage()""")
        row = list(cursor.fetchone())
        for i in range(len(row)):
            if row[i] != None:
                row[i] = '%.3f MB' % row[i]
        (size_indexes, size_ex_indexes, size_total) = row
        cursor.close()
        connection.close()
        #size_total = '%.2f MB' % (float(os.popen('du -sm ' + self.dbFarm + '/' + self.dbName).read().split('\t')[0]))
        return ' Size indexes= ' + str(size_indexes) + '. Size excluding indexes= ' + str(size_ex_indexes) + '. Size total= ' + str(size_total)
    
    def getNumPoints(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute('select count(*) from ' + self.flatTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n
    
    def process(self):
        return self.processSingle([self.inputFolder, ], self.processInputFolder)
    
    def processInputFolder(self, index, inputFolder):
#        os.chdir(inputFolder)
#        inputFiles = glob.glob('*' + self.extension)       
        inputFiles = utils.getFiles(inputFolder, self.extension)[self.fileOffset:]
        # Split the list of input files in bunches of maximum MAX_NUM_FILES files
        if self.maxFiles > 0:
            inputFilesLists = numpy.array_split(inputFiles, int(math.ceil(float(len(inputFiles))/float(self.maxFiles))))
        else:
            inputFilesLists = [inputFiles, ]
        
        connection = self.connect()
        cursor = connection.cursor()    
        for i in range(len(inputFilesLists)):
            tempFile =  self.tempDir + '/' + str(i) + '_tempFile'
            
            if self.useLOF:
                listFile =  self.tempDir + '/' + str(i) + '_listFile'
                outputFile = open(listFile, 'w')
                for f in inputFilesLists[i]:
                    outputFile.write(f + '\n')
                outputFile.close()
                inputArg = '-f ' + listFile
            else:
                inputArg = '-i ' + (' -i '.join(inputFilesLists[i]))
                
            c = 'las2col ' + inputArg + ' ' + tempFile + ' --parse ' + self.columns
            if 'k' in self.columns:
                c += ' --moffset ' + str(int(float(self.mortonGlobalOffsetX) / float(self.mortonScaleX))) + ','+ str(int(float(self.mortonGlobalOffsetY) / float(self.mortonScaleY))) + ' --check ' + str(self.mortonScaleX) + ',' + str(self.mortonScaleY)
            
            logging.info(c)
            os.system(c)
            
            bs = []
            for col in self.columns:
                bs.append("'" + tempFile + "_col_" + col + ".dat'")
            
            if not self.imprints:
                ftName = self.tempFlatTable
            else:
                ftName = self.flatTable
            
            if self.partitioning:
                partitionName = ftName + str(i)
                self.mogrifyExecute(cursor, """CREATE TABLE """ + partitionName + """ (""" + (',\n'.join(self.getFlatTableCols())) + """)""")
                self.mogrifyExecute(cursor, "COPY BINARY INTO " + partitionName + " from (" + ','.join(bs) + ")")
                self.numPartitions += 1
            else:
                self.mogrifyExecute(cursor, "COPY BINARY INTO " + ftName + " from (" + ','.join(bs) + ")")
    
        connection.commit()
        connection.close()
