#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from pointcloud import lasops, oracleops
from pointcloud.oracle.AbstractLoader import AbstractLoader

class LoaderInc(AbstractLoader):
    def initialize(self):
        # Creates the user that will store the tables
        if self.columns != 'xyz':
            raise Exception('ERROR: This loader only currently accepts XYZ!. First you need to change the JAVA incremental loader')
        
        if self.cUser:
            self.createUser()
        
        # Get the point cloud folder description
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, self.srid, _, self.minX, self.minY, _, self.maxX, self.maxY, _, _, _, _) = lasops.getPCFolderDetails(self.inputFolder)
        
        # Creates connection
        connection = self.getConnection()
        cursor = connection.cursor()
        
        # Create blocks table and base table
        self.createBlocksTable(cursor, self.blockTable, self.tableSpace, self.compression, self.baseTable)
        
        self.blockSeq = self.blockTable + '_ID_SEQ'
        oracleops.mogrifyExecute(cursor, "CREATE SEQUENCE " + self.blockSeq)
        self.initCreatePC(cursor, self.srid, self.minX, self.minY, self.maxX, self.maxY, None, self.blockTable, self.baseTable, self.blockSize, self.tolerance, self.workTableSpace, False)
        connection.close()

    def process(self):
        logging.info('Starting data loading in parallel by python from ' + self.inputFolder + ' to ' + self.userName)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile)
        
    def loadFromFile(self,  index, fileAbsPath):
        self.loadInc(fileAbsPath, 1, self.blockTable, self.blockSeq, self.blockSize, self.batchSize)

    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.updateBlocksSRID(cursor, self.blockTable, self.srid)
        self.createBlockIndex(cursor, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.blockTable, self.indexTableSpace, self.workTableSpace, self.numProcessesLoad)
        connection.close()
        
    def size(self):
        return self.sizeBlocks(self.blockTable, self.baseTable)
        
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)