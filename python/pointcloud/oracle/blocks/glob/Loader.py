#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud import lasops
from pointcloud.oracle.AbstractLoader import AbstractLoader

# LAS2TXT + SQLLOADER all files to staging table. Then, create blocks globally with R-Tree blocking

class Loader(AbstractLoader):
    def initialize(self):
        # Check parameters for this loader
        if self.numProcessesLoad > 1:
            raise Exception('ERROR: multi-core is not supported!')
        
        if self.blockMethod == 'hilbert':
            raise Exception('ERROR: Hilbert blocking is not supported!')
        
        if self.cUser:
            self.createUser()
        
        # Get the point cloud folder description
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, _, _, _, boundingCube, _) = lasops.getPCFolderDetails(self.inputFolder, numProc = self.numProcessesLoad)
        (self.minX, self.minY, _, self.maxX, self.maxY, _) = boundingCube
        
        # Creates connection
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.flatTable = self.blockTable + '_STAGING'
        
        # Create the flat table
        self.createFlatTable(cursor, self.flatTable, self.columns)
        # Create blocks table and base table
        self.createBlocksTable(cursor, self.blockTable, self.tableSpace, self.compression, self.baseTable)
        
        connection.close()

    def process(self):
        logging.info('Starting data loading sequentially from ' + self.inputFolder + ' to ' + self.userName)
        return self.processSingle(self.inputFiles, self.loadFromFile)

    def loadFromFile(self,  index, fileAbsPath):
        self.las2txt_sqlldr(fileAbsPath, self.flatTable, self.columns)

    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.populateBlocks(cursor, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.flatTable, self.blockTable, self.baseTable, self.blockSize, self.columns, self.tolerance, self.workTableSpace)
        self.createBlockIdIndex(cursor, self.blockTable, self.indexTableSpace)
        connection.close()
        
    def size(self):
        return self.sizeBlocks(self.blockTable, self.baseTable)
        
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)