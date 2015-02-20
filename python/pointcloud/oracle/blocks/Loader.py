#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud import utils
from pointcloud.oracle.AbstractLoader import AbstractLoader

class Loader(AbstractLoader):
    def initialize(self):
        # Check parameters for this loader
        if self.partition != 'none':
            raise Exception('ERROR: partitions are not supported!')
        
        if self.numProcessesLoad > 1:
            raise Exception('ERROR: multi-core is not supported!')
        
        if self.cluster:
            raise Exception('ERROR: clustering is not supported!')
        
        if self.blockMethod == 'hilbert':
            raise Exception('ERROR: only rtree blocking supported with this loader')
        
        self.createUser()
        
        connection = self.connect()
        cursor = connection.cursor()
        self.createFlat(cursor, self.flatTable)
        self.createBlocks(cursor, self.blockTable, self.baseTable)
        connection.close()
        
        logging.info( 'Files are loaded sequentially...')

    def process(self):
        inputFiles = utils.getFiles(self.inputFolder, self.extension)[self.fileOffset:]
        return self.processSingle(inputFiles, self.loadFromFile)

    def loadFromFile(self,  index, fileAbsPath):
        self.loadToFlat(fileAbsPath, self.flatTable)

    def close(self):
        connection = self.connect()
        cursor = connection.cursor()
        self.populateBlocks(cursor)
        self.createBlockIdIndex(cursor)
        #self.computeStatistics(cursor, self.baseTable)
        #self.computeStatistics(cursor, self.blockTable)
        connection.close()
        
    def size(self):
        return self.sizeBlocks()
        
    def getNumPoints(self):
        return self.getNumPointsBlocks()