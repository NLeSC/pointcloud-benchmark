#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud.oracle.blocks.incremental.LoaderInc import LoaderInc
from pointcloud import oracleops

class LoaderIncOrdered(LoaderInc):
    def getFileBlockTable(self, index):
        return self.blockTable + '_' + str(index)

    def process(self):   
        logging.info('Starting data loading in parallel by python from ' + self.inputFolder + ' to ' + self.userName)     
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile, self.loadFromFileSequential, True)

    def loadFromFile(self,  index, fileAbsPath):
        blockTable = self.getFileBlockTable(index)
        connection = self.getConnection()
        cursor = connection.cursor()
        # We create a temporal blocks table without baseTable and in the work table space
        self.createBlocksTable(cursor, blockTable, self.workTableSpace, self.compression, None)
        connection.close()
        self.loadInc(fileAbsPath, 1, blockTable, self.blockSeq, self.blockSize, self.batchSize)
        
    def loadFromFileSequential(self, fileAbsPath, index, numFiles):
        fileBlockTable = self.getFileBlockTable(index)
        connection = self.getConnection()
        cursor = connection.cursor()
        oracleops.mogrifyExecute(cursor, "INSERT INTO " + self.blockTable + " SELECT * FROM " + fileBlockTable)
        oracleops.mogrifyExecute(cursor, "DROP TABLE " + fileBlockTable)
        connection.close()
