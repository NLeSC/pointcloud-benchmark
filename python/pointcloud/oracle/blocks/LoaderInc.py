#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging, numpy, math, os
from pointcloud import utils
from pointcloud.oracle.AbstractLoader import AbstractLoader

class LoaderInc(AbstractLoader):
    def initialize(self):
        # Check parameters for this loader
        if self.partition != 'none':
            raise Exception('ERROR: partitions are not supported!')
        
        if self.cluster:
            raise Exception('ERROR: clustering is not supported!')
        
        #if self.numProcessesLoad != 1:
        #    raise Exception('ERROR: single process is allowed!')
        
        
        # Creates the user that will store the tables
        self.createUser()
        
        if self.cUser:
            connection = self.connect()
            cursor = connection.cursor()
            
            # Creates the global blocks tables
            self.createBlocks(cursor, self.blockTable, self.baseTable)
            self.blockSeq = self.blockTable + '_ID_SEQ'
            cursor.execute("create sequence " + self.blockSeq )
            connection.commit()
            self.initCreatePC(cursor, create = False)
            connection.commit() 
            connection.close()

    def process(self):
        inputFiles = utils.getFiles(self.inputFolder)
        
        if self.chunkSize > 0:
            inputFilesLists = numpy.array_split(inputFiles, int(math.ceil(float(len(inputFiles))/float(self.chunkSize))))
        else:
            inputFilesLists = [inputFiles, ]
        
        resultsGlobal = []
        for i in range(len(inputFilesLists)):
            if i != 0:
                os.system('sleep ' + str(self.pause))
            logging.info('Loading chunk ' + str(i+1) + ' of ' + str(len(inputFilesLists)))
            resultsGlobal.extend(self.processMulti(inputFilesLists[i], self.numProcessesLoad, self.loadFromFile))
        return resultsGlobal
        
    def loadFromFile(self,  index, fileAbsPath):
        self.loadInc(fileAbsPath, 1, self.blockTable, self.blockSeq)

    def close(self):
        connection = self.connect()
        cursor = connection.cursor()
        self.mogrifyExecute(cursor, "update " + self.blockTable + " b set b.blk_extent.sdo_srid = " + str(self.srid))
        self.createBlockIndex(cursor)
        connection.close()
        
    def size(self):
        return self.sizeBlocks()
        
    def getNumPoints(self):
        return self.getNumPointsBlocks()
