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
        
        if self.clusterhilbert:
            raise Exception('ERROR: hilbert clustering not supported!')
        
        if not self.cluster2Step:
            raise Exception('ERROR: clustering is always in two steps!')
        
        self.createUser()
        
        connection = self.connect()
        self.createFlat(connection.cursor(), self.flatTable)
        connection.close()
        
        logging.info( 'Files are loaded sequentially...')
    
    def process(self):
        inputFiles = utils.getFiles(self.inputFolder)
        return self.processSingle(inputFiles, self.loadFromFile)
    
    def loadFromFile(self,  index, fileAbsPath):
        self.loadToFlat(fileAbsPath, self.flatTable)

    def close(self):
        connection = self.connect()
        cursor = connection.cursor()
        if self.cluster:
            tempFlatTable = self.flatTable + '_TEMP'
            self.mogrifyExecute(cursor, "ALTER TABLE " + self.flatTable + " RENAME TO " + tempFlatTable )
            connection.commit()
            self.createIOT(cursor, self.flatTable, tempFlatTable, self.tableSpace, self.columns, self.columns, self.index, self.clusterDistinct)
            
            self.dropTable(cursor, tempFlatTable, False)
            connection.commit()
        else:
            if self.index != 'false':    
                self.createIndex(cursor, self.flatTable, self.index)
        #self.computeStatistics(cursor, self.flatTable)
        connection.close()
        
    def size(self):
        return self.sizeFlat()

    def getNumPoints(self):
        return self.getNumPointsFlat()
