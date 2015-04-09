#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud import utils, oracleops
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

        self.createUser()
        
        (self.inputFiles, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY) = self.getPCDescription(self.inputFolder)
        
        connection = self.getConnection()
        cursor = connection.cursor()
        self.createFlat(cursor, self.flatTable)
        self.createFlatMeta(cursor, self.metaTable)
        
        connection.close()
        
        logging.info( 'Files are loaded sequentially...')
    
    def process(self):
        return self.processSingle(self.inputFiles, self.loadFromFile)
    
    def loadFromFile(self,  index, fileAbsPath):
        self.loadToFlat(fileAbsPath, self.flatTable)

    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        metaArgs = (self.flatTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        oracleops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" , metaArgs)

        if self.cluster:
            tempFlatTable = self.flatTable + '_TEMP'
            oracleops.mogrifyExecute(cursor, "ALTER TABLE " + self.flatTable + " RENAME TO " + tempFlatTable )
            connection.commit()
            self.createIOT(cursor, self.flatTable, tempFlatTable, self.tableSpace, self.columns, self.columns, self.index)
            
            oracleops.dropTable(cursor, tempFlatTable, False)
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
