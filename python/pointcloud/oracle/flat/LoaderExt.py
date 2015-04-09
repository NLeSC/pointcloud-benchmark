#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os
from pointcloud.oracle.AbstractLoader import AbstractLoader
from pointcloud import utils, oracleops

class LoaderExt(AbstractLoader):
    def initialize(self):
        if self.partition != 'none':
            raise Exception('ERROR: partitions are not supported!')
        
        if 'k' in self.columns or 'k' in self.index:
            raise Exception('ERROR: LoaderExt not compatible with Morton codes')

        (self.inputFiles, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY) = self.getPCDescription(self.inputFolder)

        if os.path.isfile(self.inputFolder):
            parentFolder = os.path.abspath(os.path.join(self.inputFolder,'..'))
            lasFiles = os.path.basename(self.inputFolder)
        else:
            parentFolder = self.inputFolder
            if len(self.inputFiles) == 0:
                raise Exception('ERROR: None PC file in ' + self.inputFolder)
            lasFiles = '*.' + self.inputFiles.split('.')[-1]

        self.createUser()

        self.createLASDirectory(parentFolder)

        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.createExternal(cursor, lasFiles, self.extTable, self.columns)
        oracleops.dropTable(cursor, self.flatTable, True)

        self.createFlatMeta(cursor, self.metaTable)
    
        connection.close()

    def process(self):
        
         # We only execute one item, i.e. the input folder
        return self.processSingle([self.inputFolder,], self.loadInputFolder)

    def loadInputFolder(self,  index, inputFolder):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        icols = self.columns
        hfactor = None
        if self.clusterhilbert:
            hfactor = self.hilbertFactor
            icols += 'h'
        
        if self.cluster:
            self.createIOT(cursor, self.flatTable, self.extTable, self.tableSpace, icols, icols, self.index, False, hfactor)
        else:
            self.createTableAsSelect(cursor, self.flatTable, self.extTable, icols, False)
        
        #self.computeStatistics(cursor, self.flatTable)
        connection.close()

    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        metaArgs = (self.flatTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        oracleops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" , metaArgs)
        
        if not (self.cluster and not self.clusterhilbert):
            # We need to create index if we do not use IOT or if we use IOT with Hilbert clustering
            if self.index != 'false':    
                self.createIndex(cursor, self.flatTable, self.index)
        
        #self.computeStatistics(cursor, self.flatTable)
        connection.close()
        
    def size(self):
        return self.sizeFlat()
        
    def getNumPoints(self):
        return self.getNumPointsFlat()
