#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os
from pointcloud.oracle.AbstractLoader import AbstractLoader

class LoaderExt(AbstractLoader):
    def initialize(self):
        if self.partition != 'none':
            raise Exception('ERROR: partitions are not supported!')
        
        if 'k' in self.columns or 'k' in self.index:
            raise Exception('ERROR: LoaderExt not compatible with Morton codes')

        if os.path.isfile(self.inputFolder):
            parentFolder = os.path.abspath(os.path.join(self.inputFolder,'..'))
            lasFiles = os.path.basename(self.inputFolder)
        else:
            parentFolder = self.inputFolder
            lasFiles = '*.' + self.extension

        self.createUser()

        self.createLASDirectory(parentFolder)

        connection = self.connect()
        cursor = connection.cursor()
        
        self.createExternal(cursor, lasFiles, self.extTable, self.columns)
        self.dropTable(cursor, self.flatTable, True)

        connection.close()

    def process(self):
        # We only execute one item, i.e. the input folder
        return self.processSingle([self.inputFolder,], self.loadInputFolder)

    def loadInputFolder(self,  index, inputFolder):
        connection = self.connect()
        cursor = connection.cursor()
        
        icols = self.columns
        hfactor = None
        if self.clusterhilbert:
            hfactor = self.hilbertFactor
            icols += 'h'
        
        if self.cluster:
            if self.cluster2Step:
                tempFlatTable = self.flatTable + '_TEMP' 
                self.createTableAsSelect(cursor, tempFlatTable, self.extTable, icols, False)
                self.createIOT(cursor, self.flatTable, tempFlatTable, icols, icols, self.index, self.clusterDistinct, False, hfactor)
                self.dropTable(cursor, tempFlatTable)
            else:
                self.createIOT(cursor, self.flatTable, self.extTable, icols, icols, self.index, self.clusterDistinct, False, hfactor)
        else:
            self.createTableAsSelect(cursor, self.flatTable, self.extTable, icols, False)
        
        #self.computeStatistics(cursor, self.flatTable)
        connection.close()

    def close(self):
        connection = self.connect()
        cursor = connection.cursor()
        
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
