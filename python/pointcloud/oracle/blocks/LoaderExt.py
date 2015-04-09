#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os
from pointcloud.oracle.AbstractLoader import AbstractLoader
from pointcloud import utils

class LoaderExt(AbstractLoader):
    def initialize(self):
        # Check parameters for this loader
        if self.cluster:
            if self.partition != 'none':
                raise Exception('Partitioning with IOT is not supported!')
        
        if self.partition != 'none' and not self.isHilbertBlockMethod:
            raise Exception('Partitioning only supported with Hilbert blocking!')
        
        if self.partition.count('hash-'):
            self.numPartitions = int(self.partition.replace('hash-',''))
        elif self.partition != 'none':
            self.ranges = self.partition.split(',')
        
        if os.path.isfile(self.inputFolder):
            parentFolder = os.path.abspath(os.path.join(self.inputFolder,'..'))
            lasFiles = os.path.basename(self.inputFolder)
        else:
            parentFolder = self.inputFolder
            inputFiles = utils.getFiles(self.inputFolder)
            if len(inputFiles) == 0:
                raise Exception('ERROR: None PC file in ' + self.inputFolder)
            lasFiles = '*.' + inputFiles.split('.')[-1]
        
        self.createUser()
        
        self.createLASDirectory(parentFolder)

        connection = self.connect()
        cursor = connection.cursor()
        
        icols = ['x','y','z']
        if self.clusterhilbert or self.isHilbertBlockMethod:
            icols.append('h')
        
        self.createExternal(cursor, lasFiles, self.extTable, icols)
        self.dropTable(cursor, self.flatTable, True)

        self.createBlocks(cursor, self.blockTable, self.baseTable)
        connection.close()

    def process(self):
        # We only execute one item, i.e. the input folder
        return self.processSingle([self.inputFolder,], self.loadInputFolder)
    
    def loadInputFolder(self, index, inputFolder):
        connection = self.connect()
        cursor = connection.cursor()
        
        icols = ['x','y','z']
        ocols = icols[:]
        kcols = icols[:]
        hfactor = None
        if self.clusterhilbert or self.isHilbertBlockMethod:
            hfactor = self.hilbertFactor
            icols.append('h')
            ocols.append('h')
            kcols = ['h',]
        
        if self.cluster:
            if self.cluster2Step:
                tempFlatTable = self.flatTable + '_TEMP' 
                self.createTableAsSelect(cursor, tempFlatTable, self.extTable, icols, False)
                self.createIOT(cursor, self.flatTable, tempFlatTable, self.indexTableSpace, ocols, ocols, kcols, self.clusterDistinct, False, hfactor)
                self.dropTable(cursor, tempFlatTable)
            else:
                self.createIOT(cursor, self.flatTable, self.extTable, self.indexTableSpace, icols, ocols, kcols, self.clusterDistinct, False, hfactor)         
        else:
            if self.partition == 'none':
                self.createTableAsSelect(cursor, self.flatTable, self.extTable, icols, False)
#            else:
#                # This is only possible if isHilbert (checking in initialize)
#                if self.partition.count('hash-'):
#                    self.createPopulateFlatExtHash(cursor, self.numPartitions)
#                else: # Range partitioning
#                    self.createPopulateFlatExtRange(cursor, self.ranges)
        connection.close()

    def close(self):
        connection = self.connect()
        cursor = connection.cursor()
        
        if self.isHilbertBlockMethod:
            if not self.cluster:
                self.createIndex(cursor, self.flatTable, 'h', partitioned = (self.partition != 'none'))
            # Create blocks and index
            self.populateBlocksHilbert(cursor)
            self.createBlockIndex(cursor)
        else: #Rtree blocking
            self.populateBlocks(cursor)
            self.createBlockIdIndex(cursor)
            
        #self.computeStatistics(cursor, self.baseTable)
        #self.computeStatistics(cursor, self.blockTable)

        connection.close()
        
    def size(self):
        return self.sizeBlocks()
    def getNumPoints(self):
        return self.getNumPointsBlocks()