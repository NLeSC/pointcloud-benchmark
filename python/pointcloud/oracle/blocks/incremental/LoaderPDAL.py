#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging, time
from pointcloud.oracle.AbstractLoader import AbstractLoader
from pointcloud import lasops, pdalops, utils

class LoaderPDAL(AbstractLoader):
    def initialize(self):
        # Creates the user that will store the tables
        if self.cUser:
            self.createUser()
        
        # Get the point cloud folder description
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, _, _, self.minX, self.minY, _, self.maxX, self.maxY, _, _, _, _) = lasops.getPCFolderDetails(self.inputFolder, numProc = self.numProcessesLoad)
        
        # Creates connection
        connection = self.getConnection()
        cursor = connection.cursor()
        
        # Create blocks table and base table (for PDAL blocks we need the blockID as well in the blocks table)
        self.createBlocksTable(cursor, self.blockTable, self.tableSpace, self.compression, self.baseTable, includeBlockId = True)
        connection.close()

    def process(self):
        logging.info('Starting data loading in parallel by python from ' + self.inputFolder + ' to ' + self.userName)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile, None, True)

    def loadFromFile(self, index, fileAbsPath):
        # Get information of the contents of the LAS file
        logging.info(fileAbsPath)
        xmlFile = os.path.basename(fileAbsPath) + '.xml'
        if self.columns == 'all':
            pdalCols = None
        else:
            pdalCols = []
            for c in self.columns:
                pdalCols.append(self.DM_PDAL[c])
                
        useOffsetScale = self.useOffsetScale
        pdalops.OracleWriter(xmlFile, fileAbsPath, self.getConnectionString(), pdalCols, self.blockTable, self.baseTable, self.srid, self.blockSize, self.pdalCompression, self.pdalDimOrientation, useOffsetScale)
        t0 = time.time()
        pdalops.executePDAL(xmlFile)
        print 'LOADSTATS', os.path.basename(fileAbsPath), lasops.getPCFileDetails(fileAbsPath)[1], time.time() - t0
        
    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.createBlockIdIndex(cursor, self.blockTable, self.indexTableSpace)
        self.createBlockIndex(cursor, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.blockTable, self.indexTableSpace, self.workTableSpace, self.numProcessesLoad)
        connection.close()
        
    def size(self):
        return self.sizeBlocks(self.blockTable, self.baseTable)
        
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)
