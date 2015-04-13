#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
import utils
from pointcloud.oracle.AbstractLoader import AbstractLoader
from pointcloud import lasops, pdalops

class Loader(AbstractLoader):
    def initialize(self):
        # Creates the user that will store the tables
        if self.cUser:
            self.createUser()
        
        # Get the point cloud folder description
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, self.srid, _, self.minX, self.minY, _, self.maxX, self.maxY, _, _, _, _) = lasops.getPCFolderDetails(self.inputFolder)
        
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
        xmlFile = pdalops.OracleWriter(fileAbsPath, self.getConnectionString(), self.columns, self.blockTable, self.baseTable, self.srid, self.blockSize)
        c = 'pdal pipeline ' + xmlFile # + ' -d -v 6'
        logging.info(c)
        os.system(c)
        # remove the XML file
        os.system('rm ' + xmlFile)

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