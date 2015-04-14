#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud import pdalops, postgresops, utils, lasops
from pointcloud.postgres.AbstractLoader import AbstractLoader

class Loader(AbstractLoader):
    def initialize(self):
        # Initialize DB and extensions if creation of user is required
        if self.cDB:
            self.createDB()
            self.initPointCloud()
        # Create the blocks table 
        self.createBlocksTable(self.blockTable, self.tableSpace)
        
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, self.srid, _, self.minX, self.minY, _, self.maxX, self.maxY, _, self.scaleX, self.scaleY, _) = lasops.getPCFolderDetails(self.inputFolder)
        
    def process(self):
        logging.info('Starting data loading with PDAL (parallel by python) from ' + self.inputFolder + ' to ' + self.dbName)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile)
        
    def loadFromFile(self, index, fileAbsPath):
        # Add poitn cloud format to poinctcloud_formats table
        (dimensionsNames, pcid, compression) = self.addPCFormat(self.schemaFile, fileAbsPath, self.srid)
        xmlFile = pdalops.PostgreSQLWriter(fileAbsPath, self.getConnectionString(), pcid, dimensionsNames, self.blockTable, self.srid, self.blockSize, compression)
        c = 'pdal pipeline ' + xmlFile
        logging.info(c)
        os.system(c)
        # remove the XML file
        os.system('rm ' + xmlFile)

    def close(self):
        self.indexBlockTable(self.blockTable, self.indexTableSpace, False, self.cluster)    
    
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)
