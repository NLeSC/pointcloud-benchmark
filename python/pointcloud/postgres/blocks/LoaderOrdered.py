#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud import pdalops, postgresops
from pointcloud.postgres.blocks.Loader import Loader

class LoaderOrdered(Loader):
    def getFileBlockTable(self, index):
        return self.blockTable + '_' + str(index)

    def process(self):
        logging.info('Starting ordered data loading with PDAL (parallel by python) from ' + self.inputFolder + ' to ' + self.dbName)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile, self.loadFromFileSequential, True)

    def loadFromFile(self, index, fileAbsPath):
        # Get connection
        connection = self.getConnection()
        cursor = connection.cursor()
        #Create a temporal blocks table for the blocks of the current file
        fileBlockTable = self.getFileBlockTable(index)
        self.createBlocksTable(cursor, fileBlockTable, self.indexTableSpace) # We use the index table space for the temporal table
        
        # Add point cloud format to poinctcloud_formats table
        (dimensionsNames, pcid, compression) = self.addPCFormat(cursor, self.schemaFile, fileAbsPath, self.srid)
        connection.close()
        # Get PDAL config and run PDAL
        xmlFile = pdalops.PostgreSQLWriter(fileAbsPath, self.getConnectionString(), pcid, dimensionsNames, fileBlockTable, self.srid, self.blockSize, compression)
        pdalops.executePDAL(xmlFile)
        
    def loadFromFileSequential(self, fileAbsPath, index, numFiles):
        fileBlockTable = self.getFileBlockTable(index)
        connection = self.getConnection()
        cursor = connection.cursor()
        # Insert the blocks on the global blocks table (with correct order)
        query = "INSERT INTO " + self.blockTable + " (pa) SELECT pa FROM " + fileBlockTable + " ORDER BY id"
        postgresops.mogrifyExecute(cursor, query)
        # Drop the temporal table
        postgresops.dropTable(cursor, fileBlockTable)
        connection.close()
