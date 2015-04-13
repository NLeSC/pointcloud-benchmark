#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
import utils
from pointcloud import pdalops, postgresops
from pointcloud.postgres.blocks.Loader import Loader

class LoaderOrdered(Loader):
    def getFileBlockTable(self, index):
        return self.blockTable + '_' + str(index)

    def process(self):
        logging.info('Starting ordered data loading with PDAL (parallel by python) from ' + self.inputFolder + ' to ' + self.dbName)
        return self.processMulti(inputFiles, self.numProcessesLoad, self.loadFromFile, self.loadFromFileSequential, True)

    def loadFromFile(self, index, fileAbsPath):
        # Add point cloud format to poinctcloud_formats table
        fileBlockTable = self.getFileBlockTable(index)
        self.createBlocksTable(fileBlockTable, self.indexTableSpace) # We use the index table space for the temporal table
        
        (dimensionsNames, pcid, compression) = self.addPCFormat(self.schemaFile, fileAbsPath)
        xmlFile = pdalops.PostgreSQLWriter(fileAbsPath, self.getConnectionString(), pcid, dimensionsNames, fileBlockTable, self.srid, self.blockSize, compression)
        c = 'pdal pipeline ' + xmlFile
        logging.debug(c)
        os.system(c)
        # remove the XML file
        os.system('rm ' + xmlFile)
        
    def loadFromFileSequential(self, fileAbsPath, index, numFiles):
        fileBlockTable = self.getFileBlockTable(index)
        connection = self.getConnection()
        cursor = connection.cursor()
        #query = "INSERT INTO " + self.blockTable + " (pa) SELECT pa FROM " + fileBlockTable
        query = "INSERT INTO " + self.blockTable + " (pa) SELECT pa FROM " + fileBlockTable + " ORDER BY id"
        postgresops.mogrifyExecute(cursor, query)
        postgresops.mogrifyExecute(cursor, "DROP TABLE " + fileBlockTable)