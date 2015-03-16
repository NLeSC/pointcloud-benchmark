#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
import utils
import pdal_xml
from pointcloud.postgres.AbstractLoader import AbstractLoader

class LoaderOrdered(AbstractLoader):
    def initialize(self):
        self.ordered = True
        
        # Creates the DB and loads pointCloud extension
        self.createDB()
        self.initPointCloud()
        # Creates the table that will contain ALL pathces
        self.createBlocks(self.blockTable)   
    
    def getFileBlockTable(self, index):
        return self.blockTable + '_' + str(index)
    
    def loadFromFile(self, index, fileAbsPath):
        # Get information of the contents of the LAS file
        logging.debug(fileAbsPath)
        fileBlockTable = self.getFileBlockTable(index)
        self.createBlocks(fileBlockTable)  
        (dimensionsNames, pcid, compression, offsets, scales) = self.addPCFormat(self.schemaFile, fileAbsPath)
        xmlFile = pdal_xml.PostgreSQLWriter(fileAbsPath, self.connectString(), pcid, dimensionsNames, fileBlockTable, self.srid, self.blockSize, compression, offsets, scales)
        c = 'pdal pipeline ' + xmlFile
        logging.debug(c)
        os.system(c)
        # remove the XML file
        os.system('rm ' + xmlFile)
        
    def loadFromFileSequential(self, fileAbsPath, index, numFiles):
        fileBlockTable = self.getFileBlockTable(index)
        connection = self.connect()
        cursor = connection.cursor()
        #query = "INSERT INTO " + self.blockTable + " (pa) SELECT pa FROM " + fileBlockTable
        query = "INSERT INTO " + self.blockTable + " (pa) SELECT pa FROM " + fileBlockTable + " ORDER BY id"
        self.mogrifyExecute(cursor, query)
        connection.commit()
        self.mogrifyExecute(cursor, "DROP TABLE " + fileBlockTable)
        connection.commit()

    def close(self):
        self.indexClusterVacuumBlock(self.blockTable)
        
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)
