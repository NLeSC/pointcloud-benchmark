#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
import utils
import pdal_xml
from pointcloud.postgres.AbstractLoader import AbstractLoader

class Loader(AbstractLoader):
    def initialize(self):
        self.createDB()
        self.initPointCloud()
        self.createBlocks(self.blockTable)
        
    def loadFromFile(self, index, fileAbsPath):
        # Get information of the contents of the LAS file
        logging.debug(fileAbsPath)
        
        (self.dimensionsNames, pcid, compression, offsets, scales) = self.addPCFormat(self.schemaFile, fileAbsPath)  
        xmlFile = pdal_xml.PostgreSQLWriter(fileAbsPath, self.connectString(), pcid, self.dimensionsNames, self.blockTable, self.srid, self.blockSize, compression, offsets, scales)
        c = 'pdal pipeline ' + xmlFile
        logging.debug(c)
        os.system(c)
        # remove the XML file
        os.system('rm ' + xmlFile)

    def close(self):
        self.indexClusterVacuumBlock(self.blockTable)    
    
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)
