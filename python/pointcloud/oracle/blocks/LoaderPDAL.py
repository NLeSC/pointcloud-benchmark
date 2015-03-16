#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
import utils
import pdal_xml
from pointcloud.oracle.AbstractLoader import AbstractLoader

class Loader(AbstractLoader):
    def initialize(self):
        # Check parameters for this loader
        if self.partition != 'none':
            raise Exception('ERROR: partitions are not supported!')
        
        if self.cluster:
            raise Exception('ERROR: clustering is not supported!')
        
        #if self.numProcessesLoad != 1:
        #    raise Exception('ERROR: single process is allowed!')
        
        
        # Creates the user that will store the tables
        self.createUser()
        
        if self.cUser:
            connection = self.connect()
            cursor = connection.cursor()
            
            # Creates the global blocks tables
            self.createBlocks(cursor, self.blockTable, self.baseTable, self.tableSpace, includeBlockId = True)
            connection.close()
        
    def loadFromFile(self, index, fileAbsPath):
        # Get information of the contents of the LAS file
        logging.debug(fileAbsPath)
        
        #(self.dimensionsNames, pcid, compression, offsets, scales) = self.addPCFormat(self.schemaFile, fileAbsPath)
        (_, _, _, _, _, _, _, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ) = utils.getLASParams(fileAbsPath, tool = self.las2txtTool)  
        offsets = {'X': offsetX, 'Y': offsetY, 'Z': offsetZ}
        scales = {'X': scaleX, 'Y': scaleY, 'Z': scaleZ}
        xmlFile = pdal_xml.OracleWriter(fileAbsPath, self.connectString(), self.dimensionsNames, self.blockTable, self.baseTable, self.srid, self.blockSize, offsets, scales)
        c = 'pdal pipeline ' + xmlFile + ' -d -v 6'
        logging.debug(c)
        os.system(c)
        # remove the XML file
        os.system('rm ' + xmlFile)

    def close(self):
        connection = self.connect()
        cursor = connection.cursor()
        #self.mogrifyExecute(cursor, "update " + self.blockTable + " b set b.blk_extent.sdo_srid = " + str(self.srid))
        self.createBlockIdIndex(cursor)
        self.createBlockIndex(cursor)
        connection.close()
        
    def size(self):
        return self.sizeBlocks()
        
    def getNumPoints(self):
        return self.getNumPointsBlocks()
