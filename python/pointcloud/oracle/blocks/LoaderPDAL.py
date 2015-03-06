#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud import utils
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
            self.blockSeq = self.blockTable + '_ID_SEQ'
            cursor.execute("create sequence " + self.blockSeq )
            connection.commit()
            #self.initCreatePC(cursor, create = False)
            connection.commit() 
            connection.close()
        
    def loadFromFile(self, index, fileAbsPath):
        # Get information of the contents of the LAS file
        logging.debug(fileAbsPath)
        
        #(self.dimensionsNames, pcid, compression, offsets, scales) = self.addPCFormat(self.schemaFile, fileAbsPath)  
        xmlFile = self.createPDALXML(fileAbsPath, self.connectString(), pcid, self.dimensionsNames, self.blockTable, self.baseTable, self.srid, self.blockSize, offsets, scales)
        c = 'pdal pipeline ' + xmlFile
        logging.debug(c)
        os.system(c)
        # remove the XML file
        os.system('rm ' + xmlFile)

    def close(self):
        connection = self.connect()
        cursor = connection.cursor()
        #self.mogrifyExecute(cursor, "update " + self.blockTable + " b set b.blk_extent.sdo_srid = " + str(self.srid))
        self.createBlockIndex(cursor)
        connection.close()
        
    def size(self):
        return self.sizeBlocks()
        
    def getNumPoints(self):
        return self.getNumPointsBlocks()
