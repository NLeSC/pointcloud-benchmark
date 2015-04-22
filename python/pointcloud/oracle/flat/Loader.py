#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud import oracleops, lasops
from pointcloud.oracle.AbstractLoader import AbstractLoader

class Loader(AbstractLoader):
    def initialize(self):
        if self.cUser:
            self.createUser()
        
        # Get the point cloud folder description
        logging.info('Getting files, extent, scale and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, _, _, self.minX, self.minY, _, self.maxX, self.maxY, _, self.scaleX, self.scaleY, _) = lasops.getPCFolderDetails(self.inputFolder)
        
        # Creates connection
        connection = self.getConnection()
        cursor = connection.cursor()
        
        # Create the flat table
        self.createFlatTable(cursor, self.flatTable, self.columns)
        self.createFlatMeta(cursor, self.metaTable)
        connection.close()
        
    def process(self):
        logging.info('Starting data loading sequentially from ' + self.inputFolder + ' to ' + self.userName)
        return self.processSingle(self.inputFiles, self.loadFromFile)
    
    def loadFromFile(self,  index, fileAbsPath):
        self.las2txt_sqlldr(fileAbsPath, self.flatTable, self.columns)

    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        metaArgs = (self.flatTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        oracleops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (:1,:2,:3,:4,:5,:6,:7,:8)" , metaArgs)
        if self.flatTableIOT:
            tempFlatTable = self.flatTable + '_TEMP'
            oracleops.mogrifyExecute(cursor, "ALTER TABLE " + self.flatTable + " RENAME TO " + tempFlatTable )
            self.createIOTTable(cursor, self.flatTable, tempFlatTable, self.tableSpace, self.columns, self.columns, self.index, self.numProcessesLoad)
            oracleops.dropTable(cursor, tempFlatTable, False)
        else:
            self.createIndex(cursor, self.flatTable, self.index, self.indexTableSpace, self.numProcessesLoad)
        connection.close()
        
    def size(self):
        return self.sizeFlat(self.flatTable)

    def getNumPoints(self):
        return self.getNumPointsFlat(self.flatTable)
