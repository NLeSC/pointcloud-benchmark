#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud.postgres.AbstractLoader import AbstractLoader
from pointcloud import postgresops, lasops

class LoaderBinary(AbstractLoader):
    def initialize(self):
        # Initialize DB and extensions if creation of user is required
        if self.cDB:
            self.createDB()
        # Get connection
        connection = self.getConnection()
        cursor = connection.cursor()
        # Create flat table
        self.createFlatTable(cursor, self.flatTable, self.tableSpace, self.columns)
        
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, _, _, _, boundingCube, scales) = lasops.getPCFolderDetails(self.inputFolder, numProc = self.numProcessesLoad)
        (self.minX, self.minY, _, self.maxX, self.maxY, _) = boundingCube
        (self.scaleX, self.scaleY, _) = scales
        
        # Create meta table to save the extent of the PC
        self.createMetaTable(cursor, self.metaTable)
        connection.close()
        
    def process(self):
        logging.info('Starting data loading with las2pg (parallel by python) from ' + self.inputFolder + ' to ' + self.dbName)
        # Insert the extent of the loaded PC
        connection = self.getConnection()
        cursor = connection.cursor()
        metaArgs = (self.flatTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        postgresops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" , metaArgs)
        connection.close()
        # Start the multiprocessing (las2pg in parallel)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile)
     
    def loadFromFile(self, index, fileAbsPath):
        self.loadFromBinaryLoader(self.getConnectionString(False, True), self.flatTable, fileAbsPath, self.columns, self.minX, self.minY, self.scaleX, self.scaleY)
 
    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.indexFlatTable(cursor, self.flatTable, self.indexTableSpace, self.index, self.cluster)
        connection.close()
        
    def getNumPoints(self):
        return self.getNumPointsFlat(self.flatTable)
