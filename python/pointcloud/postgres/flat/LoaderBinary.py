#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging, postgresops
from pointcloud.postgres.AbstractLoader import AbstractLoader

class LoaderBinary(AbstractLoader):
    def initialize(self):
        # Initialize DB and extensions if creation of user is required
        if self.cDB:
            self.createDB()
        self.createFlatTable(self.flatTable, self.tableSpace, self.columns)
        
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, self.srid, _, self.minX, self.minY, _, self.maxX, self.maxY, _, self.scaleX, self.scaleY, _) = lasops.getPCFolderDetails(self.inputFolder)
        
        postgresops.mogifyExecute(cursor, "CREATE TABLE " + self.metaTable + " (tablename text, srid integer, minx DOUBLE PRECISION, miny DOUBLE PRECISION, maxx DOUBLE PRECISION, maxy DOUBLE PRECISION, scalex DOUBLE PRECISION, scaley DOUBLE PRECISION)")
        
    def process(self):
        logging.info('Starting data loading with las2pg (parallel by python) from ' + self.inputFolder + ' to ' + self.dbName)
        metaArgs = (self.flatTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        postgresops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" , metaArgs)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile)
     
    def loadFromFile(self, index, fileAbsPath):
        self.loadFromBinaryLoader(self.getConnectionString(False, True), self.flatTable, fileAbsPath, self.columns, self.minX, self.minY, self.scaleX, self.scaleY)
 
    def close(self):
        self.indexFlatTable(self.flatTable, self.indexTableSpace, self.index, self.cluster)
    
    def getNumPoints(self):
        return self.getNumPointsFlat(self.flatTable)