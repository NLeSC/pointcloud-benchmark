#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud.oracle.AbstractLoader import AbstractLoader
from pointcloud import utils, oracleops, lasops

class LoaderExt(AbstractLoader):
    def initialize(self):
        if 'k' in self.columns or 'k' in self.index:
            raise Exception('ERROR: LoaderExt not compatible with Morton codes')

        if self.cUser:
            self.createUser()
            
        # Get the point cloud folder description
        logging.info('Getting files, extent, scale and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, _, _, self.minX, self.minY, _, self.maxX, self.maxY, _, self.scaleX, self.scaleY, _) = lasops.getPCFolderDetails(self.inputFolder, numProc = self.numProcessesLoad)
        
        # Creates connection
        connection = self.getConnection()
        cursor = connection.cursor()
        
        # Get the parent folder and the wildcard text with file selection
        if os.path.isfile(self.inputFolder):
            parentFolder = os.path.abspath(os.path.join(self.inputFolder,'..'))
            lasFiles = os.path.basename(self.inputFolder)
            extension = self.inputFolder.split('.')[-1]
        else:
            parentFolder = self.inputFolder
            if len(self.inputFiles) == 0:
                raise Exception('ERROR: None PC file in ' + self.inputFolder)
            extension = self.inputFiles[0].split('.')[-1]
            lasFiles = '*.' + extension

        if extension.lower() == 'laz':
            raise Exception('ERROR: pre-processor only accepts LAS files!')

        self.extTable = ('EXT_' + self.flatTable).upper()
        self.lasDirVariableName = (self.userName + '_las_dir').upper()

        # Create the Oracle directory within the DB
        self.createLASDirectory(self.lasDirVariableName, parentFolder)

        connection = self.getConnection()
        cursor = connection.cursor()
        
        # Define the external table (which use the preprocessor file in Oracle directory EXE_DIR)
        self.createExternalTable(cursor, lasFiles, self.extTable, self.columns, self.lasDirVariableName, self.numProcessesLoad)
        self.createFlatMeta(cursor, self.metaTable)
        connection.close()

    def process(self):
        logging.info('Starting data loading in parallel by an external table loader from ' + self.inputFolder + ' to ' + self.userName)
        return self.processSingle([self.inputFolder,], self.loadInputFolder)

    def loadInputFolder(self,  index, inputFolder):
        connection = self.getConnection()
        cursor = connection.cursor()
        if self.flatTableIOT:
            self.createIOTTable(cursor, self.flatTable, self.extTable, self.tableSpace, self.columns, self.columns, self.index, self.numProcessesLoad)
        else:
            self.createTableAsSelect(cursor, self.flatTable, self.extTable, self.columns, self.tableSpace, self.numProcessesLoad)
        connection.close()

    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        metaArgs = (self.flatTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        oracleops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (:1,:2,:3,:4,:5,:6,:7,:8)" , metaArgs)
        if not self.flatTableIOT:
            self.createIndex(cursor, self.flatTable, self.index, self.indexTableSpace, self.numProcessesLoad)
        connection.close()
        
    def size(self):
        return self.sizeFlat(self.flatTable)

    def getNumPoints(self):
        return self.getNumPointsFlat(self.flatTable)
