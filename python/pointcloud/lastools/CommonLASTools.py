#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, glob
from pointcloud import utils

class CommonLASTools():
    def setVariables(self, configuration):
        """ Set configuration parameters and create user if required """
        # The table spaces to be used
        self.userName = configuration.get('DB','User')
        self.password = configuration.get('DB','Pass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        self.dbIndex = configuration.getboolean('DB','DBIndex')
        self.lasIndexTableName = configuration.get('DB','LASIndexTableName').lower()
        
        self.inputFolder = configuration.get('Load','Folder')
        self.fileOffset = configuration.getint('Load','FileOffset')
        self.extension = configuration.get('Load','Extension')
        self.dataFolder = configuration.get('Load','DataFolder')
        self.sort = configuration.getboolean('Load','Sort')
        self.dataExtension = configuration.get('Load','DataExtension')
        self.srid = configuration.get('Load','SRID')
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')

        self.queryTable = configuration.get('Query','QueryTable').lower()
        self.outputExtension = configuration.get('Query','OutputExtension')
        self.numProcessesQuery = configuration.getint('Query','NumberProcesses')
        for extension in (self.extension, self.dataExtension, self.outputExtension):
            if extension not in ('las','laz'):
                raise Exception('Accepted extensions/formats for input files are: las, laz')
        
        self.isSingle = False
        if os.path.isfile(self.inputFolder):
            self.isSingle = True
        else:
            if len(glob.glob(self.inputFolder + '/*_1.' + self.extension)):
                raise Exception('ERROR: input file names can not finish with *_1.' + self.extension + '. This may cause problems when using lasclip')
            numExtFiles = len(glob.glob(self.inputFolder + '/*.' + self.extension))
            numAllFiles = len(glob.glob(self.inputFolder + '/*'))
            if numAllFiles != numExtFiles:
                raise Exception('ERROR: input folder contains other files other than specified by extension')
            if numExtFiles == 1:
                self.isSingle = True
                
        self.colsData = ['x','y','z'] #TODO: add the rest
        
    def connectString(self, super = False, commandLine = False):
        if not super:
            connString = utils.postgresConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort, commandLine)
        else:
            connString = utils.postgresConnectString(self.userName, self.userName, self.password, self.dbHost, self.dbPort, commandLine)
        return connString