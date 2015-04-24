#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import psycopg2
from pointcloud import utils, postgresops

class CommonLASTools():
    def setVariables(self, configuration):
        """ Set configuration parameters and create user if required """
        # The table spaces to be used
        self.userName = configuration.get('DB','User')
        self.password = configuration.get('DB','Pass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        
        self.inputFolder = configuration.get('Load','Folder')
        self.srid = configuration.get('Load','SRID')
        self.dataFolder = configuration.get('Load','DataFolder')
        self.sort = configuration.getboolean('Load','Sort')
        self.dataExtension = configuration.get('Load','DataExtension')
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')

        self.createDB = configuration.getboolean('Load','CreateDB')
        self.dbIndex = configuration.getboolean('Load','DBLASIndex')
        self.lasIndexTableName = configuration.get('Load','DBLASIndexTableName').lower()
        
        
        self.outputExtension = configuration.get('Query','OutputExtension')
        for extension in (self.dataExtension, self.outputExtension):
            if extension not in utils.PC_FILE_FORMATS:
                raise Exception('Accepted extensions/formats for point cloud files are: ' + ','.join(utils.PC_FILE_FORMATS))
        
        self.DM_LASTOOLS = {
            'x': 'x',
            'y': 'y',
            'z': 'z',
            'X': 'X',
            'Y': 'Y',
            'Z': 'Z',
            'i': 'i',
            'r': 'r',
            'n': 'n',
            'd': 'd',
            'e': 'e',
            'c': 'c',
            'a': 'a',
            'u': 'u',
            'p': 'p',
            'R': 'R',
            'G': 'G',
            'B': 'B',
            't': 't',
            'k': 'k'
        }
        utils.checkDimensionMapping(self.DM_LASTOOLS)
        
    def getConnectionString(self, superUser = False, commandLine = False):
        if not superUser:
            connString = postgresops.getConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort, commandLine)
        else:
            connString = postgresops.getConnectString(self.userName, self.userName, self.password, self.dbHost, self.dbPort, commandLine)
        return connString
    
    def getConnection(self, superUser = False):
        return psycopg2.connect(self.getConnectionString(superUser))