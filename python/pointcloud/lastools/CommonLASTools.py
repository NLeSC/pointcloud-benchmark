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
        self.createDB = configuration.getboolean('DB','CreateDB')
        self.dbIndex = configuration.getboolean('DB','DBLASIndex')
        self.lasIndexTableName = configuration.get('DB','DBLASIndexTableName').lower()
        
        self.inputFolder = configuration.get('Load','Folder')
        self.dataFolder = configuration.get('Load','DataFolder')
        self.sort = configuration.getboolean('Load','Sort')
        self.dataExtension = configuration.get('Load','DataExtension')
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')

        self.outputExtension = configuration.get('Query','OutputExtension')
        self.numProcessesQuery = configuration.getint('Query','NumberProcesses')
        for extension in (self.dataExtension, self.outputExtension):
            if extension not in utils.PC_FILE_FORMATS:
                raise Exception('Accepted extensions/formats for point cloud files are: ' + ','.join(utils.PC_FILE_FORMATS))
        
        self.colsData = ['x','y','z'] #TODO: add the rest
        
    def getConnectionString(self, superUser = False, commandLine = False):
        if not superUser:
            connString = postgresops.getConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort, commandLine)
        else:
            connString = postgresops.getConnectString(self.userName, self.userName, self.password, self.dbHost, self.dbPort, commandLine)
        return connString
    
    def getConnection(self, superUser = False):
        return psycopg2.connect(self.getConnectionString(superUser))