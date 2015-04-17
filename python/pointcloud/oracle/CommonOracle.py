#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging, cx_Oracle
from pointcloud import utils, oracleops

class CommonOracle():
    def setVariables(self, configuration):
        """ Set configuration parameters and create user if required """
        # DB connection parameters
        self.userName = configuration.get('DB','User').upper()
        self.password = configuration.get('DB','Pass')
        self.superUserName = configuration.get('DB','SuperUser') 
        self.superPassword = configuration.get('DB','SuperPass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        
        #
        # LOADING VARIABLES
        #
        
        # Input data to use
        self.inputFolder = configuration.get('Load','Folder')
        self.srid = configuration.get('Load','SRID')
        self.columns = configuration.get('Load','Columns')
        self.cUser = configuration.getboolean('Load','CreateUser')
        # Table spaces to use
        self.tableSpace = configuration.get('Load','TableSpace').strip().upper()
        self.indexTableSpace = configuration.get('Load','IndexTableSpace').strip().upper()
        self.workTableSpace = configuration.get('Load','WorkTableSpace').strip().upper()
        self.tempTableSpace = configuration.get('Load','TempTableSpace').strip().upper()
                
        # Get number of processes to be used in Loading
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')
        self.tolerance = configuration.get('Load','Tolerance')
                
        # The rest of loading options depend on which mode wer are using (global, incremental, or flat)
        loadingMode = ''
        hasBlockTable = configuration.has_option('Load','BlockTable')
        hasFlatTable = configuration.has_option('Load','FlatTable')
         
        if hasBlockTable:
            if hasFlatTable:
                loadingMode = 'BLK_GLOB'
            else:
                loadingMode = 'BLK_INCR'
        else:
            loadingMode = 'FLAT'
            
        if loadingMode == 'BLK_GLOB':
            self.columnType = 'BINARY_DOUBLE'
            self.blockTable = configuration.get('Load','BlockTable').upper()
            self.baseTable = configuration.get('Load','BaseTable').upper()
    
            self.blockSize =  configuration.get('Load','BlockSize')
            self.blockMethod = configuration.get('Load','BlockMethod').strip().lower()
            self.compression = configuration.get('Load','Compression').strip()
            self.hilbertFactor = configuration.get('Load','HilbertFactor')
        elif loadingMode == 'BLK_INCR':
            self.columnType = 'BINARY_DOUBLE'
            self.blockTable = configuration.get('Load','BlockTable').upper()
            self.baseTable = configuration.get('Load','BaseTable').upper()
            
            self.blockSize =  configuration.get('Load','BlockSize')
            self.batchSize =  configuration.getint('Load','BatchSize')
            self.compression = configuration.get('Load','Compression').strip()
            self.useOffsetScale = configuration.getboolean('Load','PDAL32bitCoordinates')
            self.pdalCompression = configuration.getboolean('Load','PDALCompression')
            self.pdalDimOrientation = configuration.getboolean('Load','PDALDimensionalOrientation')
        else:#if loadingMode == 'FLAT':
            self.columnType = 'NUMBER'
            self.flatTable = configuration.get('Load','FlatTable').upper()
            self.flatTableIOT = configuration.getboolean('Load','FlatTableIOT')
            self.index = configuration.get('Load','Index').lower()
            
            self.metaTable = configuration.get('Load','MetaTable').upper()
        #
        # QUERY VARIABLES
        #
        self.queryFile = configuration.get('Query','File')
        self.numUsers = configuration.getint('Query','NumberUsers')
        self.numIterations = configuration.getint('Query','NumberIterations')
        self.numProcessesQuery = configuration.getint('Query','NumberProcesses')
        if loadingMode != 'FLAT':
            self.parallelType = configuration.get('Query','ParallelType').lower()
        self.queryTable = utils.QUERY_TABLE.upper()
        
        # colsData is dimName: (dbType, controlFileType, controlFileNumDigits)
        self.colsData = {
                    'x': (self.columnType, 'float', 10),# x coordinate
                    'y': (self.columnType, 'float', 10),# y coordinate
                    'z': (self.columnType, 'float', 8),#z coordinate
                    'X': ('NUMBER', 'integer', 10),# x coordinate raw (unscaled)
                    'Y': ('NUMBER', 'integer', 10),# y coordinate raw (unscaled)
                    'Z': ('NUMBER', 'integer', 8),# z coordinate raw (unscaled)
                    'i': ('NUMBER', 'integer', 5),# intensity
                    'r': ('NUMBER', 'integer', 3),# number of this return
                    'n': ('NUMBER', 'integer', 3),# number of returns for given pulse
                    'd': ('NUMBER', 'integer', 3),# direction of scan flag
                    'e': ('NUMBER', 'integer', 3),# edge of flight line
                    'c': ('NUMBER', 'integer', 3),# classification
                    'a': ('NUMBER', 'integer', 3),# scan angle
                    'u': ('NUMBER', 'integer', 3),# user data (does not currently work)
                    'p': ('NUMBER', 'integer', 3),# point source ID
                    'R': ('NUMBER', 'integer', 5),# red channel of RGB color
                    'G': ('NUMBER', 'integer', 5),# green channel of RGB color
                    'B': ('NUMBER', 'integer', 5), # blue channel of RGB color
                    't': ('NUMBER', 'float', 10), # GPS time
                    'k': ('NUMBER', 'integer', 20), # Morton code 2D
                    # Others given by external table loader
                    'm': ('NUMBER', 'integer', 3), # file_marker
                    'h': ('NUMBER', 'integer', 20), # Hilbert code
                    'l': ('NUMBER', 'integer', 3), # pyramid_level
                    'n': ('VARCHAR2(128)', 'string', 128), # file_name
                    'v': ('NUMBER', 'integer', 2), # las_version
                    'f': ('NUMBER', 'integer', 3), # point_format
            }
    
    def getConnectionString(self, superUser = False):
        if not superUser:
            connString = oracleops.getConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort)
        else:
            connString = oracleops.getConnectString(self.dbName, self.superUserName, self.superPassword, self.dbHost, self.dbPort)
        return connString
    
    def getConnection(self, superUser = False):
        return cx_Oracle.connect(self.getConnectionString(superUser))
