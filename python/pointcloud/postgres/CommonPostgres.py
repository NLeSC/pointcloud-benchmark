#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import pyscopg2
from pointcloud import postgresops

class CommonPostgres():
    def setVariables(self, configuration):
        """ Set configuration parameters and create user if required """
        # DB connection parameters
        self.userName = configuration.get('DB','User')
        self.cDB = configuration.getboolean('DB','CreateDB')
        self.password = configuration.get('DB','Pass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        
        #
        # LOADING VARIABLES
        #
        
        # Input data to use
        self.inputFolder = configuration.get('Load','Folder')
        self.columns = configuration.get('Load','Columns')
        
        # Table spaces to use
        self.tableSpace = configuration.get('Load','TableSpace').strip().upper()
        self.indexTableSpace = configuration.get('Load','IndexTableSpace').strip().upper()
        
        # Get number of processes to be used in Loading
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')
        self.cluster = configuration.getboolean('Load','Cluster')
        
        # The rest of loading options depend on which mode wer are using (global, incremental, or flat)
        loadingMode = ''
        hasBlockTable = configuration.has_option('Load','BlockTable')
        if hasBlockTable:
            loadingMode = 'BLK'
        else:
            loadingMode = 'FLAT'
        
        if loadingMode == 'BLK':
            self.blockTable = configuration.get('Load','BlockTable').lower()
            self.schemaFile = configuration.get('Load','SchemaFile')
            self.blockSize =  configuration.getint('Load','BlockSize')
        else:
            self.flatTable = configuration.get('Load','FlatTable').lower()
            self.metaTable = configuration.get('Load','MetaTable').lower()
            self.index = configuration.get('Load','Index').lower()
            
        # Variables for queries
        self.queryFile = config.get('Query','File')
        self.numUsers = config.getint('Query','NumberUsers')
        self.numIterations = config.getint('Query','NumberIterations')
        self.queryTable = utils.QUERY_TABLE
        self.numProcessesQuery = configuration.getint('Query','NumberProcesses')
        self.parallelType = configuration.get('Query','ParallelType').lower()
        
         # colsData is dimName: (dbName, dbType)
        self.colsData = {
                    'x': ('x','DOUBLE PRECISION'),# x coordinate
                    'y': ('y','DOUBLE PRECISION'),# y coordinate
                    'z': ('z','DOUBLE PRECISION'),#z coordinate
                    'X': ('ux','INTEGER'),# x coordinate raw (unscaled)
                    'Y': ('uy','INTEGER'),# y coordinate raw (unscaled)
                    'Z': ('uz','INTEGER'),# z coordinate raw (unscaled)
                    'i': ('intensity','INTEGER'),# intensity
                    'r': ('returnnum','SMALLINT'),# number of this return
                    'n': ('numreturnpulse','SMALLINT'),# number of returns for given pulse
                    'd': ('dirscanflag','BOOLEAN'),# direction of scan flag
                    'e': ('edgeflightline','BOOLEAN'),# edge of flight line
                    'c': ('classification','SMALLINT'),# classification
                    'a': ('scanangle','SMALLINT'),# scan angle
                    'u': ('userdata','SMALLINT'),# user data (does not currently work)
                    'p': ('pId','INTEGER'),# point source ID
                    'R': ('R','INTEGER'),# red channel of RGB color
                    'G': ('G','INTEGER'),# green channel of RGB color
                    'B': ('B','INTEGER'), # blue channel of RGB color
                    't': ('time','DOUBLE PRECISION'), # GPS time
                    'k': ('morton2D','BIGINT'), # Morton code 2D
                    }

    def getConnectionString(self, superUser = False):
        if not superUser:
            return postgresops.getConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort, cline) 
        else:
            return postgresops.getConnectString(self.userName, self.userName, self.password, self.dbHost, self.dbPort, cline)
    
    def getConnection(self, superUser = False):
        return psycopg2.connect(self.getConnectionString(superUser))   