#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import psycopg2
from pointcloud import postgresops, utils

class CommonPostgres():
    def setVariables(self, configuration):
        """ Set configuration parameters and create user if required """
        # DB connection parameters
        self.userName = configuration.get('DB','User')
        self.password = configuration.get('DB','Pass')
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
        self.cDB = configuration.getboolean('Load','CreateDB')
        
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
        self.queryFile = configuration.get('Query','File')
        self.numUsers = configuration.getint('Query','NumberUsers')
        self.numIterations = configuration.getint('Query','NumberIterations')
        self.queryTable = utils.QUERY_TABLE
        self.numProcessesQuery = configuration.getint('Query','NumberProcesses')
        self.parallelType = configuration.get('Query','ParallelType').lower()
        
        # Dimensions mapping for DB names and types
        self.DM_FLAT = {
            'x': ('x','DOUBLE PRECISION'),
            'y': ('y','DOUBLE PRECISION'),
            'z': ('z','DOUBLE PRECISION'),
            'X': ('ux','INTEGER'),
            'Y': ('uy','INTEGER'),
            'Z': ('uz','INTEGER'),
            'i': ('intensity','INTEGER'),
            'r': ('returnnum','SMALLINT'),
            'n': ('numreturnpulse','SMALLINT'),
            'd': ('dirscanflag','BOOLEAN'),
            'e': ('edgeflightline','BOOLEAN'),
            'c': ('classification','SMALLINT'),
            'a': ('scanangle','SMALLINT'),
            'u': ('userdata','SMALLINT'),
            'p': ('pId','INTEGER'),
            'R': ('R','INTEGER'),
            'G': ('G','INTEGER'),
            'B': ('B','INTEGER'),
            't': ('time','DOUBLE PRECISION'),
            'k': ('morton2D','BIGINT'),
        }
        utils.checkDimensionMapping(self.DM_FLAT)
        
        # Dimensions mapping for las2pg tool
        self.DM_LAS2PG = {
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
        utils.checkDimensionMapping(self.DM_LAS2PG)
        
        # Dimensions mapping for PDAL
        self.DM_PDAL = {
            'x': 'X',
            'y': 'Y',
            'z': 'Z',
            'X': None,
            'Y': None,
            'Z': None,
            'i': 'Intensity',
            'r': 'ReturnNumber',
            'n': 'NumberOfReturns',
            'd': 'ScanDirectionFlag',
            'e': 'EdgeOfFlightLine',
            'c': 'Classification',
            'a': 'ScanAngleRank',
            'u': 'UserData',
            'p': 'PointSourceId',
            'R': 'Red',
            'G': 'Green',
            'B': 'Blue',
            't': 'Time',
            'k': None
        }
        utils.checkDimensionMapping(self.DM_PDAL)

    def getConnectionString(self, superUser = False, commandLine = False):
        if not superUser:
            return postgresops.getConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort, commandLine) 
        else:
            return postgresops.getConnectString(self.userName, self.userName, self.password, self.dbHost, self.dbPort, commandLine)
    
    def getConnection(self, superUser = False):
        return psycopg2.connect(self.getConnectionString(superUser))   


