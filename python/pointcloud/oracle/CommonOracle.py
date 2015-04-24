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
        self.parallelType = configuration.get('Query','ParallelType').lower()
        self.queryTable = utils.QUERY_TABLE.upper()
        
        self.DM_FLAT = { # The name of the column in the DB is computed with getDBColumn
            'x': self.columnType,
            'y': self.columnType,
            'z': self.columnType,
            'X': 'NUMBER',
            'Y': 'NUMBER',
            'Z': 'NUMBER',
            'i': 'NUMBER',
            'r': 'NUMBER',
            'n': 'NUMBER',
            'd': 'NUMBER',
            'e': 'NUMBER',
            'c': 'NUMBER',
            'a': 'NUMBER',
            'u': 'NUMBER',
            'p': 'NUMBER',
            'R': 'NUMBER',
            'G': 'NUMBER',
            'B': 'NUMBER',
            't': 'NUMBER',
            'k': 'NUMBER',
            'h': 'NUMBER' # Extra dimension for the Hibert code
        }
        utils.checkDimensionMapping(self.DM_FLAT)
        
        self.DM_LAS2TXT = {
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
        utils.checkDimensionMapping(self.DM_LAS2TXT)
        
        self.DM_SQLLDR = {
            'x': ('float', 10),
            'y': ('float', 10),
            'z': ('float', 8),
            'X': ('integer', 10),
            'Y': ('integer', 10),
            'Z': ('integer', 8),
            'i': ('integer', 5),
            'r': ('integer', 3),
            'n': ('integer', 3),
            'd': ('integer', 3),
            'e': ('integer', 3),
            'c': ('integer', 3),
            'a': ('integer', 3),
            'u': ('integer', 3),
            'p': ('integer', 3),
            'R': ('integer', 5),
            'G': ('integer', 5),
            'B': ('integer', 5),
            't': ('float', 10),
            'k': ('integer', 20)
        }
        utils.checkDimensionMapping(self.DM_SQLLDR)
        
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
    
    def getConnectionString(self, superUser = False):
        if not superUser:
            connString = oracleops.getConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort)
        else:
            connString = oracleops.getConnectString(self.dbName, self.superUserName, self.superPassword, self.dbHost, self.dbPort)
        return connString
    
    def getConnection(self, superUser = False):
        return cx_Oracle.connect(self.getConnectionString(superUser))

    def getDBColumn(self, columns, index, includeType = False, hilbertColumnName = 'd'):
        column = columns[index]
        if column not in self.DM_FLAT:
            raise Exception('Wrong column!' + column)
        if column == 'h':
            if index != (len(columns)-1):
                raise Exception('Hilbert code has to be the last column!')
            columnName = hilbertColumnName
        else:
            columnName = 'VAL_D' + str(index+1)
        if includeType:
            return (columnName, self.DM_FLAT[column])
        else:
            return (columnName,)

    def getColumnNamesDict(self, usePnt = True):
        columnsNamesDict = {}
        for i in range(len(self.columns)):
            column = self.columns[i]
            (cName, ) = self.getDBColumn(self.columns, i)
            cType = 'NUMBER'
            if usePnt:
                columnsNamesDict[column] = ('pnt.' + column,cType)
            else:
                columnsNamesDict[column] = (cName,cType)
        return columnsNamesDict
