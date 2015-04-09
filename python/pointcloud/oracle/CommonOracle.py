#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud import utils

class CommonOracle():
    def setVariables(self, configuration):
        """ Set configuration parameters and create user if required """
        # The table spaces to be used
        self.userName = configuration.get('DB','User').upper()
        self.cUser = configuration.getboolean('DB','CreateUser')
        self.password = configuration.get('DB','Pass')
        self.superUserName = configuration.get('DB','SuperUser') 
        self.superPassword = configuration.get('DB','SuperPass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        
        self.tableSpace = configuration.get('Load','TableSpace').strip().upper()
        self.workTableSpace = configuration.get('Load','WorkTableSpace').strip().upper()
        self.indexTableSpace = configuration.get('Load','IndexTableSpace').strip().upper()
        self.tempTableSpace = configuration.get('Load','TempTableSpace')
        self.las2txtTool = configuration.get('Load','LASLibrary')
        # The table names to be used
        self.flatTable = configuration.get('Load','FlatTable').upper()
        self.columnType = configuration.get('Load','ColumnType').strip().upper()
        self.tolerance = configuration.get('Load','Tolerance')
        self.srid = configuration.get('Load','SRID')
        self.hilbertFactor = configuration.get('Load','HilbertFactor')
        (self.minX, self.minY, self.maxX, self.maxY) = configuration.get('Load','Extent').split(',')
        self.columns = configuration.get('Load','Columns')
            
        # Loading procedure
        self.cluster = configuration.get('Load','Cluster').lower()
        self.clusterDistinct = configuration.getboolean('Load','ClusterDistinct')
        self.cluster2Step = configuration.getboolean('Load','Cluster2Step')
        self.clusterhilbert = False
        if self.cluster == 'true':
            self.cluster = True
        elif self.cluster == 'hilbert':
            self.cluster = True
            self.clusterhilbert = True
        elif self.cluster == 'false':
            self.cluster = False
        else:
            raise Exception('ERROR: cluster must be boolean!')
        self.partition = configuration.get('Load','Partition').lower()
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')
        
        # Variables only used for block loaders
        self.blockTable = None
        self.baseTable = None
        self.blockSize =  None
        self.compression = None
        
        if configuration.has_option('Load','BlockTable'):
            self.blockTable = configuration.get('Load','BlockTable').upper()
            self.baseTable = configuration.get('Load','BaseTable').upper()
            self.blockSize =  configuration.get('Load','BlockSize')
            self.batchSize =  configuration.getint('Load','BatchSize')
            self.compression = configuration.get('Load','Compression').strip()
            self.blockMethod = configuration.get('Load','BlockMethod').strip().lower()
            self.isHilbertBlockMethod = (self.blockMethod == 'hilbert')
            self.javaBinPath = configuration.get('Load','JavaBinPath')
            self.chunkSize =  configuration.getint('Load','ChunkSize')
            self.pause =  configuration.getint('Load','Pause')
        else:
            self.index = configuration.get('Load','Index').lower()
            (self.mortonScaleX, self.mortonScaleY) = configuration.get('Load','MortonScale').split(',')
            
        # Directories to be used in external table reading
        self.inputFolder = configuration.get('Load','Folder')

        # Variable names to be used in external table reading
        self.exeDirVariableName = configuration.get('Load','ExeDir').upper()
        self.logDirVariableName = configuration.get('Load','LogDir').upper()
        self.lasDirVariableName = (self.userName + '_las_dir').upper()
        
        self.extTable = ('EXT_' + self.flatTable).upper()
        
        # Variables for queries
        self.queryTable = utils.QUERY_TABLE.upper()
        self.numProcessesQuery = configuration.getint('Query','NumberProcesses')
        self.parallelType = configuration.get('Query','ParallelType').lower()

        self.colsData = {
                    'x': ('val_d1',self.columnType, 'float', 10),# x coordinate
                    'y': ('val_d2',self.columnType, 'float', 10),# y coordinate
                    'z': ('val_d3',self.columnType, 'float', 8),#z coordinate
                    'X': ('ux','NUMBER', 'integer', 10),# x coordinate raw (unscaled)
                    'Y': ('uy','NUMBER', 'integer', 10),# y coordinate raw (unscaled)
                    'Z': ('uz','NUMBER', 'integer', 8),# z coordinate raw (unscaled)
                    'i': ('intensity','NUMBER', 'integer', 5),# intensity
                    'r': ('return_number','NUMBER', 'integer', 3),# number of this return
                    'n': ('number_of_returns','NUMBER', 'integer', 3),# number of returns for given pulse
                    'd': ('scan_direction_flag','NUMBER', 'integer', 3),# direction of scan flag
                    'e': ('edge_of_flight_line','NUMBER', 'integer', 3),# edge of flight line
                    'c': ('classification','NUMBER', 'integer', 3),# classification
                    'a': ('scan_angle_rank','NUMBER', 'integer', 3),# scan angle
                    'u': ('user_bit_field','NUMBER', 'integer', 3),# user data (does not currently work)
                    'p': ('point_id','NUMBER', 'integer', 3),# point source ID
                    'R': ('R','NUMBER', 'integer', 5),# red channel of RGB color
                    'G': ('G','NUMBER', 'integer', 5),# green channel of RGB color
                    'B': ('B','NUMBER', 'integer', 5), # blue channel of RGB color
                    't': ('time','NUMBER', 'float', 10), # GPS time
                    'k': ('morton2D','NUMBER', 'integer', 20), # Morton code 2D
                    # Others given by external table loader
                    '1': ('file_marker','NUMBER', 'integer', 3), # Morton code 2D
                    'h': ('d','NUMBER', 'integer', 20), # Morton code 2D
                    '3': ('pyramid_level','NUMBER', 'integer', 3), # Morton code 2D
                    '4': ('file_name','varchar2(128)', 'string', 128), # Morton code 2D
                    '5': ('las_version','NUMBER', 'integer', 2), # Morton code 2D
                    '6': ('point_format','NUMBER', 'integer', 3), # Morton code 2D
            }
    
    def connectString(self, superUser = False):
        if superUser:
            return self.superUserName + "/" + self.superPassword + "@//" + self.dbHost + ":" + self.dbPort + "/" + self.dbName
        else:
            return self.userName + "/" + self.password + "@//" + self.dbHost + ":" + self.dbPort + "/" + self.dbName
        
    def mogrifyExecute(self, cursor, query, queryArgs = None):
        logging.info(utils.oraclemogrify(cursor, query, queryArgs))
        if queryArgs != None:
            cursor.execute(query, queryArgs)
        else:
            cursor.execute(query)
            
    def dropTable(self, cursor, tableName, check = False):
        if check:
            cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[tableName,])
            if len(cursor.fetchall()):
                self.mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
        else:
            self.mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
        cursor.connection.commit()