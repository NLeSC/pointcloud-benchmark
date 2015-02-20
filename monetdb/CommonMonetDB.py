#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud import utils
import monetdb.sql

class CommonMonetDB():
    def setVariables(self, configuration):       
        """ Set configuration parameters and create user if required """
        # The table spaces to be used
        self.userName = configuration.get('DB','User')
        self.cDB = configuration.getboolean('DB','CreateDB')
        self.password = configuration.get('DB','Pass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        self.dbDataDir = configuration.get('DB','DataDirectory')
        self.restartDB = configuration.getboolean('DB','RestartDB') 

        self.inputFolder = configuration.get('Load','Folder')
        self.fileOffset = configuration.getint('Load','FileOffset')
        self.extension = configuration.get('Load','Extension')
        if not (self.extension.lower().endswith('las') or self.extension.lower().endswith('laz')):
            raise Exception('Accepted format for input files are: las, laz')
        (self.minX, self.minY, self.maxX, self.maxY) = configuration.get('Load','Extent').split(',')
        self.columns = configuration.get('Load','Columns')
        self.index = configuration.get('Load','Index')
        self.imprints = False
        if self.index == 'imprints':
            self.imprints = True
        self.las2txtTool = configuration.get('Load','LASLibrary')
        self.srid = configuration.get('Load','SRID')
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')
        self.maxFiles = configuration.getint('Load','MaxFiles')
        self.partitioning = configuration.getboolean('Load','Partitioning')
        
        self.flatTable = configuration.get('Load','FlatTable').lower()
        mgoTxt = configuration.get('Load','MortonGlobalOffset')
        if mgoTxt == 'extent':
            (self.mortonGlobalOffsetX, self.mortonGlobalOffsetY) = (self.minX, self.minY)
        else:
            (self.mortonGlobalOffsetX, self.mortonGlobalOffsetY) = mgoTxt.split(',')
        (self.mortonScaleX, self.mortonScaleY) = configuration.get('Load','MortonScale').split(',')
        
        # Variables for queries
        self.queryTable = configuration.get('Query','QueryTable').lower()

        self.tempDir = self.dbDataDir + '/tmp'
        if not os.path.isdir(self.tempDir):
            os.system('mkdir ' + self.tempDir)
            
        self.colsData = {
                    'x': ['x','DOUBLE PRECISION'],# x coordinate
                    'y': ['y','DOUBLE PRECISION'],# y coordinate
                    'z': ['z','DOUBLE PRECISION'],#z coordinate
                    'X': ['ux','INTEGER'],# x coordinate raw (unscaled)
                    'Y': ['uy','INTEGER'],# y coordinate raw (unscaled)
                    'Z': ['uz','INTEGER'],# z coordinate raw (unscaled)
                    'i': ['intensity','INTEGER'],# intensity
                    'r': ['returnnum','SMALLINT'],# number of this return
                    'n': ['numreturnpulse','SMALLINT'],# number of returns for given pulse
                    'd': ['dirscanflag','BOOLEAN'],# direction of scan flag
                    'e': ['edgeflightline','BOOLEAN'],# edge of flight line
                    'c': ['classification','SMALLINT'],# classification
                    'a': ['scanangle','SMALLINT'],# scan angle
                    'u': ['userdata','SMALLINT'],# user data (does not currently work)
                    'p': ['pId','INTEGER'],# point source ID
                    'R': ['R','INTEGER'],# red channel of RGB color
                    'G': ['G','INTEGER'],# green channel of RGB color
                    'B': ['B','INTEGER'], # blue channel of RGB color
                    't': ['time','DOUBLE PRECISION'], # GPS time
                    'k': ['morton2D','BIGINT'], # Morton code 2D
        }

    def connection(self):
        return monetdb.sql.connect(hostname=self.dbHost, database=self.dbName, port=self.dbPort, username=self.userName, password=self.password)
    
    def mogrifyExecute(self, cursor, query, queryArgs = None):
        logging.info(utils.monetdbmogrify(cursor, query, queryArgs))
        if queryArgs != None:
            return cursor.execute(query, queryArgs)
        else:
            return cursor.execute(query)
     
    def dropTable(self, cursor, tableName, check = False):
        if check:
            if cursor.execute('select name from tables where name = %s', (tableName,)):
                self.mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
        else:
            self.mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
        cursor.connection.commit()
