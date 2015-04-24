#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os
import monetdb.sql
from pointcloud import utils

class CommonMonetDB():
    def setVariables(self, configuration):       
        """ Set configuration parameters and create user if required """
        # The table spaces to be used
        self.userName = configuration.get('DB','User')
        self.password = configuration.get('DB','Pass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        self.dbDataDir = configuration.get('DB','DataDirectory')

        self.inputFolder = configuration.get('Load','Folder')
        self.srid = configuration.get('Load','SRID')
        self.createDB = configuration.getboolean('Load','CreateDB')
        self.columns = configuration.get('Load','Columns')
        self.index = configuration.get('Load','Index')
        self.imprints = False
        if self.index == 'imprints':
            self.imprints = True
        
        self.partitioning = configuration.getboolean('Load','Partitioning')
        
        self.flatTable = configuration.get('Load','FlatTable').lower()
        self.metaTable = configuration.get('Load','MetaTable').lower()
        
        self.tempDir = self.dbDataDir + '/tmp'
        if not os.path.isdir(self.tempDir):
            os.system('mkdir -p ' + self.tempDir)
        
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
        
        # Dimensions mapping for las2col tool
        self.DM_LAS2COL = {
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
        utils.checkDimensionMapping(self.DM_LAS2COL)

    def getConnection(self):
        return monetdb.sql.connect(hostname=self.dbHost, database=self.dbName, port=self.dbPort, username=self.userName, password=self.password)
