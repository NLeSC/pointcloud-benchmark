#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os
import monetdb.sql

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

    def getConnection(self):
        return monetdb.sql.connect(hostname=self.dbHost, database=self.dbName, port=self.dbPort, username=self.userName, password=self.password)