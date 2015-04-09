#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud import utils, postgresops

class CommonPostgres():
    def setVariables(self, configuration):
        
        """ Set configuration parameters and create user if required """
        # The table spaces to be used
        self.userName = configuration.get('DB','User')
        self.cDB = configuration.getboolean('DB','CreateDB')
        self.password = configuration.get('DB','Pass')
        self.dbName = configuration.get('DB','Name')
        self.dbHost = configuration.get('DB','Host')
        self.dbPort = configuration.get('DB','Port')
        
        self.inputFolder = configuration.get('Load','Folder')

        self.tableSpace = configuration.get('Load','TableSpace').strip().lower()
        self.indexTableSpace = configuration.get('Load','IndexTableSpace').strip().lower()
        self.las2txtTool = configuration.get('Load','LASLibrary')
        self.srid = configuration.get('Load','SRID')
        self.numProcessesLoad = configuration.getint('Load','NumberProcesses')
        
        (self.minX, self.minY, self.maxX, self.maxY) = configuration.get('Load','Extent').split(',')
        
        if configuration.has_option('Load','BlockTable'):
            self.blockTable = configuration.get('Load','BlockTable').lower()
            self.schemaFile = configuration.get('Load','SchemaFile')
            self.blockSize =  configuration.get('Load','BlockSize')
        else:
            self.columns = configuration.get('Load','Columns')
            self.flatTable = configuration.get('Load','FlatTable').lower()
            self.viewName = 'view_' + self.flatTable
            self.index = configuration.get('Load','Index').lower()
            self.fillFactor =  configuration.getint('Load','FillFactor')
            
        self.vacuum = configuration.getboolean('Load','Vacuum')
        self.cluster = configuration.getboolean('Load','Cluster')
        mgoTxt = configuration.get('Load','MortonGlobalOffset')
        if mgoTxt == 'extent':
            (self.mortonGlobalOffsetX, self.mortonGlobalOffsetY) = (self.minX, self.minY)
        else:
            (self.mortonGlobalOffsetX, self.mortonGlobalOffsetY) = mgoTxt.split(',')
        (self.mortonScaleX, self.mortonScaleY) = configuration.get('Load','MortonScale').split(',')
        
        # Variables for queries
        self.queryTable = utils.QUERY_TABLE
        self.numProcessesQuery = configuration.getint('Query','NumberProcesses')
        self.parallelType = configuration.get('Query','ParallelType').lower()
        
        self.colsData = {
                    'x': ('x','DOUBLE PRECISION', 'd', 8, 'x', 'X'),# x coordinate
                    'y': ('y','DOUBLE PRECISION', 'd', 8, 'y', 'Y'),# y coordinate
                    'z': ('z','DOUBLE PRECISION', 'd', 8, 'z', 'Z'),#z coordinate
                    'X': ('ux','INTEGER', 'i', 4, 'x', None),# x coordinate raw (unscaled)
                    'Y': ('uy','INTEGER', 'i', 4, 'y', None),# y coordinate raw (unscaled)
                    'Z': ('uz','INTEGER', 'i', 4, 'z', None),# z coordinate raw (unscaled)
                    'i': ('intensity','INTEGER', 'i', 4, 'intensity', 'Intensity'),# intensity
                    'r': ('returnnum','SMALLINT', 'h', 2, 'return_number', 'ReturnNumber'),# number of this return
                    'n': ('numreturnpulse','SMALLINT', 'h', 2, 'number_of_returns','NumberOfReturns'),# number of returns for given pulse
                    'd': ('dirscanflag','BOOLEAN','c', 1, 'scan_flags','ScanDirectionFlag'),# direction of scan flag
                    'e': ('edgeflightline','BOOLEAN','c', 1, 'flightline_edge','EdgeOfFlightLine'),# edge of flight line
                    'c': ('classification','SMALLINT', 'h', 2, 'classification','Classification'),# classification
                    'a': ('scanangle','SMALLINT', 'h', 2, 'scan_angle','ScanAngleRank'),# scan angle
                    'u': ('userdata','SMALLINT', 'h', 2, 'user_data','UserData'),# user data (does not currently work)
                    'p': ('pId','INTEGER', 'i', 4, 'point_source_id','PointSourceId'),# point source ID
                    'R': ('R','INTEGER', 'i', 4, 'color','Red'),# red channel of RGB color
                    'G': ('G','INTEGER', 'i', 4, 'color','Green'),# green channel of RGB color
                    'B': ('B','INTEGER', 'i', 4, 'color','Blue'), # blue channel of RGB color
                    't': ('time','DOUBLE PRECISION', 'd', 8, 'time','Time'), # GPS time
                    'k': ('morton2D','BIGINT', 'q', 8, 'morton',None), # Morton code 2D
                    }

    def connectString(self, super = False, cline = False):
        if super:
            return postgresops.getConnectString(self.userName, self.userName, self.password, self.dbHost, self.dbPort, cline)
        else:
            return postgresops.getConnectString(self.dbName, self.userName, self.password, self.dbHost, self.dbPort, cline)    