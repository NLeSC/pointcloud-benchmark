#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud import utils
from pointcloud.postgres.AbstractLoader import AbstractLoader

class LoaderASCII(AbstractLoader):
    def initialize(self):
        self.createDB()
        self.createFlat(self.flatTable,self.columns)
                
    def loadFromFile(self, index, fileAbsPath):
        connection = self.connect()
        cursor = connection.cursor()
        
        # We need to convert the data from las/laz to ASCII. We use a named pipe to pipe the conversion into the loading
        namedPipe = '/tmp/' + os.path.basename(fileAbsPath) + '_tempNamedPipe'
        os.system('rm -f ' + namedPipe)
        os.system('mkfifo ' + namedPipe)
        c1 = """psql """ + self.connectString(False, True) + """ -c "COPY """ + self.flatTable + """ FROM '""" + namedPipe + """' (DELIMITER ' ')"  2>&1 &"""
        c2 = utils.las2txtCommand(fileAbsPath, "stdout", columns = self.columns, separation = " ", tool = self.las2txtTool) + ' >> ' + namedPipe
        logging.debug(c1)
        logging.debug(c2)
        os.system(c1)
        os.system(c2)
        os.system('rm ' + namedPipe)
        
    def close(self):
        self.indexClusterVacuumFlat(self.flatTable, self.index)
    
    def getNumPoints(self):
        return self.getNumPointsFlat(self.flatTable)
