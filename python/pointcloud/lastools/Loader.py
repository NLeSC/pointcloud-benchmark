#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
############################
import os, logging
from pointcloud import lidaroverview, postgresops, lasops
from pointcloud.AbstractLoader import AbstractLoader
from pointcloud.lastools.CommonLASTools import CommonLASTools

class Loader(AbstractLoader,CommonLASTools):
    def __init__(self, configuration):
        """ Set configuration parameters"""
        AbstractLoader.__init__(self, configuration)
        self.setVariables(configuration)
    
    def initialize(self):
        # Remove possible previous data
        logging.info('Creating data folder ' + self.dataFolder)
        os.system('rm -rf ' + self.dataFolder)
        os.system('mkdir -p ' + self.dataFolder)
        
        logging.info('Getting files and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, _, _, _, _, _, _, _, _, _, _, _) = lasops.getPCFolderDetails(self.inputFolder)
        
    def process(self):
        logging.info('Starting data preparation (' + str(self.numProcessesLoad) + ' processes) from ' + self.inputFolder + ' to ' + self.dataFolder)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.processFile)
    
    def processFile(self, index, fileAbsPath):
        # Get the file extension
        extension = fileAbsPath.split('.')[-1]
        
        if extension == self.dataExtension:
            outputAbsPath = self.dataFolder + '/' + os.path.basename(fileAbsPath)
        else:
            outputAbsPath = self.dataFolder + '/' + os.path.basename(fileAbsPath).replace(extension, self.dataExtension)
        commands = []
        if self.sort:
            commands.append('lassort.exe -i ' + fileAbsPath + ' -o ' + outputAbsPath)
        else:
            if extension == self.dataExtension:
                commands.append('ln -s ' + fileAbsPath + ' ' + outputAbsPath)
            else:
                commands.append('las2las -i ' + fileAbsPath + ' -o ' + outputAbsPath)
        commands.append('lasindex -i ' + outputAbsPath)
        
        for command in commands:
            logging.info(command)
            os.system(command)
        
    def close(self):
        if self.dbIndex:
            logging.info('Creating index DB')
            lidaroverview.run(self.dataFolder, self.numProcessesLoad, self.dbName, self.userName, self.password, self.dbHost, self.dbPort, self.createDB, self.lasIndexTableName, 'lastools', self.srid)
        
    def size(self):
        try:
            size_indexes = float(((os.popen("stat -Lc %s " + self.dataFolder + "/*.lax | awk '{t+=$1}END{print t}'")).read().split('\t'))[0]) / (1024. * 1024.)
        except:
            size_indexes = 0.
        if self.dbIndex:
            connection = self.getConnection()
            cursor = connection.cursor()
            size_indexes += float(postgresops.getSizes(cursor)[-1])
            connection.close()
        try:
            size_ex_indexes = float(((os.popen("stat -Lc %s " + self.dataFolder + "/*." +  self.dataExtension + " | awk '{t+=$1}END{print t}'")).read().split('\t'))[0]) / (1024. * 1024.)
        except:
            size_ex_indexes = 0
        size_total = size_indexes + size_ex_indexes
        return ' Size indexes= ' +  ('%.3f MB' % size_indexes) + '. Size excluding indexes= ' +  ('%.3f MB' % size_ex_indexes) + '. Size total= ' +  ('%.3f MB' % size_total)

    def getNumPoints(self) :
        try:
            if self.dbIndex:
                connString = self.getConnectString(False, True)
                return int(os.popen('psql ' + connString + ' -c "select sum(num) from ' + self.lasIndexTableName + '" -t -A').read())
            else:
                return int(os.popen("lasinfo -i " + self.dataFolder + "/*." +  self.dataExtension + " 2>&1 | grep 'number of point records' | awk '{t+=$5}END{print t}'").read())
        except: 
            return 0

