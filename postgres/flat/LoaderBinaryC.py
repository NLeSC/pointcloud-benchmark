#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud.postgres.AbstractLoader import AbstractLoader

class LoaderBinaryC(AbstractLoader):
    def initialize(self):
        self.createDB()
        self.createFlat(self.flatTable,self.columns)
        
    def loadFromFile(self, index, fileAbsPath):
        
        c1 = 'las2pg -s '+ fileAbsPath +' --stdout --parse ' + self.columns
        if 'k' in self.columns:
            c1 += ' --moffset ' + str(int(float(self.mortonGlobalOffsetX) / float(self.mortonScaleX))) + ','+ str(int(float(self.mortonGlobalOffsetY) / float(self.mortonScaleY))) + ' --check ' + str(self.mortonScaleX) + ',' + str(self.mortonScaleY)        
        c = c1 + ' | psql '+ self.connectString(False, True) +' -c "copy '+ self.flatTable +' from stdin with binary"'
        
        logging.debug(c)
        os.system(c)
 
    def close(self):
        self.indexClusterVacuumFlat(self.flatTable, self.index)
    
    def getNumPoints(self):
        return self.getNumPointsFlat(self.flatTable)
