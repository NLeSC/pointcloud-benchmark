#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, os
from pointcloud import lasops, pdalops, oracleops
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class QuerierPDAL(AbstractQuerier):    
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        if self.numProcessesQuery > 1:
            raise Exception('ERROR: PDAL querier does not support multiple processes')
        
        # Create the quadtree
        connection = self.getConnection()
        cursor = connection.cursor()
        
        oracleops.mogrifyExecute(cursor, "SELECT srid FROM user_sdo_geom_metadata WHERE table_name = '" + self.blockTable + "'")
        (self.srid,) = cursor.fetchone()[0]

          
    def query(self, queryId, iterationId, queriesParameters):
        oracleops.dropTable(cursor, self.resultTable, True) 
        
        self.prepareQuery(queryId, queriesParameters, False)
        (eTime, result) = (-1, None)
        
        if queryMethod == 'stat' or self.qp.queryType == 'nn' or self.qp.minz != None or self.qp.maxz != None:
            # PDAL can not generate stats or run NN queries or Z queries 
            return (eTime, result) 
        
        if self.qp.queryMethod != 'stream':
            outputFileAbsPath = 'output' +  str(queryIndex) + '.las'
            xmlFile = pdalops.OracleReaderLAS(outputFileAbsPath, self.getConnectionString(), self.blockTable, self.baseTable, self.srid, self.qp.wkt)
        else:
            xmlFile = pdalops.OracleReaderStdOut(self.getConnectionString(), self.blockTable, self.baseTable, self.srid, self.qp.wkt)
            
        t0 = time.time()
        if self.qp.queryMethod != 'stream': # disk or stat
            c = 'pdal pipeline ' + xmlFile
            os.system(c)
            eTime = time.time() - t0
            result = lasops.getNumPoints(outputFileAbsPath)
            os.system('rm ' + outputFileAbsPath)     
        else:
            result = pdalops.executePDALCount(xmlFile)
            eTime = time.time() - t0
        
        os.system('rm ' + xmlFile)
        return (eTime, result)
