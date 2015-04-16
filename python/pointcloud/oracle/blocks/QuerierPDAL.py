#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, os
from pointcloud import lasops, pdalops, oracleops
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class QuerierPDAL(AbstractQuerier):    
    def initialize(self):
        if self.numProcessesQuery > 1:
            raise Exception('ERROR: PDAL querier does not support multiple processes')
        
        # Get connection
        connection = self.getConnection()
        cursor = connection.cursor()
        # Get SRID of the stored PC
        oracleops.mogrifyExecute(cursor, "SELECT srid FROM user_sdo_geom_metadata WHERE table_name = '" + self.blockTable + "'")
        self.srid = cursor.fetchone()[0]
        
        # Create table to store the query geometries
        #oracleops.dropTable(cursor, self.queryTable, check = True)
        #oracleops.mogrifyExecute(cursor, "CREATE TABLE " + self.queryTable + " ( id number primary key, geom sdo_geometry) TABLESPACE " + self.tableSpace + " pctfree 0 nologging")
        connection.close()
         
    def query(self, queryId, iterationId, queriesParameters):    
        (eTime, result) = (-1, None)    
        self.prepareQuery(None, queryId, queriesParameters, False)
        
        if queryMethod == 'stat' or self.qp.queryType == 'nn' or self.qp.minz != None or self.qp.maxz != None:
            # PDAL can not generate stats or run NN queries or Z queries 
            return (eTime, result) 
        xmlFile = 'pdal' +  str(queryIndex) + '.xml'
        if self.qp.queryMethod != 'stream':
            outputFileAbsPath = 'output' +  str(queryIndex) + '.las'
            pdalops.OracleReaderLAS(xmlFile, outputFileAbsPath, self.getConnectionString(), self.blockTable, self.baseTable, self.srid, self.qp.wkt)
        else:
            pdalops.OracleReaderStdOut(xmlFile, self.getConnectionString(), self.blockTable, self.baseTable, self.srid, self.qp.wkt)
            
        t0 = time.time()
        if self.qp.queryMethod != 'stream': # disk or stat
            pdalops.executePDAL(xmlFile)
            eTime = time.time() - t0
            result = lasops.getNumPoints(outputFileAbsPath)
            os.system('rm ' + outputFileAbsPath)     
        else:
            result = pdalops.executePDALCount(xmlFile)
            eTime = time.time() - t0
        return (eTime, result)
