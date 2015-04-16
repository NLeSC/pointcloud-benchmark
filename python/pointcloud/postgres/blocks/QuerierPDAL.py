#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, os
from pointcloud import lasops, pdalops, oracleops
from pointcloud.postgres.AbstractQuerier import AbstractQuerier

class QuerierPDAL(AbstractQuerier):    
    def initialize(self):
        if self.numProcessesQuery > 1:
            raise Exception('ERROR: PDAL querier does not support multiple processes')
        
        # Get connection
        connection = self.getConnection()
        cursor = connection.cursor()
        # Get SRID of the stored PC
        cursor.execute('SELECT srid from pointcloud_formats LIMIT 1')
        self.srid = cursor.fetchone()[0]
        
        # Create table to store the query geometries
        #postgresops.dropTable(cursor, self.queryTable, check = True)
        #postgresops.mogrifyExecute(cursor, "CREATE TABLE " +  self.queryTable + " (id integer, geom public.geometry(Geometry," + self.srid + "));")
        
        connection.close()
         
    def query(self, queryId, iterationId, queriesParameters):    
        (eTime, result) = (-1, None)    
        self.prepareQuery(None, queryId, queriesParameters, False)
        
        if self.qp.queryMethod == 'stat' or self.qp.queryType == 'nn' or self.qp.minz != None or self.qp.maxz != None:
            # PDAL can not generate stats or run NN queries or Z queries 
            return (eTime, result) 

        xmlFile = 'pdal' +  str(self.queryIndex) + '.xml'
        if self.qp.queryMethod != 'stream':
            outputFileAbsPath = 'output' +  str(self.queryIndex) + '.las'
            pdalops.PostgreSQLReaderLAS(xmlFile, outputFileAbsPath, self.getConnectionString(), self.blockTable, self.srid, self.qp.wkt)
        else:
            pdalops.PostgreSQLReaderStdOut(xmlFile, self.getConnectionString(), self.blockTable, self.srid, self.qp.wkt)
            
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
