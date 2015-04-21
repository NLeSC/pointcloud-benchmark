#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
import cx_Oracle
from pointcloud.AbstractQuerier import AbstractQuerier as AQuerier
from pointcloud.oracle.CommonOracle import CommonOracle
from pointcloud import oracleops

class AbstractQuerier(AQuerier, CommonOracle):
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AQuerier.__init__(self, configuration)
        self.setVariables(configuration)
        self.srid = None #to be filled in by the different implementations (in their init method)
            
    def close(self): 
        return

    def prepareQuery(self, cursor, queryId, queriesParameters, addGeom = True):
        self.queryIndex = int(queryId)
        self.resultTable = ('query_results_' + str(self.queryIndex)).upper()
        
        self.qp = queriesParameters.getQueryParameters('ora',queryId, self.colsData.keys())
        logging.debug(self.qp.queryKey)
        
        if addGeom:
            cursor.setinputsizes(ARG1 = cx_Oracle.CLOB)
            cursor.execute('insert into ' + self.queryTable + ' values (:ARG0,SDO_UTIL.FROM_WKTGEOMETRY(:ARG1))', ARG0=self.queryIndex, ARG1=self.qp.wkt)
            cursor.execute('UPDATE ' + self.queryTable + ' t SET T.GEOM.SDO_SRID = :1 where T.ID = :2', [int(self.srid), self.queryIndex])
            cursor.connection.commit()
    
    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        return (None, "(select geom from " + self.queryTable + " where id = " + str(self.queryIndex) + "), " + str(self.tolerance) + ", NULL ")    
    
    def createGridTableMethod(self, cursor, gridTable, ncols, nrows):
        query = """
CREATE TABLE """ + gridTable + """ AS 
WITH A AS (
    SELECT geom FROM """ + self.queryTable + """ where id = """ + str(self.queryIndex) + """
)
SELECT T.id, SDO_GEOM.SDO_INTERSECTION(A.geom, T.GEOMETRY, """ + str(self.tolerance) + """) as geom FROM 
    TABLE(select sdo_sam.tiled_bins(
        SDO_GEOM.SDO_MIN_MBR_ORDINATE(a.geom,1),
        SDO_GEOM.SDO_MAX_MBR_ORDINATE(a.geom,1),
        SDO_GEOM.SDO_MIN_MBR_ORDINATE(a.geom,2),
        SDO_GEOM.SDO_MAX_MBR_ORDINATE(a.geom,2),
        null, """ + str(self.srid) + """, """ + str(ncols) +""" - 1, """ + str(nrows) +""" - 1
    ) from A) T, A
"""
        oracleops.mogrifyExecute(cursor, query)
        cursor.execute("CREATE INDEX " + gridTable + "_id_idx ON " + gridTable + "(ID)")
        cursor.connection.commit()

    def getParallelHint(self):
        parallelHint = ''
        if self.numProcessesQuery > 1:
            parallelHint = '/*+ PARALLEL (' + str(self.numProcessesQuery) + ') */ ' 
        return parallelHint
