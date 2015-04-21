#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, math
from itertools import groupby, count
from pointcloud import dbops, utils, oracleops
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class Querier(AbstractQuerier):      
    def initialize(self):
        # Get connection
        connection = self.getConnection()
        cursor = connection.cursor()
        # Get SRID of the stored PC
        oracleops.mogrifyExecute(cursor, "SELECT srid FROM user_sdo_geom_metadata WHERE table_name = '" + self.blockTable + "'")
        self.srid = cursor.fetchone()[0]
        
        # We create an auxiliary dictionary with the column names from the point object
        self.columnsNamesTypesDict = {}
        for col in self.columns:
            self.columnsNamesTypesDict[col] = ('pnt.' + col,)
        
        # Create table to store the query geometries
        oracleops.dropTable(cursor, self.queryTable, check = True)
        oracleops.mogrifyExecute(cursor, "CREATE TABLE " + self.queryTable + " ( id number primary key, geom sdo_geometry) TABLESPACE " + self.tableSpace + " pctfree 0 nologging")
        connection.close()
          
    def query(self, queryId, iterationId, queriesParameters):
        (eTime, result) = (-1, None)
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.prepareQuery(cursor, queryId, queriesParameters, iterationId == 0)
        oracleops.dropTable(cursor, self.resultTable, True) 
        
        if self.qp.queryMethod != 'stream' and self.numProcessesQuery > 1 and self.parallelType != 'nati' and self.qp.queryType in ('rectangle','circle','generic') :
             return self.pythonParallelization()

        t0 = time.time()
        query = self.getSelect()
        
        if self.qp.queryMethod != 'stream': # disk or stat
            oracleops.mogrifyExecute(cursor, "CREATE TABLE " + self.resultTable + " AS " + query)
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, None, True, self.qp.columns, self.qp.statistics)
        else:
            sqlFileName = str(queryId) + '.sql'
            oracleops.createSQLFile(cursor, sqlFileName, query, None)
            result = oracleops.executeSQLFileCount(self.getConnectionString(False), sqlFileName)
            eTime = time.time() - t0
        connection.close()
        return (eTime, result)
    
    def getSelect(self):
        selectedColumns = dbops.getSelectCols(self.qp.columns, self.columnsNamesTypesDict, self.qp.statistics)
        zCondition = dbops.addZCondition(self.qp, self.columnsNamesTypesDict['z'][0], None)
        if self.qp.queryType in ('rectangle','circle','generic'):
            if self.numProcessesQuery == 1:
                query = """
    SELECT """ + selectedColumns + """ 
    FROM
      table(sdo_pc_pkg.clip_pc(
          (SELECT pc FROM """ + self.baseTable + """),
          (SELECT geom FROM """ + self.queryTable + """ WHERE id = """ + str(self.queryIndex) + """),
          null, 1, 1)) query_blocks,
    table(sdo_util.getvertices(sdo_pc_pkg.to_geometry(
          query_blocks.points,
          query_blocks.num_points,
          3, null))) pnt
    """ + dbops.getWhereStatement(zCondition)
            else:
                
                selectedColumns = dbops.getSelectCols(self.qp.columns, self.columnsNamesTypesDict, self.qp.statistics)
                query = """
WITH
  candidates AS (
    SELECT blocks.blk_id, subqueries.ind_dim_qry, subqueries.other_dim_qry
    FROM """ + self.blockTable + """ blocks,
      (SELECT 1 min_res, 1 max_res, 
             (SELECT geom FROM """ + self.queryTable + """ WHERE id = """ + str(self.queryIndex) + """) ind_dim_qry, 
             cast(null as sdo_mbr) other_dim_qry 
       FROM dual ) subqueries
    WHERE
      blocks.pcblk_min_res <= max_res and
      blocks.pcblk_max_res >= min_res and
      SDO_ANYINTERACT(blocks.blk_extent, subqueries.ind_dim_qry) = 'TRUE')
SELECT """ + self.getParallelHint() +  """ """ + selectedColumns + """ 
FROM
  table(
    sdo_pc_pkg.clip_pc_parallel(
      cursor(select * from candidates),
      (select pc from """ + self.baseTable + """)))
"""

        else: # NN query
            numBlocksNeigh = int(math.pow(2 + math.ceil(math.sqrt(math.ceil(float(self.qp.num)/float(self.blockSize)))), 2))
            query = """
SELECT """ + selectedColumns + """ 
FROM (SELECT a.points, a.num_points 
      FROM """ + self.blockTable + """ a, """ + self.queryTable + """ b 
      WHERE b.ID = """ + str(self.queryIndex) + """ AND SDO_NN(a.BLK_EXTENT,b.GEOM)='TRUE' 
      AND ROWNUM <= """ + str(numBlocksNeigh) + """) c,
     table (sdo_util.getvertices(sdo_pc_pkg.to_geometry(c.points,c.num_points,3,NULL))) pnt
WHERE ROWNUM <= """ + str(self.qp.num) + """ """ + dbops.getWhereStatement(zCondition) + """ 
ORDER BY (POWER((pnt.x - """ + str(self.qp.cx) + """),2) + POWER((pnt.y - """ + str(self.qp.cy) + """),2)) """            
        return query

    #
    # METHOD RELATED TO THE QUERIES OUT-OF-CORE PYTHON PARALLELIZATION 
    #
    def pythonParallelization(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        if self.parallelType == 'cand':
            idsQuery = "SELECT " + self.getParallelHint() + " BLK_ID FROM " + self.blockTable + ", " + self.queryTable + " WHERE SDO_FILTER(BLK_EXTENT,GEOM) = 'TRUE' AND id = " + str(self.queryIndex)
            (eTime, result) = dbops.genericQueryParallelCand(cursor,oracleops.mogrifyExecute, self.qp.columns, self.colsData, 
                                                             self.qp.statistics, self.resultTable, idsQuery, None, 
                                                             self.runGenericQueryParallelCandChild, self.numProcessesQuery)
            #returnDict[queryId] = self.genericQueryParallelCand()
        elif self.parallelType in ('grid','griddis'):
            gridTable = ('query_grid_' + str(self.queryIndex)).upper()
            oracleops.dropTable(cursor, gridTable, True)
            (eTime, result) =  dbops.genericQueryParallelGrid(cursor, oracleops.mogrifyExecute, self.qp.columns, self.colsData, 
                                                             self.qp.statistics, self.resultTable, gridTable, self.createGridTableMethod,
                                                             self.runGenericQueryParallelGridChild, self.numProcessesQuery, 
                                                             (self.parallelType == 'griddis'))
        connection.close()
        return (eTime, result)
         
    def runGenericQueryParallelCandChild(self, chunkIds):
        connection = self.getConnection()
        cursor = connection.cursor()
        zCondition = dbops.addZCondition(self.qp, 'pnt.z', None)
        
        elements = []
        for _,crange in groupby(chunkIds, lambda n, c=count(): n-next(c)):
            listcrange = list(crange)
            if len(listcrange) == 1:
                elements.append('(BLK_ID=' + str(listcrange[0])+ ')')
            else:
                elements.append('(BLK_ID between ' + str(listcrange[0]) + ' and ' + str(listcrange[-1])+')')      
                
        oracleops.mogrifyExecute(cursor, """INSERT INTO """ + self.resultTable + """ 
    SELECT """ + dbops.getSelectCols(self.qp.columns, {'x':'x','y':'y','z':'z'}, None) + """ FROM table ( sdo_PointInPolygon (
        cursor (SELECT """ + dbops.getSelectCols(self.columns, self.columnsNamesTypesDict, None, True) + """ FROM 
          (select points,num_points from """ + self.blockTable + """ WHERE """ + ' OR '.join(elements) + """) pcblob, 
          TABLE (sdo_util.getvertices(sdo_pc_pkg.to_geometry(pcblob.points,pcblob.num_points,3,NULL))) pnt """ + dbops.getWhereStatement(zCondition) + """),
        (select geom from """ + self.queryTable + """ where id = """ + str(self.queryIndex) + """), """ + str(self.tolerance) + """, NULL))""")
        connection.close()    

    def runGenericQueryParallelGridChild(self, index, gridTable):
        connection = self.getConnection()
        cursor = connection.cursor()
        zCondition = dbops.addZCondition(self.qp, 'pnt.z', None)
        query = """
INSERT INTO """ + self.resultTable + """ 
    SELECT """ + dbops.getSelectCols(self.qp.columns, self.columnsNamesTypesDict, None, True) + """ FROM 
        table (sdo_pc_pkg.clip_pc((SELECT pc FROM """ + self.baseTable + """),
                           (SELECT geom FROM """ + gridTable + """ WHERE id = """ + str(index) + """),
                           NULL,NULL,NULL,NULL)) pcblob, 
        table (sdo_util.getvertices(sdo_pc_pkg.to_geometry(pcblob.points,pcblob.num_points,3,NULL))) pnt """ + dbops.getWhereStatement(zCondition)  
        oracleops.mogrifyExecute(cursor, query)
        connection.close()    

