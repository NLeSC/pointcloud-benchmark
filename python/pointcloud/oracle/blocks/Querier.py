#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, math, subprocess
from itertools import groupby, count
from pointcloud import dbops, utils, oracleops
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class Querier(AbstractQuerier):      
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        # Create the quadtree
        connection = self.getConnection()
        cursor = connection.cursor()
        
        oracleops.mogrifyExecute(cursor, "SELECT srid FROM user_sdo_geom_metadata WHERE table_name = '" + self.blocksTable + "'")
        (self.srid,) = cursor.fetchone()[0]
          
    def queryDisk(self, queryId, iterationId, queriesParameters):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.columnsNamesTypesDict = {}
        for col in self.columns:
            self.columnsNamesTypesDict[col] = ('pnt.' + col, 'NUMBER')
        
        self.prepareQuery(queryId, queriesParameters, iterationId == 0)
        gridTable = ('query_grid_' + str(self.queryIndex)).upper()
        
        for table in (self.resultTable, gridTable):
            oracleops.dropTable(cursor, table, True) 
            
        if self.qp.queryType in ('rectangle','circle','generic') :
            if self.numProcessesQuery == 1:
                (eTime, result) =  self.genericQuerySingle(cursor)
            else:
                if self.parallelType == 'nati':
                    (eTime, result) = self.genericQuerySingle(cursor, True)
                elif self.parallelType == 'cand':
                    idsQuery = "SELECT " + self.getParallelHint() + " BLK_ID FROM " + self.blockTable + ", " + self.queryTable + " WHERE SDO_FILTER(BLK_EXTENT,GEOM) = 'TRUE' AND id = " + str(self.queryIndex)
                    (eTime, result) = dbops.genericQueryParallelCand(cursor,oracleops.mogrifyExecute, self.qp.columns, self.colsData, 
                                                                     self.qp.statistics, self.resultTable, idsQuery, None, 
                                                                     self.runGenericQueryParallelCandChild, self.numProcessesQuery)
                    #returnDict[queryId] = self.genericQueryParallelCand()
                elif self.parallelType in ('grid','griddis'):
                    (eTime, result) =  dbops.genericQueryParallelGrid(cursor, oracleops.mogrifyExecute, self.qp.columns, self.colsData, 
                                                                     self.qp.statistics, self.resultTable, gridTable, self.createGridTableMethod,
                                                                     self.runGenericQueryParallelGridChild, self.numProcessesQuery, 
                                                                     (self.parallelType == 'griddis'))
        elif self.qp.queryType == 'nn' :
            if self.numProcessesQuery == 1:
                (eTime, result) = self.nearneighQuerySingle(cursor)
            elif self.parallelType == 'nati':
                (eTime, result) = self.nearneighQuerySingle(cursor, True)
        connection.close()
        return (eTime, result)
            
    def queryStream(self, queryId, iterationId, queriesParameters):
        self.columnsNamesTypesDict = {}
        for col in self.columns:
            self.columnsNamesTypesDict[col] = ('pnt.' + col, 'NUMBER')
        
        self.prepareQuery(queryId, queriesParameters, iterationId == 0)
        
        connection = self.getConnection()
        cursor = connection.cursor()
        t0 = time.time()
        query = self.getSelect()
        sqlFileName = str(queryId) + '.sql'
        sqlFile = open(sqlFileName, 'w')
        sqlFile.write('set linesize 120\n')
        sqlFile.write('set trimout on\n')
        sqlFile.write('set pagesize 0\n')
        sqlFile.write(oracleops.mogrify(cursor, query) + ';\n')
        sqlFile.close()
        command = 'sqlplus -s ' + self.connectString(False) + ' < ' + sqlFileName + ' | wc -l'
        result = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].replace('\n','')
        eTime = time.time() - t0
        try:
            result  = int(result) - 3
        except:
            result = -1
        return (eTime, result)
    
    
    def getSelect(self):
        zCondition = dbops.addZCondition(self.qp, 'pnt.z', None)
        
        query = """SELECT """ + self.getParallelHint() + """ """ + dbops.getSelectCols(self.qp.columns, self.columnsNamesTypesDict, self.qp.statistics) + """ FROM 
        table (sdo_pc_pkg.clip_pc((SELECT pc FROM """ + self.baseTable + """),
                                  (SELECT geom FROM """ + self.queryTable + """ WHERE id = """ + str(self.queryIndex) + """),NULL,NULL,NULL,NULL)) pcblob, 
        table (sdo_util.getvertices(sdo_pc_pkg.to_geometry(pcblob.points,pcblob.num_points,3,NULL))) pnt""" + dbops.getWhereStatement(zCondition)
        return query
        
    def genericQuerySingle(self, cursor, nativeParallel = False):
        t0 = time.time()
        
        oracleops.mogrifyExecute(cursor, "CREATE TABLE " + self.resultTable + " AS " + self.getSelect())
        cursor.connection.commit()
        
        return dbops.getResult(cursor, t0, self.resultTable, None, True, self.qp.columns, self.qp.statistics)
         
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
        connection.commit()
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
        connection.commit()
        connection.close()    

    def nearneighQuerySingle(self, cursor, nativeParallel = False):
        t0 = time.time()
        #self.createResultTable(self.columns)
        numBlocksNeigh = int(math.pow(2 + math.ceil(math.sqrt(math.ceil(float(self.qp.num)/float(self.blockSize)))), 2))
        parallelHint = self.getParallelHint()
        oracleops.mogrifyExecute(cursor, """
CREATE TABLE """ + self.resultTable + """ AS
    SELECT """ + parallelHint + """ """ + dbops.getSelectCols(self.qp.columns, self.columnsNamesTypesDict, self.qp.statistics) + """ FROM 
        (
            SELECT """ + parallelHint + """ a.points, a.num_points FROM """ + self.blockTable + """ a, """ + self.queryTable + """ b where b.ID = """ + str(self.queryIndex) + """ AND SDO_NN(a.BLK_EXTENT,b.GEOM)='TRUE' AND ROWNUM <= """ + str(numBlocksNeigh) + """
        ) c,
        table (sdo_util.getvertices(sdo_pc_pkg.to_geometry(c.points,c.num_points,3,NULL))) pnt
    WHERE ROWNUM <= """ + str(self.qp.num) + """
    ORDER BY (POWER((pnt.x - """ + str(self.qp.cx) + """),2) + POWER((pnt.y - """ + str(self.qp.cy) + """),2)) """)
        cursor.connection.commit()
        
        return dbops.getResult(cursor, t0, self.resultTable, None, True, None, None)
