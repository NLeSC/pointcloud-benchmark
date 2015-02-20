#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, copy
from pointcloud import dbops
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class Querier(AbstractQuerier):        
    def query(self, queryId, iterationId, queriesParameters):
        connection = self.connect()
        cursor = connection.cursor()
    
        self.prepareQuery(queryId, queriesParameters, iterationId == 0)
        gridTable = ('query_grid_' + str(self.queryIndex)).upper()
        
        for table in (self.resultTable, gridTable):
            self.dropTable(cursor, table, True) 
    
        if self.parallelType in ('grid', 'griddis'):
            if self.qp.queryType in ('rectangle','circle','generic'):            
                (eTime, result) = dbops.genericQueryParallelGrid(cursor, self.mogrifyExecute, self.qp.columns, self.colsData, 
                                                                     self.qp.statistics, self.resultTable, gridTable, self.createGridTableMethod,
                                                                     self.runGenericQueryParallelGridChild, self.numProcessesQuery, 
                                                                     (self.parallelType == 'griddis'))
        else:
            t0 = time.time()
            (query, _) = dbops.getSelect(self.qp, self.flatTable, self.addContainsCondition, self.colsData, self.getParallelHint())
            self.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query)
            connection.commit()
                              
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, True, self.qp.columns, self.qp.statistics) 
        connection.close()
        return (eTime, result)

    def runGenericQueryParallelGridChild(self, sIndex, gridTable):
        connection = self.connect()
        cursor = connection.cursor()
        self.queryIndex = sIndex
        self.queryTable = gridTable
        cqp = copy.copy(self.qp)
        cqp.queryType = 'generic'
        if self.qp.queryType == 'rectangle':
            cqp.queryType = 'rectangle'
        cqp.statistics = None
        cqp.minx = "'||to_char(bbox.sdo_ordinates(1))||'"
        cqp.maxx = "'||to_char(bbox.sdo_ordinates(3))||'" 
        cqp.miny = "'||to_char(bbox.sdo_ordinates(2))||'"
        cqp.maxy = "'||to_char(bbox.sdo_ordinates(4))||'" 
        (query, _) = dbops.getSelect(cqp, self.flatTable, self.addContainsCondition, self.colsData)
        self.mogrifyExecute(cursor, """
DECLARE
  bbox sdo_geometry;
BEGIN
  select sdo_geom_mbr (geom) into bbox from """ + gridTable + """ where id = """ + str(sIndex) + """;
  execute immediate 'INSERT INTO """ + self.resultTable + """ """ + query + """';
END;""")
        connection.commit()
        connection.close()
