#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, subprocess
from pointcloud.monetdb.AbstractQuerier import AbstractQuerier
from pointcloud.monetdb.CommonMonetDB import CommonMonetDB
from pointcloud import dbops, monetdbops

class Querier(AbstractQuerier, CommonMonetDB):
    """MonetDB querier"""
    def query(self, queryId, iterationId, queriesParameters):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.prepareQuery(queryId, queriesParameters)
        monetdbops.dropTable(cursor, self.resultTable, True)    
        
        t0 = time.time()
        (query, queryArgs) = dbops.getSelect(self.qp, self.flatTable, self.addContainsCondition, self.colsData)
        
        if self.qp.queryMethod != 'stream': # disk or stat
            monetdbops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query + " WITH DATA", queryArgs)
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, True, self.qp.columns, self.qp.statistics)
        else:
            sqlFileName = str(queryId) + '.sql'
            monetdbops.createSQLFile(sqlFileName, query, queryArgs)
            result = monetdbops.executeSQLFileCount(self.dbName, sqlFileName)
            eTime = time.time() - t0
        connection.close()
        return (eTime, result)