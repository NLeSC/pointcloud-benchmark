#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, subprocess
from pointcloud.monetdb.AbstractQuerier import AbstractQuerier
from pointcloud.monetdb.CommonMonetDB import CommonMonetDB
from pointcloud import dbops, utils, monetdbops

class Querier(AbstractQuerier, CommonMonetDB):
    """MonetDB querier"""
    def query(self, queryId, iterationId, queriesParameters):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.prepareQuery(queryId, queriesParameters)
        self.dropTable(cursor, self.resultTable, True)    
        
        t0 = time.time()
        (query, queryArgs) = dbops.getSelect(self.qp, self.flatTable, self.addContainsCondition, self.colsData)
        
        if self.qp.queryMethod != 'stream': # disk or stat
            monetdbops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query + " WITH DATA", queryArgs)
            connection.commit()
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, True, self.qp.columns, self.qp.statistics)
            connection.close()
        else:
            sqlFileName = str(queryId) + '.sql'
            sqlFile = open(sqlFileName, 'w')
            sqlFile.write(monetdbops.mogrify(None, query, queryArgs) + ';\n')
            sqlFile.close()
            command = 'mclient ' + self.dbName + ' < ' + sqlFileName + ' | wc -l'
            result = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].replace('\n','')
            eTime = time.time() - t0
            try:
                result  = int(result) - 5
            except:
                result = None
        return (eTime, result)