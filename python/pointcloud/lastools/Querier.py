#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, subprocess, time, psycopg2, logging
from pointcloud.AbstractQuerier import AbstractQuerier
from pointcloud.lastools.CommonLASTools import CommonLASTools

class Querier(AbstractQuerier, CommonLASTools):   
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        self.setVariables(configuration)
        
    def initialize(self):
        # Check whether DB already exist   
        connectionSuper = self.connect(True)
        cursorSuper = connectionSuper.cursor()
        cursorSuper.execute('SELECT datname FROM pg_database WHERE datname = %s', (self.dbName,))
        self.exists = cursorSuper.fetchone()
        cursorSuper.close()
        connectionSuper.close()
        # Creates the DB if not existing
        if not self.exists:
            connString = self.connectString(False, True)
            os.system('createdb ' + connString)
        
        #  Make a new connection
        connection = self.connect()
        cursor = connection.cursor()
        if not self.exists:
            cursor.execute('CREATE EXTENSION postgis;')
            connection.commit()
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ( self.queryTable, ))
        if cursor.fetchone()[0]:
            cursor.execute('DROP TABLE ' +  self.queryTable)
            connection.commit()
        cursor.execute("CREATE TABLE " +  self.queryTable + " (id integer, geom public.geometry(Geometry," + self.srid + "));")
        connection.commit()
        cursor.close()    
        connection.close()
        
        self.inputList = 'LIST_FILES'
        os.system('ls ' + self.dataFolder + '/*' + self.dataExtension + ' > ' + self.inputList)

    def query(self, queryId, iterationId, queriesParameters):
    
        connection = self.connect()
        cursor = connection.cursor()
        queryIndex = int(queryId)
        
        self.qp = queriesParameters.getQueryParameters('psql', queryId, self.colsData)
        wkt = queriesParameters.getWKT(queriesParameters.getQuery(queryId))
        logging.debug(self.qp.queryKey)
        
        zquery = ''    
        if (self.qp.minz != None) or (self.qp.maxz != None):
            zconds = []
            if self.qp.minz != None:
                zconds.append(' -drop_z_below ' + str(self.qp.minz) + ' ')
            if self.qp.maxz != None:
                zconds.append(' -drop_z_above ' + str(self.qp.maxz) + ' ')
            zquery = ' '.join(zconds)
        
        if iterationId == 0:
            cursor.execute("INSERT INTO " + self.queryTable + " VALUES (%s,ST_GeomFromEWKT(%s))", [queryIndex, 'SRID='+self.srid+';'+wkt])
            connection.commit()
        
            precommand = 'pgsql2shp -f query' + str(queryIndex) + '.shp -h '+ self.dbHost +' -p '+ self.dbPort + ' -u '+ self.userName +' -P '+ self.password +' '+ self.dbName +' "select ST_SetSRID(geom, ' + self.srid + ') from ' + self.queryTable + ' where id = ' + str(queryIndex) + ';"'
            logging.info(precommand)
            os.system(precommand)
        
        if self.dbIndex:
            connString = self.connectString(False, True)
        
        eTime = -1
        result = None
        
        if self.qp.queryType in ('rectangle', 'circle', 'generic'):
            t0 = time.time()
            
            auxnp = ''
            if self.numProcessesQuery > 1:
                auxnp = ' -cores ' + str(self.numProcessesQuery) + ' '
    
            outputFile = 'output' +  str(queryIndex) + '.' + self.outputExtension
            
            if self.dbIndex:
                inputList = 'input' +  str(queryIndex) + '.list'
                prec = 'psql ' + connString + ' -t -A -c "select filepath from ' + self.lasIndexTableName + ',' + self.queryTable + ' where ST_Intersects( ' + self.queryTable + '.geom, ' + self.lasIndexTableName + '.geom ) and ' + self.queryTable + '.id = ' + str(queryIndex) + '" > ' + inputList
                logging.info(prec)
                os.system(prec) 
            else:
                inputList = self.inputList
            if self.qp.queryType in ('rectangle', 'circle'):
                if self.qp.queryType == 'rectangle':
                    command = 'lasmerge -lof ' + inputList + ' -inside ' + str(self.qp.minx) + ' ' + str(self.qp.miny) + ' ' + str(self.qp.maxx) + ' ' + str(self.qp.maxy) + zquery + ' -o ' + outputFile
                else:
                    command = 'lasmerge -lof ' + inputList + ' -inside_circle ' + str(self.qp.cx) + ' ' + str(self.qp.cy) + ' ' + str(self.qp.rad) + zquery + ' -o ' + outputFile
            elif self.qp.queryType == 'generic':
                command = 'lasclip.exe -lof ' + inputList + ' -poly query' + str(queryIndex) + '.shp ' + auxnp + zquery + ' -o ' + outputFile
                if not self.isSingle :
                    command += ' -merged'                    
                    
            logging.info(command)
            os.system(command)
            
            if self.qp.statistics != None:
                try:
                    options = ' -nv -nmm '
                    for i in range(len(self.qp.statistics)):
                        if self.qp.statistics[i] == 'avg':
                            options += ' -histo ' + self.qp.columns[i] + " 10000000 "
                    
                    statcommand = "lasinfo -i " + outputFile +  options + " | grep 'min \|max\|average'"
                    logging.info(statcommand)
                    lines  = subprocess.Popen(statcommand, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].split('\n')
                    results= []
                    colIs = {'x':4, 'y':5, 'z':6}
                    for i in range(len(self.qp.statistics)):
                        for line in lines:
                            if line.count(self.qp.statistics[i]) and line.count(self.qp.columns[i]):
                                if self.qp.statistics[i] == 'avg':
                                    results.append(line.split()[-1])
                                else:
                                    results.append(line).split()[colIs[self.qp.columns[i]]]
                    result = ','.join(results)
                except:
                    result = None
                eTime = time.time() - t0
            else:
            
                eTime = time.time() - t0
                npointscommand = "lasinfo " + outputFile+ " -nc -nv -nco 2>&1 | grep 'number of point records:'"
                try:
                    result  = int(subprocess.Popen(npointscommand, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].split()[-1])
                except:
                    result = None
        connection.close()
        
        return (eTime, result)

    def queryMulti(self, queryId, iterationId, queriesParameters):
        connection = self.connect()
        cursor = connection.cursor()
        queryIndex = int(queryId)
        
        self.qp = queriesParameters.getQueryParameters('psql', queryId, self.colsData)
        wkt = queriesParameters.getWKT(queriesParameters.getQuery(queryId))

        zquery = ''    
        if (self.qp.minz != None) or (self.qp.maxz != None):
            zconds = []
            if self.qp.minz != None:
                zconds.append(' -drop_z_below ' + str(self.qp.minz) + ' ')
            if self.qp.maxz != None:
                zconds.append(' -drop_z_above ' + str(self.qp.maxz) + ' ')
            zquery = ' '.join(zconds)
        
        if iterationId == 0 and self.qp.queryType == 'generic':
            cursor.execute("INSERT INTO " + self.queryTable + " VALUES (%s,ST_GeomFromEWKT(%s))", [queryIndex, 'SRID='+self.srid+';'+wkt])
            connection.commit()
        
            precommand = 'pgsql2shp -f query' + str(queryIndex) + '.shp -h '+ self.dbHost +' -p '+ self.dbPort + ' -u '+ self.userName +' -P '+ self.password +' '+ self.dbName +' "select ST_SetSRID(geom, ' + self.srid + ') from ' + self.queryTable + ' where id = ' + str(queryIndex) + ';"'
            logging.debug(precommand)
            os.system(precommand)
        
        if self.dbIndex:
            connString = self.connectString(False, True)
        
        t0 = time.time()
        
        if self.dbIndex:
            inputList = 'input' +  str(queryIndex) + '.list'
            prec = 'psql ' + connString + ' -t -A -c "select filepath from ' + self.lasIndexTableName + ',' + self.queryTable + ' where ST_Intersects( ' + self.queryTable + '.geom, ' + self.lasIndexTableName + '.geom ) and ' + self.queryTable + '.id = ' + str(queryIndex) + '" > ' + inputList
            logging.debug(prec)
            os.system(prec) 
        else:
            inputList = self.inputList
            
        if self.qp.queryType in ('rectangle', 'circle'):
            if self.qp.queryType == 'rectangle':
                command = 'lasmerge -lof ' + inputList + ' -inside ' + str(self.qp.minx) + ' ' + str(self.qp.miny) + ' ' + str(self.qp.maxx) + ' ' + str(self.qp.maxy) + zquery + ' -stdout -otxt -oparse xyz | wc -l'
            else:
                command = 'lasmerge -lof ' + inputList + ' -inside_circle ' + str(self.qp.cx) + ' ' + str(self.qp.cy) + ' ' + str(self.qp.rad) + zquery + ' -stdout -otxt -oparse xyz | wc -l'
        elif self.qp.queryType == 'generic':
            command = 'lasclip.exe -lof ' + inputList + ' -poly query' + str(queryIndex) + '.shp ' + zquery + ' -stdout -otxt -oparse xyz | wc -l' 
            if not self.isSingle :
                command += ' -merged'                    
        logging.debug(command)
        result = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].replace('\n','')
        eTime = time.time() - t0
        try:
            result  = int(result)
        except:
            result = -1
        return (eTime, result)
        
    def close(self):
        return

    def connect(self, superUser = False):
        return psycopg2.connect(self.connectString(superUser))
