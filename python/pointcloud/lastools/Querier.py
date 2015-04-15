#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, time, logging, glob
from pointcloud.AbstractQuerier import AbstractQuerier
from pointcloud.lastools.CommonLASTools import CommonLASTools
from pointcloud import postgresops, lasops, utils

DEFAULT_INPUT_FILE_LIST_FILE_NAME = 'LIST_FILES'

class Querier(AbstractQuerier, CommonLASTools):   
    def __init__(self, configuration):
        """ Set configuration parameters"""
        AbstractQuerier.__init__(self, configuration)
        self.setVariables(configuration)
    
    def initialize(self):
        # Check whether DB already exist   
        connectionSuper = self.getConnection(True)
        cursorSuper = connectionSuper.cursor()
        cursorSuper.execute('SELECT datname FROM pg_database WHERE datname = %s', (self.dbName,))
        self.exists = cursorSuper.fetchone()
        cursorSuper.close()
        connectionSuper.close()
        # Creates the DB if not existing
        if not self.exists:
            logging.info('Creating auxiliary DB ' + self.dbName)
            connString = self.getConnectString(False, True)
            os.system('createdb ' + connString)
        #  We create the PostGIS extension
        connection = self.getConnection()
        cursor = connection.cursor()
        if not self.exists:
            cursor.execute('CREATE EXTENSION postgis;')
            connection.commit()
            
        logging.info('Getting default list of files and data SRID')
        # Get the list of PC files
        pcFiles = glob.glob(os.path.join(os.path.abspath(self.dataFolder), '*' + self.dataExtension))
        # We need to know if it is a single file or multiple (in order to use -merged in lasclip or not)
        self.isSingle = False
        if len(pcFiles) == 1:
            self.isSingle = True
        # Write the input file list to be used by lasmerge and lasclip
        inputListFile = open(DEFAULT_INPUT_FILE_LIST_FILE_NAME, 'w')
        for pcFile in pcFiles:
            inputListFile.write(pcFile + '\n')
        inputListFile.close()
        # Gets the SRID of the PC files (we assume all have the same SRID as the first file)
        #self.srid = lasops.getSRID(pcFiles[0])
        
        logging.info('Creating auxiliary table ' + utils.QUERY_TABLE)
        # Drops possible query table 
        postgresops.dropTable(cursor, utils.QUERY_TABLE, check = True)
        # Create query table
        cursor.execute("CREATE TABLE " +  utils.QUERY_TABLE + " (id integer, geom public.geometry(Geometry," + self.srid + "));")
        connection.commit()
        
        cursor.close()    
        connection.close()

    def query(self, queryId, iterationId, queriesParameters):
        (eTime, result) = (-1, None)
        queryIndex = int(queryId)
        
        self.qp = queriesParameters.getQueryParameters('psql', queryId, self.colsData)
        logging.debug(self.qp.queryKey)
        
        zquery = ''    
        if (self.qp.minz != None) or (self.qp.maxz != None):
            zconds = []
            if self.qp.minz != None:
                zconds.append(' -drop_z_below ' + str(self.qp.minz) + ' ')
            if self.qp.maxz != None:
                zconds.append(' -drop_z_above ' + str(self.qp.maxz) + ' ')
            zquery = ' '.join(zconds)
        
        connString = None        
        shapeFile = 'query' + str(queryIndex) + '.shp'
         
        if iterationId == 0:
            # We insert the polygon in the DB (to be used by lasclip ot by the DB index query) 
            cursor.execute("INSERT INTO " + utils.QUERY_TABLE + " VALUES (%s,ST_GeomFromEWKT(%s))", [queryIndex, 'SRID='+self.srid+';'+self.qp.wkt])
            connection.commit()
            
            if self.qp.queryType == 'generic':
                # We generate a ShapeFile for lasclip in case of not rectangle or circle
                query = "select ST_SetSRID(geom, " + self.srid + ") from " + utils.QUERY_TABLE + " where id = " + str(queryIndex) + ";"
                connString = ' '.join(('-h',self.dbHost,'-p',self.dbPort,'-u',self.userName,'-P',self.password,self.dbName))
                precommand = 'pgsql2shp -f ' + shapeFile + ' ' + connString + ' "' + query + '"'
                logging.info(precommand)
                os.system(precommand)

        if self.qp.queryType not in ('rectangle', 'circle', 'generic'):
            connection.close()
            return (eTime, result)
            
        t0 = time.time()

        if self.dbIndex:
            inputList = 'input' +  str(queryIndex) + '.list'
            connString = self.getConnectString(False, True)
            query = 'SELECT filepath FROM ' + self.lasIndexTableName + ',' + utils.QUERY_TABLE + ' where ST_Intersects( ' + utils.QUERY_TABLE + '.geom, ' + self.lasIndexTableName + '.geom ) and ' + utils.QUERY_TABLE + '.id = ' + str(queryIndex)
            prec = 'psql ' + connString + ' -t -A -c "' + query + '" > ' + inputList
            logging.info(prec)
            os.system(prec) 
        else:
            inputList = DEFAULT_INPUT_FILE_LIST_FILE_NAME
        
        if self.qp.queryType == 'rectangle':
            command = 'lasmerge -lof ' + inputList + ' -inside ' + str(self.qp.minx) + ' ' + str(self.qp.miny) + ' ' + str(self.qp.maxx) + ' ' + str(self.qp.maxy) + zquery
        elif self.qp.queryType == 'circle':
            command = 'lasmerge -lof ' + inputList + ' -inside_circle ' + str(self.qp.cx) + ' ' + str(self.qp.cy) + ' ' + str(self.qp.rad) + zquery
        elif self.qp.queryType == 'generic':
            command = 'lasclip.exe -lof ' + inputList + ' -poly ' + shapeFile + ' ' + zquery
            if not self.isSingle :
                command += ' -merged'                    
        
        if self.qp.queryMethod != 'disk': 
            outputFile = 'output' +  str(queryIndex) + '.' + self.outputExtension
            command += ' -o ' + outputFile
            logging.debug(command)
            os.system(command)
            eTime = time.time() - t0
            npointscommand = "lasinfo " + outputFile+ " -nc -nv -nco 2>&1 | grep 'number of point records:'"
            try:
                result  = int(utils.shellExecute(npointscommand).split()[-1])
            except:
                result = None
        elif self.qp.queryMethod != 'stream':
            
            command += ' -stdout -otxt -oparse xyz | wc -l'
            logging.debug(command)
            result = utils.shellExecute(command).replace('\n','')
            eTime = time.time() - t0
            try:
                result  = int(result)
            except:
                result = None                   
        else: # Statistical query
            try:
                outputFile = 'output' +  str(queryIndex) + '.' + self.outputExtension
                command += ' -o ' + outputFile
                logging.debug(command)
                os.system(command)
                
                options = ' -nv -nmm '
                for i in range(len(self.qp.statistics)):
                    if self.qp.statistics[i] == 'avg':
                        options += ' -histo ' + self.qp.columns[i] + " 10000000 "
                
                statcommand = "lasinfo -i " + outputFile +  options + " | grep 'min \|max\|average'"
                logging.info(statcommand)
                lines  = utils.shellExecute(statcommand).split('\n')
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
    connection.close()
    
    return (eTime, result)

    def close(self):
        return