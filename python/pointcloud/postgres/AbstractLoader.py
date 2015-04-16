#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging, psycopg2
from pointcloud.AbstractLoader import AbstractLoader as ALoader
from pointcloud.postgres.CommonPostgres import CommonPostgres
from pointcloud import utils, postgresops, lasops
from lxml import etree as ET

FILLFACTOR = 99

class AbstractLoader(ALoader, CommonPostgres):
    """Abstract class for the PostgreSQL loaders, some methods are already implemented"""
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        ALoader.__init__(self, configuration)
        self.setVariables(configuration)

        self.ordered = False

    def createDB(self):
        """ Creates the database. Delete any previous database with same name"""
        # Drop any previous DB and create a new one
        connectionSuper = self.getConnection(True)
        cursorSuper = connectionSuper.cursor()
        cursorSuper.execute('SELECT datname FROM pg_database WHERE datname = %s', (self.dbName,))
        exists = cursorSuper.fetchone()
        cursorSuper.close()
        connectionSuper.close()

        connString= self.getConnectionString(False, True)
        if exists:
            os.system('dropdb ' + connString)
        os.system('createdb ' + connString)
        
        connection = self.getConnection()
        cursor = connection.cursor()
        # Add PostGIS extension
        cursor.execute('CREATE EXTENSION postgis;')
        connection.commit()
        
        # Add create fishnet method used for the parallel queries using the PQGR method
        self.createGridFunction(cursor)
        connection.close()

    def initPointCloud(self, cursor):
        # Load PostGIS and PointCloud extensions (only if new DB is created)
        cursor.execute('CREATE EXTENSION pointcloud')
        cursor.execute('CREATE EXTENSION pointcloud_postgis')
        # Add columns to pointcloud_format table to monitor the used offsets and scales
        cursor.execute('ALTER TABLE pointcloud_formats ADD COLUMN scalex double precision')
        cursor.execute('ALTER TABLE pointcloud_formats ADD COLUMN scaley double precision')
        cursor.execute('ALTER TABLE pointcloud_formats ADD COLUMN scalez double precision')
        cursor.execute('ALTER TABLE pointcloud_formats ADD COLUMN offsetx double precision')
        cursor.execute('ALTER TABLE pointcloud_formats ADD COLUMN offsety double precision')
        cursor.execute('ALTER TABLE pointcloud_formats ADD COLUMN offsetz double precision')
        cursor.execute('ALTER TABLE pointcloud_formats ADD CONSTRAINT scale_offset_key UNIQUE (srid,scalex,scaley,scalez,offsetx,offsety,offsetz)')
        cursor.connection.commit()
        
    def createBlocksTable(self, cursor, blockTable, tableSpace, quadcell = False):
        aux = ''
        if quadcell:
            aux = ",quadCellId BIGINT"
        postgresops.mogrifyExecute(cursor, "CREATE TABLE " + blockTable + " (id SERIAL PRIMARY KEY,pa PCPATCH" + aux + ")" + self.getTableSpaceString(tableSpace))

    def createGridFunction(self, cursor):
        cursor.execute("""
    CREATE OR REPLACE FUNCTION ST_CreateFishnet(
    nrow integer, ncol integer,
    xsize float8, ysize float8,
    x0 float8 DEFAULT 0, y0 float8 DEFAULT 0,
    OUT "row" integer, OUT col integer,
    OUT geom geometry)
    RETURNS SETOF record AS
    $$
    SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * $3 + $5, i * $4 + $6) AS geom
    FROM generate_series(0, $1 - 1) AS i,
         generate_series(0, $2 - 1) AS j,
    (
    SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS cell
    ) AS foo;
    $$ LANGUAGE sql IMMUTABLE STRICT""")
        cursor.connection.commit()
    
    def createQuadCellId(self, cursor):
        cursor.execute("""
CREATE OR REPLACE FUNCTION QuadCellId(IN bigint, IN integer, OUT f1 bigint)
    AS $$SELECT (((1 << ($2<<1)) - 1) << (64 - ($2<<1))) & $1;$$ 
    LANGUAGE SQL 
    IMMUTABLE       
    RETURNS NULL ON NULL INPUT""")
        cursor.connection.commit()
    
    def getTableSpaceString(self, tableSpace):
        tableSpaceString = ''
        if tableSpace != '':
            tableSpaceString = ' TABLESPACE ' + tableSpace
        return tableSpaceString
    
    def getIndexTableSpaceString(self, indexTableSpace):
        indexTableSpaceString = ''
        if indexTableSpace != '':
            indexTableSpaceString = ' TABLESPACE ' + indexTableSpace
        return indexTableSpaceString
     
    def createFlatTable(self, cursor, flatTable, tableSpace, columns):
        cols = []
        for c in columns:
            if c not in self.colsData:
                raise Exception('Wrong column!' + c)
            cols.append(self.colsData[c][0] + ' ' + self.colsData[c][1])
        
        # Create the flat table that will contain all the data
        postgresops.mogrifyExecute(cursor, """CREATE TABLE """ + flatTable + """ (
        """ + (',\n'.join(cols)) + """)""" + self.getTableSpaceString(tableSpace))
    
    def createMetaTable(self, cursor, metaTable):
        postgresops.mogifyExecute(cursor, "CREATE TABLE " + metaTable + " (tablename text, srid integer, minx DOUBLE PRECISION, miny DOUBLE PRECISION, maxx DOUBLE PRECISION, maxy DOUBLE PRECISION, scalex DOUBLE PRECISION, scaley DOUBLE PRECISION)")
        
    def indexFlatTable(self, cursor, flatTable, indexTableSpace, index, cluster = False):
        if index in ('xy', 'xyz'):
            indexName = flatTable + "_" + index + "_btree_idx"
            postgresops.mogrifyExecute(cursor, "create index " + indexName + " on " + flatTable + " (" + (','.join(index)) + ") WITH (FILLFACTOR=" + str(FILLFACTOR) + ")" + self.getIndexTableSpaceString(indexTableSpace))
        elif index == 'k':
            mortonIndexName = flatTable + "_morton_btree_idx"
            postgresops.mogrifyExecute(cursor, "create index " + indexName + " on " + flatTable + " (morton2D) WITH (FILLFACTOR=" + str(FILLFACTOR) + ")" + self.getIndexTableSpaceString(indexTableSpace))
        if cluster:
            postgresops.mogrifyExecute(cursor, "CLUSTER " + flatTable + " USING " + indexName)
        #self.vacuumTable(cursor, flatTable)
        
    def indexBlockTable(self, cursor, blockTable, indexTableSpace, quadcell = False, cluster = False):
        if quadcell:
            indexName = self.blockTable + "_btree"
            postgresops.mogrifyExecute(cursor, 'CREATE INDEX ' + indexName + ' ON ' + blockTable + ' (quadCellId)' + self.getIndexTableSpaceString(indexTableSpace))
        else:
            indexName = blockTable + "_gist"
            postgresops.mogrifyExecute(cursor, 'CREATE INDEX ' + indexName + ' ON ' + blockTable + ' USING GIST ( geometry(pa) )' + self.getIndexTableSpaceString(indexTableSpace))
        if cluster:
            postgresops.mogrifyExecute(cursor, "CLUSTER " + blockTable + " USING " + indexName)   
        # Close the connection
        #self.vacuumTable(cursor, blockTable)
    
    def vacuumTable(self, cursor, tableName):
        old_isolation_level = connection.isolation_level
        connection.set_isolation_level(0)
        postgresops.mogrifyExecute(cursor, "VACUUM FULL ANALYZE " + tableName)
        connection.set_isolation_level(old_isolation_level)

    def size(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        row = postgresops.getSizes(cursor)
        for i in range(len(row)):
            if row[i] != None:
                row[i] = '%.3f MB' % row[i]
        (size_indexes, size_ex_indexes, size_total) = row
        cursor.close()
        connection.close()
        return ' Size indexes= ' + str(size_indexes) + '. Size excluding indexes= ' + str(size_ex_indexes) + '. Size total= ' + str(size_total)
    
    def getNumPointsFlat(self, flatTable):
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute('select count(*) from ' + flatTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n
    
    def getNumPointsBlocks(self, blockTable):
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute('select sum(pc_numpoints(pa)) from ' + blockTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n
        
    def addPCFormat(self, cursor, schemaFile, fileAbsPath, srid):
        (_, _, _, _, _, _, _, _, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ) = lasops.getPCFileDetails(fileAbsPath, srid)

        updatedFormat = False
        schema = None
        
        pc_namespace = '{http://pointcloud.org/schemas/PC/}'
        
        while not updatedFormat:
        
            # Check whether there is already a format with current scale-offset values 
            cursor.execute("SELECT pcid,schema FROM pointcloud_formats WHERE srid = %s AND scaleX = %s AND scaleY = %s AND scaleZ = %s AND offsetX = %s AND offsetY = %s AND offsetZ = %s", 
                            [srid, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ])
            rows = cursor.fetchall()
            
            if len(rows):
                # There is already a format with these scale-offset values
                [pcid,schema] = rows[0]
                root = ET.fromstring(schema)  
                updatedFormat = True            
            else:
                if schema == None:
                    # There is not a format with these scale-offset values. We add a schema for these ones
                    # Get ElementTree of the XML schema file
                    tree = ET.parse(schemaFile)
                    root = tree.getroot()
                    
                    offsets = {'x':offsetX, 'y':offsetY, 'z':offsetZ}
                    scales = {'x':scaleX, 'y':scaleY, 'z':scaleZ}
                    for dimension in root.findall(pc_namespace+'dimension'):
                        dimName = dimension.find(pc_namespace+'name').text
                        if dimName.lower() in offsets:
                            dimension.find(pc_namespace+'offset').text = str(offsets[dimName.lower()])
                            dimension.find(pc_namespace+'scale').text = str(scales[dimName.lower()])
                    
                    schema = '<?xml version="1.0" encoding="UTF-8"?>' + '\n' + ET.tostring(tree, encoding='utf8', method='xml')
                
                cursor.execute("SELECT max(pcid) FROM pointcloud_formats")
                rows = cursor.fetchall()
                pcid = 1
                if len(rows) and rows[0][0] != None:
                    pcid = rows[0][0] + 1
                try:
                    postgresops.mogrifyExecute(cursor, "INSERT INTO pointcloud_formats (pcid, srid, schema, scalex, scaley, scalez, offsetx, offsety, offsetz) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                               [pcid, srid, schema, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ])
                    updatedFormat = True
                except :
                    cursor.connection.rollback() 
        
        # Get the used dimensions if still not acquired (they should be always the same)
        compression = root.find(pc_namespace+'metadata').find('Metadata').text
        dimensionsNames = []
        for dimension in root.findall(pc_namespace+'dimension'):
            dName = dimension.find(pc_namespace+'name').text
            dimensionsNames.append(dName)
        return (dimensionsNames, pcid, compression)  
    
    def loadFromBinaryLoader(self, connectionString, flatTable, fileAbsPath, columns, minX = None, minY = None, scaleX = None, scaleY = None):
        c1 = 'las2pg -s '+ fileAbsPath +' --stdout --parse ' + columns
        if 'k' in columns:
            c1 += ' --moffset ' + str(int(float(minX) / float(scaleX))) + ','+ str(int(float(minY) / float(scaleY))) + ' --check ' + str(scaleX) + ',' + str(scaleY)        
        c = c1 + ' | psql '+ connectionString +' -c "copy '+ flatTable +' from stdin with binary"'
        logging.debug(c)
        os.system(c)
