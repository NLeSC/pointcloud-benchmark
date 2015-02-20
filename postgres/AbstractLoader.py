#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os
import psycopg2
from pointcloud.AbstractLoader import AbstractLoader as ALoader
from pointcloud.postgres.CommonPostgres import CommonPostgres
from pointcloud import utils
from lxml import etree as ET

#XML_NAMESPACES = {'pc':'http://pointcloud.org/schemas/PC/1.1',
#              'xsi':'http://www.w3.org/2001/XMLSchema-instance'}
#for namespace in XML_NAMESPACES:
#    ET.register_namespace(namespace,XML_NAMESPACES[namespace])

class AbstractLoader(ALoader, CommonPostgres):
    """Abstract class for the PostgreSQL loaders, some methods are already implemented"""
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        ALoader.__init__(self, configuration)
        self.setVariables(configuration)

        self.ordered = False
        
    def connect(self, superUser = False):
        return psycopg2.connect(self.connectString(superUser))
        
    def size(self):
        connection = self.connect()
        cursor = connection.cursor()
        row = utils.getPostgresSizes(cursor)
        for i in range(len(row)):
            if row[i] != None:
                row[i] = '%.3f MB' % row[i]
        (size_indexes, size_ex_indexes, size_total) = row
        cursor.close()
        connection.close()
        return ' Size indexes= ' + str(size_indexes) + '. Size excluding indexes= ' + str(size_ex_indexes) + '. Size total= ' + str(size_total)

    def createDB(self):
        """ Creates the database. Delete any previous database with same name"""
        if self.cDB:
            # Drop any previous DB and create a new one
            connectionSuper = self.connect(True)
            cursorSuper = connectionSuper.cursor()
            cursorSuper.execute('SELECT datname FROM pg_database WHERE datname = %s', (self.dbName,))
            exists = cursorSuper.fetchone()
            cursorSuper.close()
            connectionSuper.close()
    
            connString= self.connectString(False, True)
            if exists:
                os.system('dropdb ' + connString)
            os.system('createdb ' + connString)
            
            connection = self.connect()
            cursor = connection.cursor()
            # Add PostGIS extension
            cursor.execute('CREATE EXTENSION postgis;')
            connection.commit()
            
            # Add create fishnet method used for the parallel queries using the PQGR method
            self.createGridFunction(cursor)
            connection.close()
            
    def initPointCloud(self):
        if self.cDB:
            connection = self.connect()
            cursor = connection.cursor()
        
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
            cursor.execute('ALTER TABLE pointcloud_formats ADD CONSTRAINT scale_offset_key UNIQUE (scalex,scaley,scalez,offsetx,offsety,offsetz)')
            connection.commit()
            connection.close()
        
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
    
    def createQuadCellId(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute("""
CREATE OR REPLACE FUNCTION QuadCellId(IN bigint, IN integer, OUT f1 bigint)
    AS $$SELECT (((1 << ($2<<1)) - 1) << (64 - ($2<<1))) & $1;$$ 
    LANGUAGE SQL 
    IMMUTABLE       
    RETURNS NULL ON NULL INPUT""")
        connection.commit()
        connection.close()
    
    def getTableSpaceString(self):
        tableSpaceString = ''
        if self.tableSpace != '':
            tableSpaceString = ' TABLESPACE ' + self.tableSpace
        return tableSpaceString
    
    def getIndexTableSpaceString(self):
        indexTableSpaceString = ''
        if self.indexTableSpace != '':
            indexTableSpaceString = ' TABLESPACE ' + self.indexTableSpace
        return indexTableSpaceString
    
    def process(self):
        inputFiles = utils.getFiles(self.inputFolder, self.extension)[self.fileOffset:]
        return self.processMulti(inputFiles, self.numProcessesLoad, self.loadFromFile, self.loadFromFileSequential, self.ordered)

    def loadFromFile(self, index, fileAbsPath):
        """ Process the input data """
        raise NotImplementedError( "Should have implemented this" )
    
    def loadFromFileSequential(self, fileAbsPath, index, numFiles):
        return None
     
    def createFlat(self, flatTable, columns):
        connection = self.connect()
        cursor = connection.cursor()
        cols = []
        for c in columns:
            if c not in self.colsData:
                raise Exception('Wrong column!' + c)
            cols.append(self.colsData[c][0] + ' ' + self.colsData[c][1])
        
        # Create the flat table that will contain all the data
        self.mogrifyExecute(cursor, """CREATE TABLE """ + flatTable + """ (
        """ + (',\n'.join(cols)) + """)""" + self.getTableSpaceString())
        connection.commit()
        connection.close()
    
    def createBlocks(self, blockTable, quadcell = False):
        aux = ''
        if quadcell:
            aux = ",quadCellId BIGINT"
        connection = self.connect()
        cursor = connection.cursor()
        self.mogrifyExecute(cursor, "CREATE TABLE " + blockTable + " (id SERIAL PRIMARY KEY,pa PCPATCH" + aux + ")" + self.getTableSpaceString())
        connection.commit()
        connection.close()  
    
    
    def indexClusterVacuumFlat(self, flatTable, index):
        connection = self.connect()
        cursor = connection.cursor()
        
        if index in ('gxy','gxyz'):
            gistIndexName = flatTable + "_" + index + "_gist_idx"
            auxindex = index.replace('g','')
            cursor.execute("create view " + self.viewName + " as select st_setSRID(st_makepoint(" + (','.join(auxindex)) + ")," + self.srid + ") as " + index + ", x, y, z from " + flatTable + ";")
            connection.commit()
            self.mogrifyExecute(cursor, "create index " + gistIndexName + " on " + flatTable + " using gist (st_setSRID(st_makepoint(" + (','.join(auxindex)) + ")," + self.srid + ")) WITH (FILLFACTOR=" + str(self.fillFactor) + ")" + self.getIndexTableSpaceString())
            connection.commit()
            if self.cluster:
                self.mogrifyExecute(cursor, "CLUSTER " + flatTable + " USING " + gistIndexName)
        elif index in ('xy', 'xyz'):
            btreeIndexName = flatTable + "_" + index + "_btree_idx"
            self.mogrifyExecute(cursor, "create index " + btreeIndexName + " on " + flatTable + " (" + (','.join(index)) + ") WITH (FILLFACTOR=" + str(self.fillFactor) + ")" + self.getIndexTableSpaceString())
            connection.commit()
            if self.cluster:
                self.mogrifyExecute(cursor, "CLUSTER " + flatTable + " USING " + btreeIndexName)
        elif index == 'k':
            mortonIndexName = flatTable + "_morton_btree_idx"
            self.mogrifyExecute(cursor, "create index " + mortonIndexName + " on " + flatTable + " (morton2D) WITH (FILLFACTOR=" + str(self.fillFactor) + ")" + self.getIndexTableSpaceString())
            connection.commit()
            if self.cluster:
                self.mogrifyExecute(cursor, "CLUSTER " + flatTable + " USING " + mortonIndexName)
        connection.commit()
        connection.close()
        
        if self.vacuum:
            self.vacuumTable(flatTable)
        
    def indexClusterVacuumBlock(self, blockTable, quadcell = False):
        connection = self.connect()
        cursor = connection.cursor()
        if quadcell:
            indexName = self.blockTable + "_btree"
            self.mogrifyExecute(cursor, 'CREATE INDEX ' + indexName + ' ON ' + blockTable + ' (quadCellId)' + self.getIndexTableSpaceString())
        else:
            indexName = blockTable + "_gist"
            self.mogrifyExecute(cursor, 'CREATE INDEX ' + indexName + ' ON ' + blockTable + ' USING GIST ( geometry(pa) )' + self.getIndexTableSpaceString())
            
        connection.commit()
        if self.cluster:
            self.mogrifyExecute(cursor, "CLUSTER " + blockTable + " USING " + indexName)
            connection.commit()
        
        # Close the connection
        connection.close()
        if self.vacuum:
            self.vacuumTable(blockTable)
    
    
    def vacuumTable(self, tableName):
        connection = self.connect()
        cursor = connection.cursor()
        old_isolation_level = connection.isolation_level
        connection.set_isolation_level(0)
        self.mogrifyExecute(cursor, "VACUUM FULL ANALYZE " + tableName)
        connection.commit()
        connection.set_isolation_level(old_isolation_level)
        connection.close()
    
    def getNumPointsFlat(self, flatTable):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute('select count(*) from ' + flatTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n
    
    def getNumPointsBlocks(self, blockTable):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute('select sum(pc_numpoints(pa)) from ' + blockTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n
        
    def addPCFormat(self, schemaFile, fileAbsPath):
        (_, _, _, _, _, _, _, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ) = utils.getLASParams(fileAbsPath, tool = self.las2txtTool)
        
        # Get connection to DB
        connection = self.connect()
        cursor = connection.cursor()
        
        updatedFormat = False
        schema = None
        
        pc_namespace = '{http://pointcloud.org/schemas/PC/}'
        
        while not updatedFormat:
        
            # Check whether there is already a format with current scale-offset values 
            cursor.execute("SELECT pcid,schema FROM pointcloud_formats WHERE scaleX = %s AND scaleY = %s AND scaleZ = %s AND offsetX = %s AND offsetY = %s AND offsetZ = %s", 
                            [scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ])
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
                    self.mogrifyExecute(cursor, "INSERT INTO pointcloud_formats (pcid, srid, schema, scalex, scaley, scalez, offsetx, offsety, offsetz) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                               [pcid, self.srid, schema, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ])
                    connection.commit()
                    updatedFormat = True
                except :
                    connection.rollback() 
        connection.close()
        
        # Get the used dimensions if still not acquired (they should be always the same)
        compression = root.find(pc_namespace+'metadata').find('Metadata').text
        dimensionsOffsets = {}
        dimensionsScales = {}
        dimensionsNames = []
        for dimension in root.findall(pc_namespace+'dimension'):
            dName = dimension.find(pc_namespace+'name').text
            dimensionsNames.append(dName)
            if dName in ('X','Y','Z'):
                dimensionsOffsets[dName] = dimension.find(pc_namespace+'offset').text
                dimensionsScales[dName] = dimension.find(pc_namespace+'scale').text
        return (dimensionsNames, pcid, compression, dimensionsOffsets, dimensionsScales)  
    
    def createPDALXML(self, inputFileAbsPath, connectionString, pcid, dimensionsNames, blockTable, srid, blockSize, compression, offsets, scales):
        """ Create a XML file to load the data, in the given file, into the DB """
        
#        print connectionString,blockTable,srid,pcid,blockSize,compression,dimensionsNames,offsets,scales,blockSize,inputFileAbsPath,srid
 
        xmlContent = """<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
    <Writer type="writers.pgpointcloud">
        <Option name="connection">""" + connectionString + """</Option>
        <Option name="table">""" + blockTable + """</Option>
        <Option name="column">pa</Option>
        <Option name="srid">""" + str(srid) + """</Option>
        <Option name="pcid">""" + str(pcid) + """</Option>
        <Option name="overwrite">false</Option>
        <Option name="capacity">""" + blockSize + """</Option>
        <Option name="compression">""" + compression + """</Option>
        <Option name="output_dims">""" + ",".join(dimensionsNames) + """</Option>
        <Option name="offset_x">""" + offsets['X'] + """</Option>
        <Option name="offset_y">""" + offsets['Y'] + """</Option>
        <Option name="offset_z">""" + offsets['Z'] + """</Option>
        <Option name="scale_x">""" + scales['X'] + """</Option>
        <Option name="scale_y">""" + scales['Y'] + """</Option>
        <Option name="scale_z">""" + scales['Z'] + """</Option>
        <Filter type="filters.chipper">
            <Option name="capacity">""" + blockSize + """</Option>
            <Reader type="readers.las">
                <Option name="filename">""" + inputFileAbsPath + """</Option>
                <Option name="spatialreference">EPSG:""" + str(srid) + """</Option>
            </Reader>
        </Filter>
    </Writer>
</Pipeline>
"""
        outputFileName = os.path.basename(inputFileAbsPath) + '.xml'
        outputFile = open(outputFileName, 'w')
        outputFile.write(xmlContent)
        outputFile.close()
        return outputFileName


