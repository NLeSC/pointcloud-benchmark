#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging, traceback
from pointcloud.AbstractLoader import AbstractLoader as ALoader
from pointcloud.oracle.CommonOracle import CommonOracle
from pointcloud import oracleops

class AbstractLoader(ALoader, CommonOracle):
    """ Abstract class for the Oracle loaders, some methods are already implemented"""
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        ALoader.__init__(self, configuration)
        self.setVariables(configuration)
        
    def createUser(self):
        logging.info('Creating user ' + self.userName)
        connectionSuper = self.getConnection(True)
        cursorSuper = connectionSuper.cursor()
        oracleops.createUser(cursorSuper, self.userName, self.password, self.tableSpace, self.tempTableSpace)
        connectionSuper.close()
    
    def createLASDirectory(self, lasDirVariableName, parentFolder):
        connectionSuper = self.getConnection(True)
        cursorSuper = connectionSuper.cursor()
        oracleops.createDirectory(cursorSuper, lasDirVariableName, parentFolder, self.userName)
        connectionSuper.close()
    
    def getParallelString(self, numProcesses):
        parallelString = ''
        if numProcesses > 1:
            parallelString = ' parallel ' + str(self.numProcessesLoad) + ' '
        return parallelString 
    
    def getCompressString(self, compression):
        if compression != '' and (compression.lower().count('none') == 0):
            return ' compress ' + compression
        else:
            return ' nocompress '
    
    def getDBColumns(self, columns, includeType = False, hilbertColumnName = 'd'):
        cols = []
        for i in range(len(columns)):
            column = columns[i]
            if column not in self.colsData:
                raise Exception('Wrong column!' + column)
            if column == 'h':
                if i != (len(columns)-1):
                    raise Exception('Hilbert code has to be the last column!')
                columnName = hilbertColumnName
            else:
                columnName = 'VAL_D' + str(i+1)
            c = columnName
            if includeType:
                c += ' ' + self.colsData[column][0]
            cols.append(c)
        return cols
    
    def createFlatTable(self, cursor, tableName, columns):
        """ Creates a empty flat table"""
        oracleops.dropTable(cursor, tableName, True)

        oracleops.mogrifyExecute(cursor,"""
CREATE TABLE """ + tableName + """ (""" + (',\n'.join(self.getDBColumns(columns, True))) + """) TABLESPACE """ + self.tableSpace + """ pctfree 0 nologging""")

    def createBlocksTable(self, cursor, blockTable, tableSpace, compression, baseTable = None, includeBlockId = False):    
        """ Create the blocks table and meta-data table"""
        oracleops.dropTable(cursor, blockTable, True)
        if baseTable != None:
            oracleops.dropTable(cursor, baseTable, True)
        
        # Tables to contain point data and point cloud metadata
        oracleops.mogrifyExecute(cursor,"""
CREATE TABLE """ + blockTable + """
  TABLESPACE """ + tableSpace + """ pctfree 0 nologging 
  lob(points) store as securefile (tablespace """ + tableSpace + self.getCompressString(compression) + """ cache reads nologging)
as SELECT * FROM mdsys.SDO_PC_BLK_TABLE where 0 = 1""")
        
        if baseTable != None:
            if includeBlockId:
                oracleops.mogrifyExecute(cursor,"""
    CREATE TABLE """ + baseTable + """ (id number, pc sdo_pc)
      TABLESPACE """ + tableSpace + """ pctfree 0 nologging""")
            else:
                oracleops.mogrifyExecute(cursor,"""
    CREATE TABLE """ + baseTable + """ (pc sdo_pc)
      TABLESPACE """ + tableSpace + """ pctfree 0 nologging""")  
         
    def las2txt_sqlldr(self, fileAbsPath, tableName, columns):
        commonFile = os.path.basename(fileAbsPath).replace(fileAbsPath.split('.')[-1],'')
        controlFile = commonFile + 'ctl'
        badFile = commonFile + 'bad'
        logFile = commonFile + 'log'
        
        ctfile = open(controlFile,'w')
        cols = []
        for i in range(len(columns)):
            column = columns[i]
            if column not in self.colsData:
                raise Exception('Wrong column! ' + column)
            if column in 'mhlnvf':
                raise Exception('Column ' + column + ' not compatible with las2txt+sqlldr')
            columnName = 'VAL_D' + str(i+1)
            cols.append(columnName + ' ' + self.colsData[column][1] + ' external(' + str(self.colsData[column][2]) + ')')
        
        ctfile.write("""load data
append into table """ + tableName + """
fields terminated by ','
(
""" + (',\n'.join(cols)) + """
)""")
        ctfile.close()
        las2txtCommand = 'las2txt -i ' + fileAbsPath + ' -stdout -parse ' + self.columns + ' -sep comma'
        sqlLoaderCommand = "sqlldr " + self.getConnectionString() + " direct=true control=" + controlFile + " data=\\'-\\' bad=" + badFile + " log=" + logFile
        command = las2txtCommand + " | " + sqlLoaderCommand
        logging.debug(command)
        os.system(command)
        
    def populateBlocks(self, cursor, srid, minX, minY, maxX, maxY, flatTable, blockTable, baseTable, blockSize, columns, tolerance, workTableSpace):
        """Populate blocks from points in a flat table and delete flat table afterwards"""
        viewFlatTable = 'VIEW_' + flatTable
        
        # Drop previous Base and Block tables if existing
        cursor.execute('SELECT view_name FROM all_views WHERE view_name = :1',[viewFlatTable,])
        if len(cursor.fetchall()):
            oracleops.mogrifyExecute(cursor,'DROP VIEW ' + viewFlatTable)
        
        # Create a view that contains of the flat table to include the rid column required by the blocks
        oracleops.mogrifyExecute(cursor,"CREATE VIEW " + viewFlatTable + " as SELECT '0' rid, " + (','.join(self.getDBColumns(columns, False))) + " from " + flatTable)

        #Initialize point cloud metadata and create point cloud   
        self.initCreatePC(cursor, srid, minX, minY, maxX, maxY, viewFlatTable, blockTable, baseTable, blockSize, tolerance, workTableSpace) 
        
        #oracleops.mogrifyExecute(cursor,"""ALTER TABLE """ + self.blockTable + """ add constraint """ + self.blockTable + """_PK primary key (obj_id, blk_id) using index tablespace """ + self.indexTableSpace)
        oracleops.mogrifyExecute(cursor,"""DROP VIEW """ + viewFlatTable)
        oracleops.dropTable(cursor, flatTable)    

    def initCreatePC(self, cursor, srid, minX, minY, maxX, maxY, flatTable, blockTable, baseTable, blockSize, tolerance, workTableSpace, create = True):
        c=''
        if create:
            c = "sdo_pc_pkg.create_pc (ptcld, '" + flatTable + "', NULL);"
        
        oracleops.mogrifyExecute(cursor,"""
DECLARE
    ptn_params varchar2(80) := 'blk_capacity=""" + str(blockSize) + """, work_tablespace=""" + workTableSpace + """';
    extent     sdo_geometry := sdo_geometry(2003,""" + str(srid) + """,NULL,sdo_elem_info_array(1,1003,3),sdo_ordinate_array(""" + str(minX) + """,""" + str(minY) + """,""" + str(maxX) + """,""" + str(maxY) + """));
    ptcld      sdo_pc;
BEGIN
    ptcld := sdo_pc_pkg.init ('""" + baseTable + """', 'PC', '""" + blockTable + """', ptn_params, extent, """ + str(tolerance) + """, 3, NULL, NULL, NULL);
    insert into """ + baseTable + """ values (ptcld);
    commit;
    """ + c + """
END;
""")

    def createBlockIdIndex(self, cursor, blockTable, indexTableSpace):
        oracleops.mogrifyExecute(cursor,"""ALTER TABLE """ + blockTable + """ add constraint """ + blockTable + """_PK primary key (obj_id, blk_id) using index tablespace """ + indexTableSpace)      
        
    def createExternalTable(self, cursor, lasFiles, tableName, columns, lasDirVariableName, numProcesses):
        # Executes the external table setting it to use hilbert_prep.sh script (which must be located in the EXE_DIR Oracle directory)
        oracleops.dropTable(cursor, tableName, True)
        oracleops.mogrifyExecute(cursor, """
CREATE TABLE """ + tableName + """ (""" + (',\n'.join(self.getDBColumns(columns, True))) + """)
organization external
(
type oracle_loader
default directory """ + lasDirVariableName + """
access parameters (
    records delimited by newline
    preprocessor EXE_DIR:'hilbert_prep.sh'
    badfile LOG_DIR:'hilbert_prep_%p.bad'
    logfile LOG_DIR:'hilbert_prep_%p.log'
    fields terminated by ',')
location ('""" + lasFiles + """')
)
""" + self.getParallelString(numProcesses) + """ reject limit 0""")     
        
    def createIOTTable(self, cursor, iotTableName, tableName, tableSpace, icolumns, ocolumns, keycolumns, numProcesses, check = False, hilbertFactor = None):
        """ Create Index-Organized-Table and populate it from tableName Table"""
        oracleops.dropTable(cursor, iotTableName, True)
        hilbertColumnName = 'd'
        if hilbertFactor != None:
            hilbertColumnName = 'd+(rownum*' + hilbertFactor + ') d'
        
        icols = self.getDBColumns(icolumns,False, hilbertColumnName)
        ocols = self.getDBColumns(ocolumns,False)
        kcols = self.getDBColumns(keycolumns,False)
        
        oracleops.mogrifyExecute(cursor, """
CREATE TABLE """ + iotTableName + """
(""" + (','.join(ocols)) + """
    , constraint """ + iotTableName + """_PK primary key (""" + (','.join(kcols)) + """))
    organization index
    tablespace """ + tableSpace + """ pctfree 0 nologging
    """ + self.getParallelString(numProcesses) + """
as
    SELECT """ + (','.join(icols)) + """ FROM """ + tableName)
    
    def populateBlocksHilbert(self, cursor, srid, minX, minY, maxX, maxY, flatTable, blockTable, baseTable, blockSize, tolerance):
        # NOTE: In this case we do not require to create a view, since the fixed format by the hilbert pre-processor makes it directly compatible
        # if we change the pre-processor we need to change these ones
        self.initCreatePCHilbert(cursor, srid, minX, minY, maxX, maxY, flatTable, blockTable, baseTable, blockSize, tolerance)
        oracleops.dropTable(cursor, flatTable)
    
    def updateBlocksSRID(self, cursor, blockTable, srid):
        oracleops.mogrifyExecute(cursor, "update " + blockTable + " b set b.blk_extent.sdo_srid = " + str(srid))
        
    def initCreatePCHilbert(self, cursor, srid, minX, minY, maxX, maxY, flatTable, blockTable, baseTable, blockSize, tolerance):
        # this one also populates
        oracleops.mogrifyExecute(cursor,"""
DECLARE
    ptcld      sdo_pc;
    ptn_params varchar2(80) := 'blk_capacity=""" + str(blockSize) + """';
    extent     sdo_geometry := sdo_geometry(2003,""" + str(srid) + """,NULL,sdo_elem_info_array(1,1003,3),sdo_ordinate_array(""" + str(minX) + """,""" + str(minY) + """,""" + str(maxX) + """,""" + str(maxY) + """));
    other_attrs XMLType     := xmltype('
                                <opc:sdoPcObjectMetadata
                                    xmlns:opc="http://xmlns.oracle.com/spatial/vis3d/2011/sdovis3d.xsd"
                                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                    blockingMethod="Hilbert R-tree">
                                </opc:sdoPcObjectMetadata>');
BEGIN
    ptcld := sdo_pc_pkg.init ('""" + baseTable + """', 'PC', '""" + blockTable + """', ptn_params, extent, """ + str(tolerance) + """, 3, NULL, NULL, other_attrs);
    insert into """ + baseTable + """ values (ptcld);
    commit;
    sdo_pc_pkg.create_pc (ptcld, '""" + flatTable + """', NULL);
END;
""")
        self.updateBlocksSRID(cursor, blockTable, srid)
        
    def createBlockIndex(self, cursor, srid, minX, minY, maxX, maxY, blockTable, indexTableSpace, workTableSpace, numProcesses):
        oracleops.mogrifyExecute(cursor,"""insert into USER_SDO_GEOM_METADATA values ('""" + blockTable + """','BLK_EXTENT',
sdo_dim_array(sdo_dim_element('X',""" + str(minX) + """,""" + str(maxX) + """,""" + str(self.tolerance) + """),
              sdo_dim_element('Y',""" + str(minY) + """,""" + str(maxY) + """,""" + str(self.tolerance) + """)),""" + str(srid) + """)""")

        oracleops.mogrifyExecute(cursor,"""create index """ + blockTable + """_SIDX on """ + blockTable + """ (blk_extent) indextype is mdsys.spatial_index
parameters ('tablespace=""" + indexTableSpace + """ work_tablespace=""" + workTableSpace + """ layer_gtype=polygon sdo_indx_dims=2 sdo_rtr_pctfree=0')""" + self.getParallelString(numProcesses))
        
                
    def loadInc(self, fileAbsPath, objId, blockTable, blockSeq, blockSize, batchSize):
        javaPart = 'java -classpath ${ORACLE_HOME}/jdbc/lib/ojdbc6.jar:${ORACLE_HOME}/md/jlib/sdoutl.jar oracle.spatial.util.Las2SqlLdrIndep'
        (userPass, hostPortName) = self.getConnectionString().split("@//")
        (userName,userPass) = userPass.split('/')
        
        numBlocks = int(batchSize) / int(blockSize)
        
        tempFile = None
        inputFile = fileAbsPath
        if fileAbsPath.lower().endswith('laz'):
            tempFile = '/tmp/' + os.path.basename(fileAbsPath).lower().replace('laz','las')
            os.system('rm -f ' + tempFile)
            cz = 'laszip -i ' + fileAbsPath + ' -o ' + tempFile
            logging.info(cz)
            os.system(cz)
            inputFile = tempFile
        command = javaPart + ' ' + str(objId) + ' ' + blockTable + ' ' + blockSeq + ' ' + inputFile + ' ' + str(blockSize) + ' jdbc:oracle:thin:@//' + hostPortName + ' ' + userName + ' ' + userPass + ' ' + str(batchSize) + ' ' + str(numBlocks)
        logging.info(command)
        os.system(command)
        
    def createFlatMeta(self, cursor, tableName):
        #  Create the meta-data table
        oracleops.dropTable(cursor, tableName, True)
        oracleops.mogrifyExecute(cursor, "CREATE TABLE " + tableName + " (tablename text, srid integer, minx DOUBLE PRECISION, miny DOUBLE PRECISION, maxx DOUBLE PRECISION, maxy DOUBLE PRECISION, scalex DOUBLE PRECISION, scaley DOUBLE PRECISION)")
                
    def createIndex(self, cursor, tableName, columns, indexTableSpace, numProcesses):
        oracleops.mogrifyExecute(cursor,"""
CREATE INDEX """ + tableName + """_IDX on """ + tableName + """ (""" + (','.join(self.getDBColumns(columns, False))) + """) 
 tablespace """ + indexTableSpace + """ pctfree 0 nologging """ + self.getParallelString(numProcesses))

    def createTableAsSelect(self, cursor, newTableName, tableName, columns, tableSpace, numProcesses):
        oracleops.dropTable(cursor, newTableName, True)
        oracleops.mogrifyExecute(cursor, """
CREATE TABLE """ + newTableName + """
tablespace """ + tableSpace + """ pctfree 0 nologging
""" + self.getParallelString(numProcesses) + """
as
select """ + (','.join(self.getDBColumns(columns,False))) + """ from """ + tableName)

    def computeStatistics(self, cursor, tableName):
        oracleops.mogrifyExecute(cursor, "ANALYZE TABLE " + tableName + "  compute system statistics for table")
        oracleops.mogrifyExecute(cursor,"""
BEGIN
    dbms_stats.gather_table_stats('""" + self.userName + """','""" + tableName + """',NULL,NULL,FALSE,'FOR ALL COLUMNS SIZE AUTO',8,'ALL');
END;""")

    def sizeFlat(self, flatTable):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        size_total = oracleops.getSizeTable(cursor, flatTable)
        size_indexes = oracleops.getSizeUserIndexes(cursor)
        
        connection.close()
    
        return self.formatSize(size_total, size_indexes)


    def sizeBlocks(self, blockTable, baseTable):
        connection = self.getConnection()
        cursor = connection.cursor()
       
        try: 
            size_total = oracleops.getSizeTable(cursor, [blockTable, baseTable]) 
            size_indexes = oracleops.getSizeUserSDOIndexes(cursor, blockTable)
        except Exception, err:
            print traceback.format_exc()
            size_total = None
            size_indexes = None
        connection.close()
        return self.formatSize(size_total, size_indexes)
    
    def formatSize(self, size_total, size_indexes):
        try:
            size_ex_indexes = float(size_total) - float(size_indexes)
            return (" Size indexes= %.2f MB" % size_indexes) + (". Size excluding indexes= %.2f MB" % size_ex_indexes) + (". Size total= %.2f MB" % size_total)
        except:
            return ''
            
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
        cursor.execute('select sum(NUM_POINTS) from ' + blockTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n  
