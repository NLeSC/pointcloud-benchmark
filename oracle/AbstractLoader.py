#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
import cx_Oracle
from pointcloud.AbstractLoader import AbstractLoader as ALoader
from pointcloud.oracle.CommonOracle import CommonOracle
from pointcloud import utils

class AbstractLoader(ALoader, CommonOracle):
    """ Abstract class for the Oracle loaders, some methods are already implemented"""
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        ALoader.__init__(self, configuration)
        self.setVariables(configuration)
        
    def connect(self, superUser = False):    
        return cx_Oracle.connect(self.connectString(superUser))
    
    def createUser(self):
        if self.cUser:
            connectionSuper = self.connect(True)
            cursorSuper = connectionSuper.cursor()
            
            cursorSuper.execute('select * from all_users where username = :1',[self.userName,])
            if len(cursorSuper.fetchall()):
                cursorSuper.execute('drop user ' + self.userName + ' CASCADE')
                connectionSuper.commit()
            
            cursorSuper.execute('create user ' + self.userName + ' identified by ' + self.password + ' default tablespace ' + self.tableSpace + ' temporary tablespace ' + self.tempTableSpace)
            cursorSuper.execute('grant unlimited tablespace, connect, resource, create view to ' + self.userName)
    
    def createLASDirectory(self, parentFolder):
        connectionSuper = self.connect(True)
        cursorSuper = connectionSuper.cursor()
        #print self.lasDirVariableName 
        cursorSuper.execute('select directory_name from all_directories where directory_name = :name', [self.lasDirVariableName,])
        if len(cursorSuper.fetchall()):
            cursorSuper.execute("drop directory " + self.lasDirVariableName)
            connectionSuper.commit()
        
        cursorSuper.execute("create directory " + self.lasDirVariableName + " as '" + parentFolder + "'")
        
        cursorSuper.execute("grant read on directory " + self.lasDirVariableName + " to " + self.userName)

        connectionSuper.commit()
        connectionSuper.close()

    def createExeLogDirectory(self):
        connectionSuper = self.connect(True)
        cursorSuper = connectionSuper.cursor()
        
#        for dname in (self.exeDirVariableName, self.logDirVariableName): 
        for dname in (self.exeDirVariableName,):
            cursorSuper.execute('select directory_name from all_directories where directory_name = :name', [dname,])
            if len(cursorSuper.fetchall()):
                cursorSuper.execute("drop directory " + dname)
                connectionSuper.commit()
            fold = os.path.abspath(os.path.curdir) + '/' + dname
            os.system('mkdir -p ' + fold)
            os.system('chmod -R 777 ' + fold) 
            cursorSuper.execute("create directory " + dname + " as '" +  fold + "'")
            
            cursorSuper.execute("grant read,write,execute on directory " + dname + " to public")
#            cursorSuper.execute("grant read,write,execute on directory " + dname + " to " + self.userName)
    
            connectionSuper.commit()
        connectionSuper.close()
        
    def createPreProc(self):
        cols = []
        for c in self.columns:
            if c not in self.colsData:
                raise Exception('Wrong column!' + c)
            cols.append(self.colsData[c][0].upper())
        fold = os.path.abspath(os.path.curdir) + '/' + self.exeDirVariableName
        of = open(fold + '/hilbert_prep.sh' , 'w')
        of.write("""#!/bin/sh
""" +  self.javaBinPath +  """ -classpath ${ORACLE_HOME}/md/jlib/sdoutl.jar oracle.spatial.util.Las2SqlLdr ${1} """ + ' '.join(cols) + """
exit
""")
        of.close()
        
    def giveReadPermission(self, absPath):
        connectionSuper = self.connect(True)
        cursorSuper = connectionSuper.cursor()
        
        cursorSuper.callproc('dbms_java.grant_permission', ('MDSYS', 'SYS:java.io.FilePermission',absPath, 'read'))
        connectionSuper.commit()

        cursorSuper.callproc('dbms_java.grant_permission', (self.userName, 'SYS:java.io.FilePermission',absPath, 'read')) 
        connectionSuper.commit()

        connectionSuper.close()
    
    def getParallelString(self):
        parallelString = ''
        if self.numProcessesLoad > 1:
            parallelString = ' parallel ' + str(self.numProcessesLoad) + ' '
        return parallelString 
    
    def getCompressString(self):
        if self.compression != '' and (self.compression.lower().count('none') == 0):
            return ' compress ' + self.compression
        else:
            return ' nocompress '
        
    def getColumnNames(self, columns, check=True, extraDict = None):
        cols = []
        for c in columns:
            if check and c not in self.colsData:
                raise Exception('Wrong column!' + c)
            if extraDict != None and c in extraDict:
                cols.append(extraDict[c])
            else:
                cols.append(self.colsData[c][0])
        return cols
    
    def createFlat(self, cursor, tableName):
        """ Creates a empty flat table"""
        self.dropTable(cursor, tableName, True)
        
        cols = []
        for c in self.columns:
            if c not in self.colsData:
                raise Exception('Wrong column!' + c)
            cols.append(self.colsData[c][0] + ' ' + self.colsData[c][1])

        self.mogrifyExecute(cursor,"""
CREATE TABLE """ + tableName + """ (""" + (',\n'.join(cols)) + """) TABLESPACE """ + self.tableSpace + """ pctfree 0 nologging""")
        cursor.connection.commit()
         
    def loadInc(self, fileAbsPath, objId, blockTable, blockSeq):
        javaPart = self.javaBinPath + ' -classpath ${ORACLE_HOME}/jdbc/lib/ojdbc6.jar:${ORACLE_HOME}/md/jlib/sdoutl.jar oracle.spatial.util.Las2SqlLdrIndep'
        (userPass, hostPortName) = self.connectString().split("@//")
        (userName,userPass) = userPass.split('/')
        
        numBlocks = self.batchSize / int(self.blockSize)
        
        if fileAbsPath.lower().endswith('laz'):
            tempFile = '/tmp/' + os.path.basename(fileAbsPath).lower().replace('laz','las')
            os.system('rm -f ' + tempFile)
            cz = 'laszip -i ' + fileAbsPath + ' -o ' + tempFile
            logging.info(cz)
            os.system(cz)
            
            command = javaPart + ' ' + str(objId) + ' ' + blockTable + ' ' + blockSeq + ' ' + tempFile + ' ' + str(self.blockSize) + ' jdbc:oracle:thin:@//' + hostPortName + ' ' + userName + ' ' + userPass + ' ' + str(self.batchSize) + ' ' + str(numBlocks)
            logging.info(command)
            os.system(command)
            os.system('rm ' + tempFile)
        else:
            command = javaPart + ' ' + str(objId) + ' ' + blockTable + ' ' + blockSeq + ' ' + fileAbsPath + ' ' + str(self.blockSize) + ' jdbc:oracle:thin:@//' + hostPortName + ' ' + userName + ' ' + userPass + ' ' + str(self.batchSize) + ' ' + str(numBlocks)
            logging.info(command)
            os.system(command)
         
    def loadToFlat(self, fileAbsPath, tableName):
        conf = self.getConfiguration()
        
        commonFile = os.path.basename(fileAbsPath).replace(conf.get('Load','Extension'),'')
        controlFile = commonFile + 'ctl'
        badFile = commonFile + 'bad'
        logFile = commonFile + 'log'
        
        ctfile = open(controlFile,'w')
        
        cols = []
        for c in self.columns:
            if c not in self.colsData:
                raise Exception('Wrong column!' + c)
            cols.append(self.colsData[c][0] + ' ' + self.colsData[c][2] + ' external(' + str(self.colsData[c][3]) + ')')
        
        ctfile.write("""load data
append into table """ + tableName + """
fields terminated by ','
(
""" + (',\n'.join(cols)) + """
)""")
        ctfile.close()
        las2txtCommand = utils.las2txtCommand(fileAbsPath, "stdout", columns = self.columns, separation = ',', tool = self.las2txtTool)
        sqlLoaderCommand = "sqlldr " + self.connectString() + " direct=true control=" + controlFile + " data=\\'-\\' bad=" + badFile + " log=" + logFile
        command = las2txtCommand + " | " + sqlLoaderCommand
        logging.debug(command)
        os.system(command)
    
    def loadToBlocks(self, cursor, fileAbsPath):
        cursor.execute('select TREAT(pc as SDO_PC).PC_ID from ' + self.baseTable)
        pcId = cursor.fetchone()[0]
        #cursor.callproc('sdo_pc_pkg.create_pc_incrementally', (pcId, self.blockTable, 'BLOCK_ID_SEQ', fileAbsPath, int(self.blockSize)))
        self.mogrifyExecute(cursor,"""
BEGIN
    sdo_pc_pkg.create_pc_incrementally(
        """ + str(pcId) + """,
        '""" + self.blockTable + """',
        'BLOCK_ID_SEQ',
        '""" + fileAbsPath + """',
        """ + str(self.blockSize) + """);
    commit;
END;""")
        cursor.connection.commit()
    
    def createExternal(self, cursor, lasFiles, tableName, columns):
        self.dropTable(cursor, tableName,True)
        #self.createExeLogDirectory()
        #self.createPreProc()        
        cols = []
#        for c in ['x','y','z','i','r','n','d','e','c','a','1','u','h','3','4','5','6','p']:
#        for c in ['x','y','z','h','3',]:
        for c in columns:
            if c not in self.colsData:
                raise Exception('Wrong column!' + c)
            cols.append(self.colsData[c][0] + ' ' + self.colsData[c][1])
        
        self.mogrifyExecute(cursor, """
CREATE TABLE """ + tableName + """ (""" + (',\n'.join(cols)) + """)
organization external
(
type oracle_loader
default directory """ + self.lasDirVariableName + """
access parameters (
    records delimited by newline
    preprocessor """ + self.exeDirVariableName + """:'hilbert_prep.sh'
    badfile """ + self.logDirVariableName + """:'hilbert_prep_%p.bad'
    logfile """ + self.logDirVariableName + """:'hilbert_prep_%p.log'
    fields terminated by ',')
location ('""" + lasFiles + """')
)
""" + self.getParallelString() + """ reject limit 0""")
        cursor.connection.commit()
        
    def createBlocks(self, cursor, blockTable, baseTable = None, tableSpace = None):    
        """ Create the blocks table and meta-data table"""
        if tableSpace == None:
            tableSpace = self.tableSpace
        
        self.dropTable(cursor, blockTable, True)
        if baseTable != None:
            self.dropTable(cursor, baseTable, True)
        
        # Tables to contain point data and point cloud metadata
        self.mogrifyExecute(cursor,"""
CREATE TABLE """ + blockTable + """
  TABLESPACE """ + tableSpace + """ pctfree 0 nologging 
  lob(points) store as securefile (tablespace """ + tableSpace + self.getCompressString() + """ cache reads nologging)
as SELECT * FROM mdsys.SDO_PC_BLK_TABLE where 0 = 1""")
        
        if baseTable != None:
            self.mogrifyExecute(cursor,"""
CREATE TABLE """ + baseTable + """ (pc sdo_pc)
  TABLESPACE """ + tableSpace + """ pctfree 0 nologging""")
            cursor.connection.commit()
        
    def createIndex(self, cursor, tableName, columns, partitioned = False, check = False):
        part = ''
        if partitioned:
            part = ' local '

        self.mogrifyExecute(cursor,"""
CREATE INDEX """ + tableName + """_IDX on """ + tableName + """ (""" + (','.join(self.getColumnNames(columns, check))) + """) 
  """ + part + """ tablespace """ + self.indexTableSpace + """ pctfree 0 nologging """ + self.getParallelString())
        cursor.connection.commit()
        
    def createBlockIndex(self, cursor):
        self.mogrifyExecute(cursor,"""insert into USER_SDO_GEOM_METADATA values ('""" + self.blockTable + """','BLK_EXTENT',
sdo_dim_array(sdo_dim_element('X',""" + self.minX + """,""" + self.maxX + """,""" + self.tolerance + """),
sdo_dim_element('Y',""" + self.minY + """,""" + self.maxY + """,""" + self.tolerance + """)),""" + self.srid + """)""")

        self.mogrifyExecute(cursor,"""create index """ + self.blockTable + """_SIDX on """ + self.blockTable + """ (blk_extent) indextype is mdsys.spatial_index
parameters ('tablespace=""" + self.indexTableSpace + """ work_tablespace=""" + self.workTableSpace + """ layer_gtype=polygon sdo_indx_dims=2 sdo_rtr_pctfree=0')""" + self.getParallelString())
        
        cursor.connection.commit()
    
    def createBlockIdIndex(self, cursor):
        self.mogrifyExecute(cursor,"""ALTER TABLE """ + self.blockTable + """ add constraint """ + self.blockTable + """_PK primary key (obj_id, blk_id) using index tablespace """ + self.indexTableSpace)
        cursor.connection.commit()
    
    def createIOT(self, cursor, newTableName, tableName, icolumns, ocolumns, keycolumns, distinct = False, check = False, hilbertFactor = None):
        """ Create Index-Organized-Table and populate it from tableName Table"""
        d = ""
        if distinct:
            d = "DISTINCT " 
        
        extraDict = None
        if hilbertFactor != None:
            extraDict = {'h':'d+(rownum*' + hilbertFactor + ') d'}
        
        icols = self.getColumnNames(icolumns,check, extraDict)
        ocols = self.getColumnNames(ocolumns,check)
        kcols = self.getColumnNames(keycolumns,check)
               
        self.mogrifyExecute(cursor, """
CREATE TABLE """ + newTableName + """
(""" + (','.join(ocols)) + """
    , constraint """ + newTableName + """_PK primary key (""" + (','.join(kcols)) + """))
    organization index
    tablespace """ + self.tableSpace + """ pctfree 0 nologging
    """ + self.getParallelString() + """
as
    SELECT """ + d + """ """ + (','.join(icols)) + """ FROM """ + tableName)
        cursor.connection.commit()


    def createTableAsSelect(self, cursor, newTableName, tableName, columns, check = False):
        self.mogrifyExecute(cursor, """
CREATE TABLE """ + newTableName + """
tablespace """ + self.tableSpace + """ pctfree 0 nologging
""" + self.getParallelString() + """
as
select """ + (','.join(self.getColumnNames(columns,check))) + """ from """ + tableName)
        cursor.connection.commit()

#     def createPopulateFlatExtHash(self, cursor, numPartitions):
#         """ Create partitioned source table, hash partitioning """   
#         self.mogrifyExecute(cursor, """
# CREATE TABLE """ + self.flatTable + """
# tablespace """ + self.tableSpace + """ pctfree 0 nologging
# partition by hash (d) partitions """ + str(numPartitions) + """
# """ + self.getParallelString() + """
# as
# select val_d1, val_d2, val_d3, d+(rownum*""" + self.hilbertFactor + """) d
# from """ + self.extTable)
#         cursor.connection.commit()
#         
#     def createPopulateFlatExtRange(self, cursor, ranges):
#         ps = []
#         for i in range(len(ranges)):
#             ps.append("partition p" + ('%03d' % i) + " values less than (" + ranges[i] + ")") 
#         
#         self.mogrifyExecute(cursor, """
# CREATE TABLE """ + self.flatTable + """
# tablespace """ + self.tableSpace + """ pctfree 0 nologging
# partition by range (d) (
# """ + (',\n'.join(ps)) + """
# )
# """ + self.getParallelString() + """
# as
# select val_d1, val_d2, val_d3, d+(rownum*""" + self.hilbertFactor + """) d
# from """ + self.extTable)
#         cursor.connection.commit()
    
    def initCreatePC(self, cursor, create = True):
        c=''
        if create:
            c = "sdo_pc_pkg.create_pc (ptcld, '" + viewFlatTable + "', NULL);"
        
        self.mogrifyExecute(cursor,"""
DECLARE
    ptn_params varchar2(80) := 'blk_capacity=""" + self.blockSize + """, work_tablespace=""" + self.workTableSpace + """';
    extent     sdo_geometry := sdo_geometry(2003,""" + self.srid + """,NULL,sdo_elem_info_array(1,1003,3),sdo_ordinate_array(""" + str(self.minX) + """,""" + str(self.minY) + """,""" + str(self.maxX) + """,""" + str(self.maxY) + """));
    ptcld      sdo_pc;
BEGIN
    ptcld := sdo_pc_pkg.init ('""" + self.baseTable + """', 'PC', '""" + self.blockTable + """', ptn_params, extent, """ + self.tolerance + """, 3, NULL, NULL, NULL);
    insert into """ + self.baseTable + """ values (ptcld);
    commit;
    """ + c + """
END;
""")
        
    def populateBlocks(self, cursor):
        viewFlatTable = 'VIEW_' + self.flatTable
        
        # Drop previous Base and Block tables if existing
        cursor.execute('SELECT view_name FROM all_views WHERE view_name = :1',[viewFlatTable,])
        if len(cursor.fetchall()):
            self.mogrifyExecute(cursor,'DROP VIEW ' + viewFlatTable)
            cursor.connection.commit()
    
        # Create a view that contains of the flat table to include the rid column required by the blocks
        self.mogrifyExecute(cursor,"CREATE VIEW " + viewFlatTable + " as SELECT '0' rid, " + (','.join(self.getColumnNames('xyz'))) + " from " + self.flatTable)

        #Initialize point cloud metadata and create point cloud   
        self.initCreatePC(cursor, create = True) 
        cursor.connection.commit()
        
        #self.mogrifyExecute(cursor,"""ALTER TABLE """ + self.blockTable + """ add constraint """ + self.blockTable + """_PK primary key (obj_id, blk_id) using index tablespace """ + self.indexTableSpace)
        self.mogrifyExecute(cursor,"""DROP VIEW """ + viewFlatTable)
        self.dropTable(cursor, self.flatTable)
        cursor.connection.commit()
        
    def populateBlocksHilbert(self, cursor):
        self.mogrifyExecute(cursor,"""
DECLARE
    ptcld      sdo_pc;
    ptn_params varchar2(80) := 'blk_capacity=""" + self.blockSize + """';
    extent     sdo_geometry := sdo_geometry(2003,""" + self.srid + """,NULL,sdo_elem_info_array(1,1003,3),sdo_ordinate_array(""" + str(self.minX) + """,""" + str(self.minY) + """,""" + str(self.maxX) + """,""" + str(self.maxY) + """));
    other_attrs XMLType     := xmltype('
                                <opc:sdoPcObjectMetadata
                                    xmlns:opc="http://xmlns.oracle.com/spatial/vis3d/2011/sdovis3d.xsd"
                                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                    blockingMethod="Hilbert R-tree">
                                </opc:sdoPcObjectMetadata>');
BEGIN
    ptcld := sdo_pc_pkg.init ('""" + self.baseTable + """', 'PC', '""" + self.blockTable + """', ptn_params, extent, """ + self.tolerance + """, 3, NULL, NULL, other_attrs);
    insert into """ + self.baseTable + """ values (ptcld);
    commit;
    sdo_pc_pkg.create_pc (ptcld, '""" + self.flatTable + """', NULL);
END;
""")
        self.mogrifyExecute(cursor, "update " + self.blockTable + " b set b.blk_extent.sdo_srid = " + str(self.srid))
        self.dropTable(cursor, self.flatTable)
        cursor.connection.commit()
        
    def computeStatistics(self, cursor, tableName):
        self.mogrifyExecute(cursor, "ANALYZE TABLE " + tableName + "  compute system statistics for table")
        self.mogrifyExecute(cursor,"""
BEGIN
    dbms_stats.gather_table_stats('""" + self.userName + """','""" + tableName + """',NULL,NULL,FALSE,'FOR ALL COLUMNS SIZE AUTO',8,'ALL');
END;""")
        cursor.connection.commit()
        
    def getSizeTable(self, cursor, tableName):
        try:
            if type(tableName) == str:
                tableName = [tableName, ]
            
            queryArgs = {}
            segs = []
            tabs = []
            for i in range(len(tableName)):
                name = 'name' + str(i)
                queryArgs[name] = tableName[i] + '%'
                segs.append('segment_name LIKE :' + name)
                tabs.append('table_name LIKE :' + name)
            
            cursor.execute("""
SELECT sum(bytes/1024/1024) size_in_MB FROM user_segments
  WHERE (""" + ' OR '.join(segs) + """
        OR segment_name in (
          SELECT segment_name FROM user_lobs
            WHERE """ + ' OR '.join(tabs) + """
          UNION
          SELECT index_name FROM user_lobs
            WHERE """ + ' OR '.join(tabs) + """
           )       
        )""", queryArgs)
            
            size = cursor.fetchall()[0][0]
            if size == None:
                size = 0
        except:
            size = 0
        return size
    
    def getSizeUserIndexes(self, cursor):
        try:
            cursor.execute("select sum(leaf_blocks * 0.0078125) from USER_INDEXES")
            size = cursor.fetchall()[0][0]
            if size == None:
                size = 0
        except:
            size = 0
        return size
    
    def getSizeUserSDOIndexes(self, cursor):
        try:
            cursor.callproc("dbms_output.enable")
            q = """
DECLARE
    size_in_mb  number;
    idx_tabname varchar2(32);
BEGIN
    dbms_output.enable;
    SELECT sdo_index_table into idx_tabname FROM USER_SDO_INDEX_INFO
    where table_name = :name and sdo_index_type = 'RTREE';
    execute immediate 'analyze table '||idx_tabname||' compute system statistics for table';
    select blocks * 0.0078125 into size_in_mb from USER_TABLES where table_name = idx_tabname;
    dbms_output.put_line (to_char(size_in_mb));
END;
        """
            cursor.execute(q, [self.blockTable,])
            statusVar = cursor.var(cx_Oracle.NUMBER)
            lineVar = cursor.var(cx_Oracle.STRING)
            size = float(cursor.callproc("dbms_output.get_line", (lineVar, statusVar))[0])
        except:
            size = 0
        return size
        
    def sizeFlat(self):
        connection = self.connect()
        cursor = connection.cursor()
        
        size_total = self.getSizeTable(cursor, self.flatTable)
        size_indexes = self.getSizeUserIndexes(cursor)
        
        connection.close()
    
        return self.formatSize(size_total, size_indexes)


    def sizeBlocks(self):
        connection = self.connect()
        cursor = connection.cursor()
       
        try: 
            size_total = self.getSizeTable(cursor, [self.blockTable, self.baseTable]) 
            size_indexes = self.getSizeUserSDOIndexes(cursor)
        except:
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
            
    def getNumPointsFlat(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute('select count(*) from ' + self.flatTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n
    
    def getNumPointsBlocks(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute('select sum(NUM_POINTS) from ' + self.blockTable)
        n = cursor.fetchone()[0]
        connection.close()
        return n  
    
