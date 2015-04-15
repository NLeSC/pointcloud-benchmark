#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, optparse, psycopg2, multiprocessing, logging
from pointcloud import utils, postgresops, lasops

def runChild(childId, childrenQueue, connectionString, dbtable, lasinfotool, srid):
    kill_received = False
    connection = psycopg2.connect(connectionString)
    cursor = connection.cursor()
    while not kill_received:
        job = None
        try:
            # This call will patiently wait until new job is available
            job = childrenQueue.get()
        except:
            # if there is an error we will quit the loop
            kill_received = True
        if job == None:
            kill_received = True
        else:            
            [identifier, inputFile,] = job
            (srid, count, minX, minY, minZ, maxX, maxY, maxZ, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ) = lasops.getPCFileDetails(inputFile)
            
            insertStatement = """INSERT INTO """ + dbtable + """(id,filepath,num,scalex,scaley,scalez,offsetx,offsety,offsetz,geom) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_MakeEnvelope(%s, %s, %s, %s, %s));"""
            insertArgs = [identifier, inputFile, int(count), float(scaleX), float(scaleY), float(scaleZ), float(offsetX), float(offsetY), float(offsetZ), float(minX), float(minY), float(maxX), float(maxY), 28992]
            logging.info(cursor.mogrify(insertStatement, insertArgs))
            cursor.execute(insertStatement, insertArgs)
            connection.commit()
    cursor.close()
    connection.close()

def run(inputFolder, numcores, dbname, dbuser, dbpass, dbhost, dbport, createdb, dbtable, lasinfotool, srid):
    opts = 0
    childrenQueue = multiprocessing.Queue()
    ifiles = utils.getFiles(inputFolder)
    for i in range(len(ifiles)):
        childrenQueue.put([i, ifiles[i]])
    for i in range(int(numcores)): #we add as many None jobs as numWorkers to tell them to terminate (queue is FIFO)
        childrenQueue.put(None)
    
    clineConString = postgresops.getConnectString(dbname, dbuser, dbpass, dbhost, dbport, True)
    psycopgConString = postgresops.getConnectString(dbname, dbuser, dbpass, dbhost, dbport, False)
    
    if createdb:
        os.system('dropdb ' + clineConString)
        os.system('createdb ' + clineConString)

    connection = psycopg2.connect(psycopgConString)
    cursor = connection.cursor()
    if createdb:
        cursor.execute('CREATE EXTENSION postgis')
    connection.commit()
    q = """
    CREATE TABLE """ + dbtable + """ (
        id integer,
        filepath text,
        num integer,
        scalex double precision,
        scaley double precision,
        scalez double precision,
        offsetx double precision,     
        offsety double precision,
        offsetz double precision,
        geom public.geometry(Geometry,""" + srid + """)
    )"""
    logging.info(cursor.mogrify(q))
    cursor.execute(q)
    connection.commit()

#    q = "select addgeometrycolumn('" + dbtable + "','geom',28992,'POLYGON',2)"
#    logging.info(cursor.mogrify(q))
#    cursor.execute(q)
#    connection.commit()
    print 'numcores',numcores
    children = []
    # We start numcores children processes
    for i in range(int(numcores)):
        children.append(multiprocessing.Process(target=runChild, 
            args=(i, childrenQueue, psycopgConString, dbtable, lasinfotool)))
        children[-1].start()

    # wait for all children to finish their execution
    for i in range(int(numcores)):
        children[i].join()
         
    q = "create index ON " + dbtable + " using GIST (geom)"
    logging.info(cursor.mogrify(q))
    cursor.execute(q)

    connection.commit()
     
    old_isolation_level = connection.isolation_level
    connection.set_isolation_level(0)
    q = "VACUUM FULL ANALYZE " + dbtable
    logging.info(cursor.mogrify(q))
    cursor.execute(q)
    connection.commit()
    connection.set_isolation_level(old_isolation_level)
    cursor.close()        

def main(opts):
    run(opts.input, opts.cores, opts.dbname, opts.dbuser, opts.dbpass, opts.dbhost, opts.dbport, opts.create, opts.dbtable, opts.lasinfo, opts.srid)
        
if __name__ == "__main__":
    usage = 'Usage: %prog [options]'
    description = "Creates a table with geometries describing the areas of which the LAS files contain points."
    op = optparse.OptionParser(usage=usage, description=description)
    op.add_option('-i','--input',default='',help='Input folder where to find the LAS files',type='string')
    op.add_option('-x','--create',default=False,help='Creates the database',action='store_true')
    op.add_option('-n','--dbname',default='',help='Postgres DB name where to store the geometries',type='string')
    op.add_option('-u','--dbuser',default='',help='DB user',type='string')
    op.add_option('-p','--dbpass',default='',help='DB pass',type='string')
    op.add_option('-m','--dbhost',default='',help='DB host',type='string')
    op.add_option('-r','--dbport',default='',help='DB port',type='string')
    op.add_option('-t','--dbtable',default='',help='DB table',type='string')
    op.add_option('-c','--cores',default='',help='Number of used processes',type='string')
    op.add_option('-l','--lasinfo',default='liblas',help='Library used for lasinfo: lastools or liblas',type='string')
    op.add_option('-s','--srid',default='',help='SRID',type='string')
    (opts, args) = op.parse_args()
    main(opts)
