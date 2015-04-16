NLeSC LAS to DB tools
=====================

lasnlesc converts LAS files, or its compressed format LAZ, into the binary
formats used by MonetDB and PostgreSQL.

For MonetDB use las2col, while for Postgres use las2pg
See INSTALL to find out how to install these apps

The loaders (las2col and las2pg) generate dump files as used by those two DBs 
that can be then imported into the DBs producing a table with one point per row.


MonetDB example
---------------
Use binary loader to create the columnar files:


`las2col -f listFile /pak1/mondata/tmp/0_tempFile --parse xyz`


Where listFile is the list of LAS/LAZ files. Note that las2col is using multiple cores automatically 
(the number of cores is 13 by default, if you want to change that modify NUM_READ_THREADS in las2col.c before compiling it)

Use mclient or your favorite language API to import data in DB:


`COPY BINARY INTO ahn_flat from ('/pak1/mondata/tmp/0_tempFile_col_x.dat','/pak1/mondata/tmp/0_tempFile_col_y.dat','/pak1/mondata/tmp/0_tempFile_col_z.dat')`


Note that the table receiving the data must have been previously created.
The columnar files need to be in the same file system as the MonetDB instance files.


PostgreSQL example
------------------
In this case we can use a pipe to directly import the LAS file into a PostgreSQL table:


`las2pg -s tile_85800_447300.las --stdout --parse xyz | psql  pf20Mxyz -c "copy ahn_flat from stdin with binary"`


Note that the table receiving the data must have been previously created.

las2pg uses one single core but you can have multiple instances of it importing data simultaneously.


Morton codes
------------
Both loaders can generate morton codes (codification of XY -> position of the point in the Morton SFC)
The code to decode the generated morton codes can be found in src/decodemorton.c
This can be used in cases where we only store the Morton code (and not the XY) and we use use the code to decode XY when necessary
The Morton code can be used, in addition to order points, to do queries. In that case use the code in python/pointcloud/QuadTree.py 
or the one in sfcnlesc (even though this one does not have the distinction between ranges fully in the query region)
 