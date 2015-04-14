Installation
====================

This software has several dependencies that need to be installed: 
geos, libtiff, proj, boost, geotiff, GDAL, laszip, libLAS, and the open-source part of LAStools.

Then, depending on which PCDMSs you want to use you may also need to install
several DB software:
 - MonetDB-based PCDMSs: requires MonetDB (http://dev.monetdb.org/hg/MonetDB)
 - PostgreSQL-based PCDMSs: PostgreSQL, PostGIS, pointcloud extension by P.Ramsey, PDAL
 - Oracle-based PCDMSs: Oracle 12c and PDAL
 - LAStools-based PCDMSs: PostgreSQL and PostGIS (used for improved performance) and closed-part of LAStools

Finally, since this is a Python benchmark platform some Python modules need to 
be installed in addition to the python bindings of previous dependencies 
(libLAS, GDAL): libxml2, libxslt, lxml, psutil, numpy, pycopg2, matplotlib

The installation of all the previous software is not trivial and can be tricky 
depending on the used OS. We have compiled guidelines for their installation
in several OS. See the various sub-folders in this folder.