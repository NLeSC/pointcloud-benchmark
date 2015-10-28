pointcloud-benchmark
====================

This project contains the *pointcloud* Python package (in python/pointcloud) 
which is used to deal with point clouds 
in various Point Cloud Data Management Systems (PCDMS's). Concretely the 
different PCDMS's are:
 - Combination of LAStools tools (lassort, lasindex, lasmerge and lasclip)
 - MonetDB: Flat table model
 - PostgreSQL: Blocks model (PostgreSQL point cloud extension by P.Ramsey) and 
 also a flat table model
 - Oracle: Blocks model and also a flat table model

Alternative storage models based on Morton space filling curves are also offered
as alternatives in some cases.

The main goal of the *pointcloud* Python package is to be used for the execution of benchmarks 
for the several PCDMS's. However it can be used in general:
 - To load/prepare the point cloud data through the loader tool
 (for example: import point cloud data from LAS files to PostgreSQL)
 - To query/retrieve portions of the loaded data through the querier tool
 (for example: get the points overlapping a rectangular region) 
 
 
Installation
------------

In the `install` folder you can find details on the requirements of the
*pointcloud* Python package as well as installation instructions for the several PCDMS's. 


Architecture
------------

The architecture of the *pointcloud* Python package is based on implementing, for each 
PCDMS, at least a Loader class (that must inherit from AbstractLoader and 
implement the missing methods) and a Querier class (that must inherit from 
AbstractQuerier and implement the missing methods)

The Loader classes are very generic so they can be used for the benchmark executions
but also for other applications to load point clouds in the PCDMS. 
The Querier classes are tailored to execute the queries specified in 
the benchmarks (see `queries` folder) but some of them implement parallel methods that may be interesting in other cases. 


Content
-------
- The `install` folder contains the installation instructions.
- The `python/pointcloud` folder contains the *pointcloud* Python package, i.e. 
the Loader and Querier classes for
the different PCDMS as well as the loader tool (`run/load_pc.py`) and the querier tool
(`run/query_pc.py`). 
- The `ini` folder contains the initialization files that are required to use
the loader and querier tools.
- `lasnlesc` contains the C binary loaders for PostgreSQL and MonetDB 
- `sfcnlesc` contains C and C++ version of the Morton-based queries used in the 
alternative storage structures.
- The `queries` folder contains XML files where the benchmark queries are defined
- the `doc` folder contains a usage guide for the various PCDMS. It contains how to use them independently of the *pointcloud* Python package and also with it.


Execution
---------

In order to use the loader tool of the *pointcloud* Python package execute the following command:

`python/pointcloud/run/load_pc.py -i [init file]` 


In order to use the querier tool of the *pointcloud* Python package execute the following command:

`python/pointcloud/run/query_pc.py -i [init file]`

where `[init file]` is the initialization file for the related PCDMS that you are 
using (use the files in `ini` folder as templates)

Adding a new PCDMS
------------------

To add a new PCDMS into the *pointcloud* Python package it is required:
 - To add a Loader class. The new class must inherit from AbstractLoader and implement the methods 
 initialize, process, close, size, getNumPoints.
 - To add a Querier class. The new class must inherit from AbstractQuerier and implement the methods
 initialize, query, close.
 - To add a ini file which must contain at least the following sections and options:
 
 ```
 [General]
Loader: pointcloud.newpcdms.Loader
Querier: pointcloud.newpcdms.Querier
ExecutionPath: newpcdms
LogLevel: DEBUG
UsageMonitor: True
IOMonitor:

[Load]
Folder: 

[Query]
File: 
NumberUsers: 1
NumberIterations: 2
```
 
