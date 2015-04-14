pointcloud-benchmark
====================

This project contains the Python platform used for the execution of benchmarks 
for several Point Cloud Data Management Systems (PCDMS). Concretely the 
different PCDMS are:
 - LAStools
 - MonetDB (flat table model)
 - PostgreSQL: Blocks model (PostgreSQL point cloud extension by P.Ramsey) and 
 also a flat table model
 - Oracle: Blocks model and also a flat table model

Alternative storage models based on Morton space filling curves are also offered
as alternatives in some cases.

The software available in this project can be used:
 - To load/prepare the point cloud data through the loader tool
 (for example: import point cloud data from LAS files to PostgreSQL)
 - To query/retrieve portions of the loaded data through the querier tool
 (for example: get the points overlapping a rectangular region) 
 
 
Installation
------------

In the install folder you can find details on the requirements of the
Python benchmark platform as well as installation instructions. 


Architecture
------------

The architecture of the benchmark platform is based on implementing, for each 
PCDMS, at least a Loader class (that must inherit from AbstractLoader and 
implement the missing methods) and a Querier class (that must inherit from 
AbstractQuerier and implement the missing methods)

While the Querier classes are thought to execute only the queries specified in 
the benchmark definition the Loader classes can also be used in other 
applications to load point clouds in the PCDMS. 


Content
-------

- The `python/pointcloud` folder mainly contains the Loader and Querier classes for
the different PCDMS as well as the loader tool (`run/load_pc.py`) and the querier tool
(`run/query_pc.py`). 
- The `ini` folder contains the initialization files that are required to use
the loader and querier tools.
- `lasnlesc` contains the C binary loaders for PostgreSQL (alternative to PDAL) and MonetDB 
- `sfcnlesc` contains C and C++ version of the Morton-based queries used in the 
alternative storage structures.
- The `queries` folder contains XML files where the benchmark queries are defined


Execution
---------

In order to run the benchmark you need to execute the loader tool and the querier tool:

`python/pointcloud/run/load_pc.py -i [init file]` 
`python/pointcloud/run/query_pc.py -i [init file]`

where `[init file]` is the initialization file for the related PCDMS that you are 
using (use the files in `ini` folder as templates)