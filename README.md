# pointcloud-benchmark

This project contains the platform used for the execution of benchmark for several 
Point Cloud Data Management Systems (PCDMS) using Oracle, PostgreSQL, MonetDB and LASTools.

Concretely there are:

 - LASTOOLS
 - MONETDB
 - POSTGRESQL: Blocks (PostgreSQL point cloud extension by P.Ramsey) and also flat table model
 - ORACLE: Blocks and also flat table model
  
In install you can find instructions on the requirements and installation instructions
for the benchmark platform but also for the different used PCDMS. 

The benchmark platform which is done in Python has for the different PCDMS Loader classes that
can also be used in other applications to load point clouds in each PCDMS. The Loaders have 
many options that are configured though the initialization files in ini folder.

The are also Querier classes that contain method to do the queries of the benchmarks. 

In order to run the benchmark you need to execute one loader and one querier

Use run/load_pc.py -i [ini] and run/query_pc.py [ini]

where [ini] is the initialization file for the related PCDMS that you are using