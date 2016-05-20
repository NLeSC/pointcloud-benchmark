#Installation:

First set the paths in [config/lasNLeSC_config.sh](https://github.com/NLeSC/pointcloud-benchmark/blob/master/lasnlesc/config/lasNLeSC_config.sh)
```
for:
LASNLESC_HOME=
LIBLAS_HOME=
LASZIP_HOME=
```

Then do:
```
. config/lasNLeSC_config.sh

mkdir makefiles

cd makefiles

cmake -DCMAKE_FIND_ROOT_PATH=$LIBLAS_HOME -DCMAKE_INSTALL_PREFIX=$LASNLESC_HOME -DCMAKE_BUILD_TYPE=Release -G "Unix Makefiles" ../

make

make install
```
