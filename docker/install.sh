yum update

yum install -y wget cmake make CUnit-devel autoconf automake gcc cpp gcc-c++ \
python-devel swig libjpeg-turbo-devel bzip2 bzip2-devel zlib-devel \
git libxml2-devel numpy flann flann-devel sqlite sqlite-devel openmpi-devel \
subversion expat-devel libcurl-devel xerces-c-devel unixODBC-devel json-c-devel \
pcre pcre-devel ant bison bison-devel openssl openssl-devel gettext-devel libtool \
hg readline readline-devel unixODBC unixODBC-devel unzip freetype freetype-devel \
libxslt libxslt-devel libpng-devel python-psycopg2 python-matplotlib

mkdir /src
cd /src

# GEOS
wget http://download.osgeo.org/geos/geos-3.4.2.tar.bz2
tar xvf geos-3.4.2.tar.bz2
cd geos-3.4.2
mkdir build
mkdir makefiles
cd makefiles
../configure --prefix=/opt/sw/geos-3.4.2/build --enable-python
make -j8
make install
export LD_LIBRARY_PATH="/opt/sw/geos-3.4.2/build/lib:$LD_LIBRARY_PATH"
export PATH="/opt/sw/geos-3.4.2/build/bin:$PATH"

cd /src

# PROJ.4
wget http://download.osgeo.org/proj/proj-4.8.0.tar.gz
tar xvf proj-4.8.0.tar.gz
cd proj-4.8.0
mkdir build
./configure --prefix=/opt/sw/proj-4.8.0/build
make -j8
make install
export LD_LIBRARY_PATH="/opt/sw/proj-4.8.0/build/lib:$LD_LIBRARY_PATH"
export PATH="/opt/sw/proj-4.8.0/build/bin:$PATH"

cd /src

# TIFF
wget http://download.osgeo.org/libtiff/tiff-4.0.3.tar.gz
tar xvzf tiff-4.0.3.tar.gz
cd tiff-4.0.3
mkdir makefiles
cd makefiles/
../configure --prefix=/opt/sw/tiff-4.0.3/build --exec-prefix=/opt/sw/tiff-4.0.3/build/
make -j8
make install
export LD_LIBRARY_PATH="/opt/sw/tiff-4.0.3/build/lib:$LD_LIBRARY_PATH"
export PATH="/opt/sw/tiff-4.0.3/build/bin:$PATH"

cd /src

# GEOTIFF
wget wget http://download.osgeo.org/geotiff/libgeotiff/libgeotiff-1.4.1.tar.gz
tar xvf libgeotiff-1.4.1.tar.gz
cd libgeotiff-1.4.1
mkdir build
./configure --prefix=/opt/sw/libgeotiff-1.4.1/build --with-proj=/opt/sw/proj-4.8.0/build --with-libtiff=/opt/sw/tiff-4.0.3/build --with-zlib --with-jpeg
make -j8
make install
export LD_LIBRARY_PATH="/opt/sw/libgeotiff-1.4.1/build/lib:$LD_LIBRARY_PATH"
export PATH="/opt/sw/libgeotiff-1.4.1/build/bin:$PATH"

cd /src

# BOOST
wget http://downloads.sourceforge.net/project/boost/boost/1.55.0/boost_1_55_0.tar.bz2
tar xvf boost_1_55_0.tar.bz2
cd boost_1_55_0
mkdir build
./bootstrap.sh --prefix=/opt/sw/boost_1_55_0/build
./b2 -j4 stage threading=multi --layout=tagged link=shared
./b2 install
export LD_LIBRARY_PATH="/opt/sw/boost_1_55_0/build/lib:$LD_LIBRARY_PATH"

cd /src

# GDAL
svn checkout https://svn.osgeo.org/gdal/trunk/gdal gdal-trunk
cd gdal-trunk/
mkdir build
export CC="gcc -fPIC"
export CXX="g++ -fPIC"
./configure --prefix=/opt/sw/gdal-trunk/build/ --with-jpeg=external --with-jpeg12 --without-libtool --without-python --with-static-proj4=/opt/sw/proj-4.8.0/build/ --with-libtiff=/opt/sw/tiff-4.0.3/build/ --with-geotiff=/opt/sw/libgeotiff-1.4.1/build/ --with-geos=/opt/sw/geos-3.4.2/build/bin/geos-config
make -j4
make install
export LD_LIBRARY_PATH="/opt/sw/gdal-trunk/build/lib:$LD_LIBRARY_PATH"
export PATH="/opt/sw/gdal-trunk/build/bin:$PATH"
export PATH="/opt/sw/gdal-trunk/swig/python/scripts:$PATH"

mkdir -p /opt/sw/gdal-trunk/build/lib64/python2.7/site-packages
export PYTHONPATH="$PYTHONPATH:/opt/sw/gdal-trunk/build/lib64/python2.7/site-packages"
cd swig
make -j4
cd python
python setup.py install --prefix=/opt/sw/gdal-trunk/build
export PYTHONPATH="$PYTHONPATH:/opt/sw/gdal-trunk/build/lib64/python2.7/site-packages"

cd /src

# LASZIP
wget http://download.osgeo.org/laszip/laszip-2.1.0.tar.gz
tar xvf laszip-2.1.0.tar.gz
cd laszip-2.1.0
mkdir build
mkdir makefiles
cd makefiles/
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/sw/laszip-2.1.0/build/
make
make install
export LD_LIBRARY_PATH="/opt/sw/laszip-2.1.0/build/lib:$LD_LIBRARY_PATH"
export PATH="/opt/sw/laszip-2.1.0/build/bin:$PATH"

cd /src

# LIBLAS
git clone git://github.com/libLAS/libLAS.git
cd libLAS/
git checkout tags/1.8.0
mkdir makefiles
cd makefiles
# The FindPROJ4.cmake is missing. It has to be put in /opt/sw/liblas/cmake/modules
# Use file in this directory
cmake -G "Unix Makefiles" .. -DCMAKE_INSTALL_PREFIX=/opt/sw/liblas/build -DWITH_GDAL=ON  -DWITH_GEOTIFF=ON -DWITH_LASZIP=ON -DWITH_PKGCONFIG=ON -DWITH_TESTS=ON -DGDAL_CONFIG=/opt/sw/gdal-trunk/build/bin/gdal-config -DGDAL_INCLUDE_DIR=/opt/sw/gdal-trunk/build/include -DGDAL_LIBRARY=/opt/sw/gdal-trunk/build/lib/libgdal.so -DGEOTIFF_INCLUDE_DIR=/opt/sw/libgeotiff-1.4.1/build/include -DGEOTIFF_LIBRARY=/opt/sw/libgeotiff-1.4.1/build/lib/libgeotiff.so -DLASZIP_INCLUDE_DIR=/opt/sw/laszip-2.1.0/build/include -DLASZIP_LIBRARY=/opt/sw/laszip-2.1.0/build/lib/liblaszip.so -DTIFF_INCLUDE_DIR=/opt/sw/tiff-4.0.3/build/include -DTIFF_LIBRARY=/opt/sw/tiff-4.0.3/build/lib/libtiff.so -DPROJ4_LIBRARY=/opt/sw/proj-4.8.0/build/lib/libproj.so -DPROJ4_INCLUDE_DIR=/opt/sw/proj-4.8.0/build/include -DBOOST_LIBRARY_DIRS=/opt/sw/boost_1_55_0/build/lib -DBOOST_INCLUDEDIR=/opt/sw/boost_1_55_0/build/include -DBOOST_PROGRAM_OPTIONS_LIBRARY=/opt/sw/boost_1_55_0/build/lib/libboost_program_options.so -DBOOST_PROGRAM_OPTIONS_LIBRARY_DEBUG=/opt/sw/boost_1_55_0/build/lib/libboost_program_options.so -DBOOST_PROGRAM_OPTIONS_LIBRARY_RELEASE=/opt/sw/boost_1_55_0/build/lib/libboost_program_options.so -DBOOST_THREAD_LIBRARY=/opt/sw/boost_1_55_0/build/lib/libboost_thread.so -DBOOST_THREAD_LIBRARY_DEBUG=/opt/sw/boost_1_55_0/build/lib/libboost_thread.so -DBOOST_THREAD_LIBRARY_RELEASE=/opt/sw/boost_1_55_0/build/lib/libboost_thread.so
make -j4
make install
export LD_LIBRARY_PATH="/opt/sw/liblas/build/lib:$LD_LIBRARY_PATH"
export PATH="/opt/sw/liblas/build/bin:$PATH"
export CPATH="/opt/sw/liblas/build/include:$CPATH"
lasinfo
cd liblas/python/
mkdir -p /opt/sw/liblas/build/lib/python2.7/site-packages
export PYTHONPATH="$PYTHONPATH:/opt/sw/liblas/build/lib/python2.7/site-packages"
python setup.py install --prefix=/opt/sw/liblas/build
export PYTHONPATH="$PYTHONPATH:/opt/sw/liblas/build/lib/python2.7/site-packages"

cd /src

# LASTOOLS (open-source part, i.e. no wine is required)
wget http://www.cs.unc.edu/~isenburg/lastools/download/lastools.zip
unzip lastools.zip
cd LAStools/
make -j4
# Binaries will be found in the 'bin' directory.
# Recommended not to run 'make install' because most of lastools' executables have the same names as the liblas utilities.
# Add to paths
export PATH="/opt/sw/LAStools/bin:$PATH"

cd /src

# Python modules for the benchmark platform
# EPEL is required
yum install -y http://mirror.its.dal.ca/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
# Check that the main repository is enabled in /etc/yum.repos.d/epel.repo (should say enabled=1 under the first, base package).

wget https://bootstrap.pypa.io/get-pip.py
python get-pip.py

pip install --upgrade setuptools

#Requirements:
pip install lxml psutil
pip install numpy --upgrade

# Add python/pointcloud directory to pythonpath
export PYTHONPATH="$PYTHONPATH:/opt/sw/pointcloud-benchmark/python/pointcloud"
