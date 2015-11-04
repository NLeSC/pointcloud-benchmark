apt-get update

apt-get install -y curl cmake libcunit1-dev autoconf automake g++ \
python-dev swig libjpeg-turbo8-dev libbz2-dev zlib1g-dev git libxml2-dev \
python-numpy libflann-dev libsqlite0-dev libopenmpi-dev subversion \
libcurl4-openssl-dev libxerces-c-dev unixodbc-dev libjson-c-dev libpcre3-dev \
ant libbison-dev gettext libtool libreadline-dev unzip libfreetype6 \
libfreetype6-dev libxslt1-dev python-pip \
libgeos-dev libtiff4-dev libgeotiff-dev libboost1.55-dev libgdal-dev liblas-dev \

# LASTOOLS (open-source part, i.e. no wine is required)
curl -O http://www.cs.unc.edu/~isenburg/lastools/download/lastools.zip
unzip lastools.zip
cd LAStools/
make -j4
# Binaries will be found in the 'bin' directory.
# Recommended not to run 'make install' because most of lastools' executables have the same names as the liblas utilities.
# Add to paths
cp -R bin/* /usr/local/bin/

pip install --upgrade pip

# python packages
pip install lxml psutil psycopq2 matplotlib numpy gdal
pip install numpy --upgrade
