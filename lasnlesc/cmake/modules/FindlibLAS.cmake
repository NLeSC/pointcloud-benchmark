###############################################################################
#
# CMake module to search for LibLAS library
#
# On success, the macro sets the following variables:
# LIBLAS_FOUND       = if the library found
# LIBLAS_LIBRARIES   = full path to the library
# LIBLAS_INCLUDE_DIR = where to find the library headers also defined,
#                       but not for general use are
# LIBLAS_LIBRARY     = where to find the PROJ.4 library.
# LIBLAS_VERSION     = version of library which was found, e.g. "1.7.0"
#
# Copyright (c) 2009 Mateusz Loskot <mateusz@loskot.net>
#
# Module source: http://github.com/mloskot/workshop/tree/master/cmake/
#
# Redistribution and use is allowed according to the terms of the BSD license.
# For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#
###############################################################################
MESSAGE(STATUS "Searching for LIBlas ${LIBlas_FIND_VERSION}+ library")

IF(LIBLAS_INCLUDE_DIR)
  # Already in cache, be silent
  SET(LIBLAS_FIND_QUIETLY TRUE)
ENDIF()

IF(WIN32)
  SET(OSGEO4W_IMPORT_LIBRARY liblas)
  IF(DEFINED ENV{OSGEO4W_ROOT})
    SET(OSGEO4W_ROOT_DIR $ENV{OSGEO4W_ROOT})
    MESSAGE(STATUS "Trying OSGeo4W using environment variable OSGEO4W_ROOT=$ENV{OSGEO4W_ROOT}")
  ELSE()
    SET(OSGEO4W_ROOT_DIR c:/OSGeo4W)
    MESSAGE(STATUS "Trying OSGeo4W using default location OSGEO4W_ROOT=${OSGEO4W_ROOT_DIR}")
  ENDIF()
ENDIF()


FIND_PATH(LIBLAS_INCLUDE_DIR
  liblas.hpp
  PATH_PREFIXES liblas
  PATHS
  /usr/include
  /usr/local/include
  ${OSGEO4W_ROOT_DIR}/include
  NO_DEFAULT_PATH)

SET(LIBLAS_NAMES ${OSGEO4W_IMPORT_LIBRARY} las_c)

FIND_LIBRARY(LIBLAS_LIBRARY
  NAMES ${LIBLAS_NAMES}
  PATHS
  /usr/lib
  /usr/local/lib
  ${OSGEO4W_ROOT_DIR}/lib
  ${LIBLAS17_HOME}/lib
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH)

IF(LIBLAS_FOUND)
  SET(LIBLAS_LIBRARIES ${LIBLAS_LIBRARY})
ENDIF()

IF(LIBLAS_INCLUDE_DIR)
  SET(LIBLAS_VERSION 0)

  SET(LIBLAS_VERSION_H "${LIBLAS_INCLUDE_DIR}/liblas/capi/las_version.h")
  FILE(READ ${LIBLAS_VERSION_H} LIBLAS_VERSION_H_CONTENTS)

  IF (DEFINED LIBLAS_VERSION_H_CONTENTS)
    string(REGEX REPLACE ".*#define[ \t*]LIBLAS_VERSION_MAJOR[ \t*]+([0-9]+).*" "\\1" LIBLAS_VERSION_MAJOR "${LIBLAS_VERSION_H_CONTENTS}")
    string(REGEX REPLACE ".*#define[ \t*]LIBLAS_VERSION_MINOR[ \t*]+([0-9]+).*" "\\1" LIBLAS_VERSION_MINOR "${LIBLAS_VERSION_H_CONTENTS}")
    string(REGEX REPLACE ".*#define[ \t*]LIBLAS_VERSION_REV[ \t*]+([0-9]+).*"   "\\1" LIBLAS_VERSION_REVISION   "${LIBLAS_VERSION_H_CONTENTS}")

    if(NOT ${LIBLAS_VERSION_MAJOR} MATCHES "[0-9]+")
      message(FATAL_ERROR "LIBlas version parsing failed for LIBLAS_VERSION_MAJOR!")
    endif()
    if(NOT ${LIBLAS_VERSION_MINOR} MATCHES "[0-9]+")
      message(FATAL_ERROR "LIBlas version parsing failed for LIBLAS_VERSION_MINOR!")
    endif()
    if(NOT ${LIBLAS_VERSION_REVISION} MATCHES "[0-9]+")
      message(FATAL_ERROR "LIBlas version parsing failed for LIBLAS_VERSION_REVISION!")
    endif()


    SET(LIBLAS_VERSION "${LIBLAS_VERSION_MAJOR}.${LIBLAS_VERSION_MINOR}.${LIBLAS_VERSION_REVISION}"
      CACHE INTERNAL "The version string for LIBlas library")

    IF (LIBLAS_VERSION VERSION_EQUAL LIBlas_FIND_VERSION OR
        LIBLAS_VERSION VERSION_GREATER LIBlas_FIND_VERSION)
      MESSAGE(STATUS "Found LIBlas version: ${LIBLAS_VERSION}")
    ELSE()
      MESSAGE(FATAL_ERROR "LIBlas version check failed. Version ${LIBLAS_VERSION} was found, at least version ${LIBlas_FIND_VERSION} is required")
    ENDIF()
  ELSE()
    MESSAGE(FATAL_ERROR "Failed to open ${LIBLAS_VERSION_H} file")
  ENDIF()

ENDIF()

# Handle the QUIETLY and REQUIRED arguments and set LIBLAS_FOUND to TRUE
# if all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LIBlas DEFAULT_MSG LIBLAS_LIBRARY LIBLAS_INCLUDE_DIR)
