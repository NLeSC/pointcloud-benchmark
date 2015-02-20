project(lasNLeSC)

MESSAGE(STATUS "Source dir - ${lasNLeSC_SOURCE_DIR}")


SET(LASNLESC_VERSION_MAJOR "0")
SET(LASNLESC_VERSION_MINOR "1")
SET(LASNLESC_VERSION_PATCH "0")
set(VERSION "${LASNLESC_VERSION_MAJOR}.${LASNLESC_VERSION_MINOR}.${LASNLESC_VERSION_PATCH}")

set(CMAKE_INCLUDE_DIRECTORIES_PROJECT_BEFORE ON)

# Choose to use pkg-config or not
set(WITH_PKGCONFIG FALSE CACHE BOOL
  "Choose whether a pkgconfig file (libNLESC.pc) should be installed")

###############################################################################
# CMake settings
cmake_minimum_required(VERSION 2.6.0)

set(CMAKE_COLOR_MAKEFILE ON)

# Allow advanced users to generate Makefiles printing detailed commands
mark_as_advanced(CMAKE_VERBOSE_MAKEFILE)

# Path to additional CMake modules
set(CMAKE_MODULE_PATH "${lasNLeSC_SOURCE_DIR}/cmake/modules" ${CMAKE_MODULE_PATH})
MESSAGE(STATUS "CMAKE_MODULE_PATH - ${CMAKE_MODULE_PATH}")

###############################################################################
# General build settings

# Default to debug mode until final 1.6.0 release 
if(NOT MSVC_IDE)
  if(NOT CMAKE_BUILD_TYPE)

  set(CMAKE_BUILD_TYPE Release CACHE STRING 
    "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel" FORCE)
  endif()
    message(STATUS "Setting libNLESC build type - ${CMAKE_BUILD_TYPE}")
endif()

# TODO: Still testing the output paths --mloskot
set(LASNLESC_BUILD_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE})

# Output directory in which to build RUNTIME target files.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${LASNLESC_BUILD_OUTPUT_DIRECTORY})

# Output directory in which to build LIBRARY target files
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${LASNLESC_BUILD_OUTPUT_DIRECTORY})

# Output directory in which to build ARCHIVE target files.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${LASNLESC_BUILD_OUTPUT_DIRECTORY}) 

file(READ "doc/index.txt" README )
file(WRITE "README.txt" "${README}")

###############################################################################
# Platform and compiler specific settings

# Recommended C++ compilation flags
# -Weffc++
set(LASNLESC_COMMON_CXX_FLAGS
        "-pedantic -ansi -Wall -Wpointer-arith -Wcast-align -Wcast-qual -Wfloat-equal -Wredundant-decls -Wno-long-long")

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LASNLESC_COMMON_CXX_FLAGS}")
if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++98")
endif()

    elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang" OR "${CMAKE_CXX_COMPILER}" MATCHES "clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LASNLESC_COMMON_CXX_FLAGS}")
endif()

###############################################################################
# Search for dependencies

# Boost C++ Libraries support - required
message(STATUS "Searching for Boost 1.38+ - done")

# Default to using static, multithreaded libraries for 
# linking under MSVC.  This is because we show users how to 
# use boostpro.com installer and install those options when linking 
# on windows in the compilation documentation.

# NOTE: Add iostreams to COMPONENTS list to enable bigfile_boost_iostreams_test
find_package(Boost 1.38 COMPONENTS program_options thread REQUIRED)

if(Boost_FOUND AND Boost_PROGRAM_OPTIONS_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
endif()

# make these available for the user to set.
mark_as_advanced(CLEAR Boost_INCLUDE_DIR) 
mark_as_advanced(CLEAR Boost_LIBRARY_DIRS) 
link_directories(${Boost_LIBRARY_DIRS}) 

# LASLIB support 
message(STATUS "Searching for LIBlas 1.7.0+ - done")
find_package(libLAS 1.7.0)

if(LIBLAS_FOUND)    
    set(CMAKE_REQUIRED_LIBRARIES ${LIBLAS_LIBRARY})
    include_directories(${LIBLAS_INCLUDE_DIR})
    add_definitions(-DHAVE_LIBLAS=1)
    mark_as_advanced(CLEAR LIBLAS_INCLUDE_DIR) 
    mark_as_advanced(CLEAR LIBLAS_LIBRARY) 
    mark_as_advanced(CLEAR LIBLAS_VERSION) 
endif()

if(WITH_ENDIANAWARE)
    add_definitions(-DLASNLESC_ENDIAN_AWARE=1)
endif()
###############################################################################
# Installation settings

# Common locatoins for Unix and Mac OS X
set(DEFAULT_BIN_SUBDIR bin)
set(DEFAULT_LIB_SUBDIR lib)
set(DEFAULT_DATA_SUBDIR share/lasNLeSC)
set(DEFAULT_INCLUDE_SUBDIR include)

# Locations are changeable by user to customize layout of libNLESC installation
# (default values are platform-specific)
set(LASNLESC_BIN_SUBDIR ${DEFAULT_BIN_SUBDIR} CACHE STRING
  "Subdirectory where executables will be installed")
set(LASNLESC_LIB_SUBDIR ${DEFAULT_LIB_SUBDIR} CACHE STRING
  "Subdirectory where libraries will be installed")
set(LASNLESC_INCLUDE_SUBDIR ${DEFAULT_INCLUDE_SUBDIR} CACHE STRING
  "Subdirectory where header files will be installed")
set(LASNLESC_DATA_SUBDIR ${DEFAULT_DATA_SUBDIR} CACHE STRING
  "Subdirectory where data will be installed")

# Mark *_SUBDIR variables as advanced and dedicated to use by power-users only.
mark_as_advanced(LASNLESC_BIN_SUBDIR
  LASNLESC_LIB_SUBDIR LASNLESC_INCLUDE_SUBDIR LASNLESC_DATA_SUBDIR)

# Full paths for the installation
set(LASNLESC_BIN_DIR ${LASNLESC_BIN_SUBDIR})
set(LASNLESC_LIB_DIR ${LASNLESC_LIB_SUBDIR})
set(LASNLESC_INCLUDE_DIR ${LASNLESC_INCLUDE_SUBDIR})
set(LASNLESC_DATA_DIR ${LASNLESC_DATA_SUBDIR})

add_subdirectory(apps)
