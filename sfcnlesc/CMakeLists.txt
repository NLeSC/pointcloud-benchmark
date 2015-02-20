cmake_minimum_required(VERSION 2.8)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")
project("SFC NLeSC")

if(UNIX)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")
endif()
 
set(SFCNLESC_BUILD_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE})

# Output directory in which to build RUNTIME target files.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${SFCNLESC_BUILD_OUTPUT_DIRECTORY})

# Output directory in which to build LIBRARY target files
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${SFCNLESC_BUILD_OUTPUT_DIRECTORY})

# Output directory in which to build ARCHIVE target files.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${SFCNLESC_BUILD_OUTPUT_DIRECTORY}) 

message(STATUS "Searching for GEOS 3.4.2+ - done")
find_package(GEOS 3.4.2 REQUIRED)
if(GEOS_FOUND)
    set(CMAKE_REQUIRED_LIBRARIES ${GEOS_LIBRARY})
    include_directories(${GEOS_INCLUDE_DIR})
    add_definitions(-DHAVE_GEOS=1)
endif()

set(SFCNLESC_BIN_DIR ${SFCNLESC_BIN_SUBDIR})
set(SFCNLESC_LIB_DIR ${SFCNLESC_LIB_SUBDIR})
set(SFCNLESC_INCLUDE_DIR ${SFCNLESC_INCLUDE_SUBDIR})

add_subdirectory(src)
