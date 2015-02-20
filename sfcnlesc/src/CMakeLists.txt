# Set the build type to release if it is not explicitly set by the user and 
# isn't in the cache yet
if (NOT CMAKE_BUILD_TYPE )
  set(CMAKE_BUILD_TYPE "Release")
endif()

add_executable(sfcnlesc sfcnlesc.cc)
target_link_libraries(sfcnlesc ${GEOS_LIBRARY})
install (TARGETS sfcnlesc DESTINATION bin)
