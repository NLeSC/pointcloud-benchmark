include_directories(
    .
    ../include
    ../include/lasNLeSC/capi)

###############################################################################
# Collect programs to build

set(LAS2COL las2col)
set(LAS2PG las2pg)
set(VOX2TXT vox2txt)

# Set the build type to release if it is not explicitly set by the user and 
# isn't in the cache yet
if (NOT CMAKE_BUILD_TYPE )
  set(CMAKE_BUILD_TYPE "Release")
endif()

set(LASNLESC_UTILITIES
    ${LAS2COL} ${LAS2PG} ${VOX2TXT})

if(MSVC)
    foreach(utility ${LASNLESC_UTILITIES})
        set(USER_FILE ${utility}.vcproj.$ENV{USERDOMAIN}.$ENV{USERNAME}.user)
        set(OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/${USER_FILE})
        message(STATUS "Generating ${CMAKE_GENERATOR} user-specific settings in ${USER_FILE}")
        configure_file(${CMAKE_SOURCE_DIR}/cmake/lasNLeSC.vcproj.user.template ${OUTPUT_PATH} @ONLY)
    endforeach()
endif()

###############################################################################
# Configure build targets

set(APPS_CPP_DEPENDENCIES
	${LASZIP_LIBRARY}
	${LIBLAS_LIBRARY}
    ${Boost_LIBRARIES})

link_directories(${Boost_LIBRARY_DIRS})

# Build las2col
if(LAS2COL)
    find_package (Threads)
    set(LAS2COL_SRC lascommon.c ${LAS2COL}.c)
    add_executable( ${LAS2COL}  ${LAS2COL_SRC} )
    target_link_libraries(${LAS2COL} ${LASNLESC_C_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} )
endif()

# Build las2pg
if(LAS2PG)
    find_package (Threads)
    set(LAS2PG_SRC lascommon.c ${LAS2PG}.c)
    add_executable( ${LAS2PG}  ${LAS2PG_SRC} )
    target_link_libraries(${LAS2PG} ${LASNLESC_C_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} )
endif()

# Build vox2txt
if(VOX2TXT)
    find_package (Threads)
    set(VOX2TXT_SRC ${VOX2TXT}.c)
    add_executable( ${VOX2TXT}  ${VOX2TXT_SRC} )
    target_link_libraries(${VOX2TXT} ${LASNLESC_C_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} )
endif()

###############################################################################
# Targets installation

install(TARGETS ${LASNLESC_UTILITIES}
    RUNTIME DESTINATION ${LASNLESC_BIN_DIR}
    LIBRARY DESTINATION ${LASNLESC_LIB_DIR}
    ARCHIVE DESTINATION ${LASNLESC_LIB_DIR})


if(UNIX)

  if(WITH_PKGCONFIG)
    
    set(PKGCFG_PREFIX "${CMAKE_INSTALL_PREFIX}")
    set(PKGCFG_INC_DIR "${LASNLESC_INCLUDE_SUBDIR}")
    set(PKGCFG_LIB_DIR "${LASNLESC_LIB_SUBDIR}")
    set(PKGCFG_REQUIRES  "")
    set(PKGCFG_VERSION ${VERSION})
    set(PKGCFG_LINK_FLAGS "-llasNLeSC -llasNLeSC_c")
    set(PKGCFG_COMPILE_FLAGS "")
    if(LIBXML2_FOUND)
      set(PKGCFG_REQUIRES "${PKGCFG_REQUIRES} libxml-2.0")
    endif()
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/lasNLeSC.pc.in
      ${CMAKE_CURRENT_BINARY_DIR}/lasNLeSC.pc @ONLY)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/lasNLeSC.pc
      DESTINATION ${LASNLESC_LIB_DIR}/pkgconfig
      PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
    
  else()

    # Autoconf compatibility variables to use the same script source.
    set(prefix ${CMAKE_INSTALL_PREFIX})
    set(exec_prefix ${CMAKE_INSTALL_PREFIX}/bin)
    set(libdir ${CMAKE_INSTALL_PREFIX}/lib)

    GET_DIRECTORY_PROPERTY(LASNLESC_DEFINITIONS DIRECTORY ${lasNLeSC_SOURCE_DIR}/ COMPILE_DEFINITIONS)   

    set(LASNLESC_CONFIG_DEFINITIONS "")
    foreach(definition ${LASNLESC_DEFINITIONS})
        set(LASNLESC_CONFIG_DEFINITIONS "${LASNLESC_CONFIG_DEFINITIONS} -D${definition}")
    endforeach()

     
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/lasNLeSC-config.in
      ${CMAKE_CURRENT_BINARY_DIR}/lasNLeSC-config @ONLY)
      
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/lasNLeSC-config
      DESTINATION bin/
      PERMISSIONS
      OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

  endif()

endif()
