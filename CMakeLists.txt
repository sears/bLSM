cmake_minimum_required(VERSION 2.4) # For all I know, 2.0 works too...

# Make cmake warning go away.  They changed the default library search behavior.
# The new behavior seems preferable, though it shouldn't affect us either way.

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

Project(Stasis)

SET(PACKAGE_VERSION 1)

SUBDIRS(test)

# Main decisions
SET(BUILD_SHARED_LIBS ON)
#ENABLE_TESTING()
INCLUDE(CTest)

INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)

SET(CMAKE_REQUIRED_FLAGS "-lm -lstasis -lpthread")

MACRO(CREATE_CHECK NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.cpp)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} nice ./${NAME})
ENDMACRO(CREATE_CHECK)

MACRO(CREATE_EXECUTABLE NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.cpp)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
ENDMACRO(CREATE_EXECUTABLE)

# Output the config.h file
#CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../stasis/
		    ${CMAKE_CURRENT_SOURCE_DIR}/../stasis/src/
		    ${CMAKE_CURRENT_SOURCE_DIR}
		    /usr/include)
# set linker path for this and all subdirs
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../stasis/build/src/stasis ${CMAKE_CURRENT_BINARY_DIR})

IF ( "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU" )
  SET(COMMON_LIBRARIES logstore stasis m pthread stdc++)
  SET(CMAKE_C_FLAGS "-g -Wall -pedantic -std=gnu99 -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_C_FLAGS}")
  SET(CMAKE_CXX_FLAGS "-g -Wall -Wno-long-long -pedantic -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_CXX_FLAGS}")
ELSEIF ( "${CMAKE_C_COMPILER_ID}" STREQUAL "SunPro" )
  SET(COMMON_LIBRARIES logstore stasis m pthread Crun Cstd)
  SET(CMAKE_C_FLAGS "-g -xc99=all -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_C_FLAGS}")
  SET(CMAKE_CXX_FLAGS "-g -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_CXX_FLAGS}")
ELSE( "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU" )
  # TODO: how to detect compilers correctly on old cmake???  This is a huge hack; it uses old gcc 
  # options, since cmake is old...
  SET(COMMON_LIBRARIES logstore stasis m pthread stdc++)
  SET(CMAKE_C_FLAGS "-g -Wall -pedantic -std=gnu99 -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_C_FLAGS}")
  SET(CMAKE_CXX_FLAGS "-g -Wall -Wno-long-long -pedantic -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_CXX_FLAGS}")
ENDIF ( "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU" )

#CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)

ADD_LIBRARY(logstore logserver.cpp logstore.cpp logiterators.cpp datapage.cpp merger.cpp tuplemerger.cpp)

CREATE_EXECUTABLE(server)