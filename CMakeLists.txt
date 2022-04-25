cmake_minimum_required(VERSION 3.0)
project(dialga)

# for clang's compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_COMPILER_ID "GNU")
set(CMAKE_CXX_STANDARD 17)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(ibverbs REQUIRED)
include_directories(${IBVERBS_INCLUDE_DIRS})
link_directories(${IBVERBS_LIB_DIR})

FILE(GLOB_RECURSE SOURCE "src/*.cpp")

# local variables
set(dialga_LINKER_LIBS_L "")
set(dialga_INCLUDE_DIR_L "")

# pthread
list(APPEND dialga_LINKER_LIBS_L pthread)

# gflags
list(APPEND dialga_LINKER_LIBS_L gflags)

# googlelog
list(APPEND dialga_INCLUDE_DIR_L ${GLOG_INCLUDE_DIRS})
list(APPEND dialga_LINKER_LIBS_L glog)

# ibverbs
list(APPEND dialga_LINKER_LIBS_L ${IBVERBS_LIBRARIES})

# dialga
list(APPEND dialga_INCLUDE_DIR_L "${PROJECT_SOURCE_DIR}/include")

# the library
add_library(dialga ${SOURCE})
include_directories(dialga ${dialga_INCLUDE_DIR_L})
target_link_libraries(dialga ${dialga_LINKER_LIBS_L})

# exports
list(APPEND dialga_LINKER_LIBS_L dialga)
set(dialga_LINKER_LIBS ${dialga_LINKER_LIBS_L} PARENT_SCOPE)
set(dialga_INCLUDE_DIRS ${dialga_INCLUDE_DIR_L} PARENT_SCOPE)
set(dialga_LIBRARY_DIRS "${PROJECT_BINARY_DIR}" PARENT_SCOPE)
