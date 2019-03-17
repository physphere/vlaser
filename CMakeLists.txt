cmake_minimum_required(VERSION 2.8.3)
project(vlaser)

set(PROJECT_BINARY_DIR build)
find_package(Threads REQUIRED)

include_directories(inc)
aux_source_directory(src SRC_FILES)
list(REMOVE_ITEM SRC_FILES src/mpal_mpi.cpp)
add_library(vlaser STATIC ${SRC_FILES})

set(CMAKE_CXX_FLAGS "")
set(CMAKE_VERBOSE_MAKEFILE on)
add_definitions(-DVLASER_DEBUG_OUT)

add_executable(vlaser_test test/vlaser_test.cpp)
target_link_libraries(vlaser_test vlaser pthread)

