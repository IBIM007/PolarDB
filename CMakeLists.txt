cmake_minimum_required(VERSION 3.10)

project(page_engine VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "-pthread -ldl -lrt ${CMAKE_CXX_FLAGS}")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

set(SOURCE_DIR page_engine/)
file(GLOB_RECURSE SOURCE_FILES "${SOURCE_DIR}/*.cpp" "${SOURCE_DIR}/*.cc" "${SOURCE_DIR}/*.c")

include_directories(include)

add_library(page_engine ${SOURCE_FILES})

add_executable(random_read_write test/random_read_write.cc)
target_link_libraries(random_read_write page_engine)

add_executable(run_trace test/run_trace.cc)
target_link_libraries(run_trace page_engine)
