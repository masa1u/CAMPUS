cmake_minimum_required(VERSION 3.10)
project(CampusProject)

set(CMAKE_CXX_STANDARD 17)

# HDF5 ライブラリのパスを設定
find_package(HDF5 REQUIRED)
include_directories(${HDF5_INCLUDE_DIRS})
link_directories(${HDF5_LIBRARY_DIRS})

# Add subdirectories
add_subdirectory(src)
add_subdirectory(benchmarks)