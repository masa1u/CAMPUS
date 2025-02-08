cmake_minimum_required(VERSION 3.10)
project(CampusProject)

set(CMAKE_CXX_STANDARD 17)


# Add subdirectories
add_subdirectory(src)
add_subdirectory(benchmarks)

# スクリプトファイルをビルドディレクトリにコピー
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_serial.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_serial.sh COPYONLY)
