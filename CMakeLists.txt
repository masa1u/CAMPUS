cmake_minimum_required(VERSION 3.10)
project(CampusProject)

set(CMAKE_CXX_STANDARD 17)


# Add subdirectories
add_subdirectory(src)
add_subdirectory(benchmarks)

# スクリプトファイルをビルドディレクトリにコピー
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_serial.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_serial.sh COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_serial_siftsmall.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_serial_siftsmall.sh COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_serial_sift.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_serial_sift.sh COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_serial_gist.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_serial_gist.sh COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_campus.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_campus.sh COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_campus_siftsmall.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_campus_siftsmall.sh COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_campus_sift.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_campus_sift.sh COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/benchmarks/bench_campus_gist.sh ${CMAKE_BINARY_DIR}/benchmarks/bench_campus_gist.sh COPYONLY)