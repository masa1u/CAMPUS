# benchmarks/CMakeLists.txt

find_package (gflags REQUIRED)
include_directories (${gflags_INCLUDE_DIR})

add_executable(bench_campus bench_campus.cc)
add_executable(bench_serial bench_serial.cc)
add_executable(bench_nocontrol bench_nocontrol.cc)

# target_link_libraries(bench_campus ${HDF5_LIBRARIES})
target_link_libraries(bench_campus campus gflags)
target_link_libraries(bench_serial serial gflags)
target_link_libraries(bench_nocontrol nocontrol gflags)