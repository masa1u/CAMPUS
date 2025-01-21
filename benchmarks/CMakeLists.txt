# benchmarks/CMakeLists.txt

add_executable(bench_campus bench_campus.cc)
add_executable(bench_serial bench_serial.cc)
add_executable(bench_nocontrol bench_nocontrol.cc)

# HDF5 ライブラリをリンク
# target_link_libraries(bench_campus ${HDF5_LIBRARIES})
target_link_libraries(bench_campus campus)
target_link_libraries(bench_serial serial)
target_link_libraries(bench_nocontrol nocontrol)