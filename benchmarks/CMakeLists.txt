# benchmarks/CMakeLists.txt

add_executable(bench_campus bench_campus.cc)

# HDF5 ライブラリをリンク
target_link_libraries(bench_campus ${HDF5_LIBRARIES})
target_link_libraries(bench_campus campus)