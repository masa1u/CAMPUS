# benchmarks/CMakeLists.txt

find_package (gflags REQUIRED)
include_directories (${gflags_INCLUDE_DIR})

add_executable(bench_campus bench_campus.cc)
add_executable(bench_serial bench_serial.cc)
add_executable(bench_nocontrol bench_nocontrol.cc)

target_link_libraries(bench_campus campus gflags)
target_link_libraries(bench_serial serial gflags)
target_link_libraries(bench_nocontrol nocontrol gflags)

# Option to include faiss build
option(BUILD_FAISS "Build faiss library" OFF)

# if(BUILD_FAISS)
#     add_executable(bench_ivfflat bench_ivfflat.cc)
#     target_link_libraries(bench_ivfflat ivflat gflags)
# endif()

if(BUILD_FAISS)
    # Include faiss directories
    include_directories(${CMAKE_SOURCE_DIR}/faiss)
    include_directories(${CMAKE_BINARY_DIR}/faiss)

    add_executable(bench_ivfflat bench_ivfflat.cc)
    target_link_libraries(bench_ivfflat faiss gflags)
endif()