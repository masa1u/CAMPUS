cmake_minimum_required(VERSION 3.10)
project(CampusProject)

set(CMAKE_CXX_STANDARD 17)

# Add subdirectories
add_subdirectory(src)
add_subdirectory(benchmarks)

# Option to include faiss build
option(BUILD_FAISS "Build faiss library" OFF)

if(BUILD_FAISS)
    # Set faiss build options
    set(FAISS_ENABLE_GPU OFF)
    set(FAISS_ENABLE_PYTHON OFF)
    set(FAISS_ENABLE_CUVS OFF)
    set(BUILD_TESTING OFF)
    set(BUILD_SHARED_LIBS OFF)
    set(FAISS_ENABLE_C_API OFF)
    add_subdirectory(faiss)
endif()

# benchmarksにあるスクリプトファイルをすべてビルドディレクトリにコピー
file(GLOB SCRIPTS "${CMAKE_SOURCE_DIR}/benchmarks/*.sh")
foreach(SCRIPT ${SCRIPTS})
    get_filename_component(SCRIPT_NAME ${SCRIPT} NAME)
    configure_file(${SCRIPT} ${CMAKE_BINARY_DIR}/benchmarks/${SCRIPT_NAME} COPYONLY)
endforeach()