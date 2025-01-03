# Create a library from the utils source files
add_library(utils
    distance.h
    distance.cc
    lock.h
)

# Specify the include directories for the utils library
target_include_directories(utils INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
