# Create a library from the campus source files
add_library(serial
    serial.cc
    serial.h
    entity.h
    insert.cc
    node.h
    node.cc
)

# Link the utils library
target_link_libraries(serial utils)