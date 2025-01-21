# Create a library from the campus source files
add_library(nocontrol
    nocontrol.cc
    nocontrol.h
    entity.h
    insert.cc
    node.h
    node.cc
)

# Link the utils library
target_link_libraries(nocontrol utils)