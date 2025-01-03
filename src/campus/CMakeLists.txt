# Create a library from the campus source files
add_library(campus
    campus.cc
    campus.h
    entity.h
    insert.cc
    node.h
    query.cc
    version.cc
    version.h
)

# Link the utils library
target_link_libraries(campus utils)