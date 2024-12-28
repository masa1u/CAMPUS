# Create an executable for the benchmark
add_executable(bench_campus
    bench_campus.cc
)

# Link the campus library
target_link_libraries(bench_campus campus)