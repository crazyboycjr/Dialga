include_directories(${dialga_INCLUDE_DIRS})
link_directories(${dialga_LIBRARY_DIRS})

# the executable
add_executable(client client.cpp)
target_link_libraries(client ${dialga_LINKER_LIBS})
add_executable(server server.cpp)
target_link_libraries(server ${dialga_LINKER_LIBS})
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark ${dialga_LINKER_LIBS})
