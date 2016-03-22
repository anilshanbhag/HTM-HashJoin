cmake_minimum_required(VERSION 3.2)
include_directories(/Volumes/ramdisk/tsx-tools/include)
find_package (TBB)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
include_directories(${TBB_INCLUDE_DIRS})
add_executable(main main.cpp)
target_link_libraries(main ${TBB_LIBRARIES})
set_property(TARGET main PROPERTY CXX_STANDARD 14)

