cmake_minimum_required(VERSION 3.2)
include_directories(/Volumes/ramdisk/tsx-tools/include)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/FindTBB")
find_package (TBB)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
include_directories(${TBB_INCLUDE_DIRS})

add_executable(main main.cpp)
target_link_libraries(main ${TBB_LIBRARIES})
set_property(TARGET main PROPERTY CXX_STANDARD 14)

add_executable(HTMSolo HTMSolo.cpp)
target_link_libraries(HTMSolo ${TBB_LIBRARIES})
set_property(TARGET HTMSolo PROPERTY CXX_STANDARD 14)

add_executable(TestBed TestBed.cpp)
target_link_libraries(TestBed ${TBB_LIBRARIES})
set_property(TARGET TestBed PROPERTY CXX_STANDARD 14)


