add_definitions(-DUSE_FDF_API -DUSE_BTREE -std=c99)
SET (CMAKE_RUNTIME_OUTPUT_DIRECTORY ../bin)
add_executable(fdf_test_engine fdf_test_engine.c thread.c) 

#To compile link inside the FDF tree
set(LIBRARY_OUTPUT_PATH .)
add_library(btreedll_l STATIC ${PROJECT_SOURCE_DIR}/dll/btree_dll.c)
target_link_libraries(fdf_test_engine btreedll_l pthread dl aio event)

#To compile copy inside the tree
#target_link_libraries(fdf_test_engine fdfdll pthread dl aio event)
