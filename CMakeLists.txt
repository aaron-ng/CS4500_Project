cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(Threads REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(
        COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download
)

if (result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()

execute_process(
        COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download
)

if (result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
        ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
        EXCLUDE_FROM_ALL
        )

find_package (Threads)

add_executable(main src/main.cpp src/ea2/kvstore/kvstore.cpp)
target_link_libraries (main ${CMAKE_THREAD_LIBS_INIT})

add_executable(tests src/ea2/kvstore/kvstore.cpp
        test/test.cpp
        test/test_dataframe.cpp
        test/test_util.cpp
        test/test_ea2.cpp
        test/test_network.cpp
        test/test_map.cpp
        test/test_adapter.cpp
        test/test_demo.cpp
)
target_link_libraries (tests ${CMAKE_THREAD_LIBS_INIT} gtest)
