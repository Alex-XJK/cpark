cmake_minimum_required(VERSION 3.25)

project(
  cpark
  VERSION 0.0.1
  LANGUAGES CXX)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  # Currently this project requires and only supports C++20.
  set(CMAKE_CXX_STANDARD 20)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

# Options indicating whether certain components of the project should be built.
option(CPARK_BUILD_TESTS "Build cpark's unit tests" ON)
option(CPARK_BUILD_EXAMPLES "Build cpark's examples" ON)
option(CPARK_BUILD_DISTRIBUTED "Build cpark with distributed computing support"
       ON)

# Set directories in the project. User should only include files in this
# directory.
set(cpark_user_include_dir include/)
set(cpark_lib_sources_dir lib/)
set(cpark_lib_headers_dir lib/)
set(cpark_examples_sources_dir examples/)
set(cpark_tests_sources_dir tests/)

# Set concepts diagnostics depth for GCC.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options(-fconcepts-diagnostics-depth=3
                      -ftemplate-backtrace-limit=0)
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options($<$<CONFIG:DEBUG>:-gdwarf-3>)
  endif(CMAKE_BUILD_TYPE STREQUAL "Debug")
endif()

if(CPARK_BUILD_DISTRIBUTED)
  add_definitions(-DCPARK_DISTRIBUTED)
endif(CPARK_BUILD_DISTRIBUTED)

# The cpark main library. (Perhaps we do not need a static library. This project
# is full of templates, so everything should be in header files.)
file(GLOB cpark_lib_sources CONFIGURE_DEPENDS ${cpark_lib_sources_dir}/*.cpp)
add_library(cpark)
target_sources(cpark PRIVATE ${cpark_lib_sources})
target_include_directories(cpark PUBLIC ${cpark_user_include_dir})

# Build the examples if its option is set.
if(CPARK_BUILD_EXAMPLES)
  file(GLOB examples_sources ${cpark_examples_sources_dir}/*.cpp)
  # Each example file will have one executable.
  foreach(example_file ${examples_sources})
    # Cut off the file extension and directory path.
    get_filename_component(example_name ${example_file} NAME_WE)
    add_executable(${example_name} ${example_file})
    target_link_libraries(${example_name} cpark)
    target_include_directories(${example_name} PRIVATE ${cpark_user_include_dir}
                                                       ${cpark_lib_sources_dir})
  endforeach(example_file ${examples_sources})
endif(CPARK_BUILD_EXAMPLES)

# Build unit tests if its option is set.
if(CPARK_BUILD_TESTS)
  enable_testing()

  # Fetch googletest from its github repository. Not using locally installed
  # googletest (by FindPackage) here because it may cause incompatibility. TODO:
  # First use FindPackage to get locally installed googletest. Fetch googletest
  # from remote only if the local one does not exist or is incompatible.
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.13.0)
  FetchContent_MakeAvailable(googletest)

  # Let the user know about the information of the specifically populated
  # googletest for this project.
  if(googletest_POPULATED)
    message(
      "A googletest library specified for this project is already populated by cmake."
    )
  elseif(googletest_POPULATED)
    message("Newly populated a googletest library under this project.")
  endif(googletest_POPULATED)
  message("googletest source directory: ${googletest_SOURCE_DIR}")

  file(GLOB cpark_tests_sources CONFIGURE_DEPENDS
       ${cpark_tests_sources_dir}/*.cpp)
  add_executable(cpark_tests)
  target_sources(cpark_tests PRIVATE ${cpark_tests_sources})
  target_include_directories(cpark_tests PRIVATE ${cpark_user_include_dir}
                                                 ${cpark_lib_headers_dir})
  target_link_libraries(cpark_tests PRIVATE cpark GTest::gtest_main)

  add_test(NAME basic_tests COMMAND cpark_tests)
endif(CPARK_BUILD_TESTS)
