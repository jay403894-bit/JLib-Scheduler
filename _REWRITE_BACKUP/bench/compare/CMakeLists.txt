# Comparison harness: JLib::Scheduler vs enkiTS.
#
# DELIBERATELY OUT OF THE MAIN BUILD AND OUT OF CI. It needs a third-party checkout that the library
# itself does not, and three vendored schedulers building on four platforms would be a maintenance
# burden attached to every push for a target nobody runs in CI. Opt in explicitly:
#
#   cmake -B build-compare -DCMAKE_BUILD_TYPE=Release -DJLIBSCHED_ENKITS_DIR=C:/enkiTS
#   cmake --build build-compare --config Release
#   ./build-compare/bin/Release/CompareEnkiTS.exe [poolSize]
#
# enkiTS is NOT vendored into this repo. It is zlib-licensed and could be, but a comparison harness
# is not a dependency of the library and copying someone else's scheduler in would imply otherwise.
# Point at a clone instead: git clone https://github.com/dougbinks/enkiTS

if(NOT JLIBSCHED_ENKITS_DIR)
    message(STATUS "compare: JLIBSCHED_ENKITS_DIR not set, skipping the enkiTS comparison target")
    return()
endif()

if(NOT EXISTS "${JLIBSCHED_ENKITS_DIR}/src/TaskScheduler.h")
    message(FATAL_ERROR
        "compare: no enkiTS at ${JLIBSCHED_ENKITS_DIR} -- expected ${JLIBSCHED_ENKITS_DIR}/src/TaskScheduler.h. "
        "Clone it with: git clone https://github.com/dougbinks/enkiTS")
endif()

# BOTH libraries have a header called TaskScheduler.h, and both are on the include path, so a plain
# #include picks whichever directory comes first -- silently, and differently per generator. Alias
# enkiTS's to an unambiguous name and include that instead.
set(_enki_alias "${CMAKE_CURRENT_BINARY_DIR}/enki_include")
file(MAKE_DIRECTORY "${_enki_alias}")
file(WRITE "${_enki_alias}/TaskScheduler_enki.h"
     "#pragma once\n#include \"${JLIBSCHED_ENKITS_DIR}/src/TaskScheduler.h\"\n")

add_executable(CompareEnkiTS
    "${CMAKE_CURRENT_SOURCE_DIR}/compare_enkits.cpp"
    "${JLIBSCHED_ENKITS_DIR}/src/TaskScheduler.cpp")

target_include_directories(CompareEnkiTS PRIVATE "${_enki_alias}")
target_link_libraries(CompareEnkiTS PRIVATE Scheduler)
target_compile_features(CompareEnkiTS PRIVATE cxx_std_17)

message(STATUS "compare: enkiTS comparison target enabled (${JLIBSCHED_ENKITS_DIR})")

# ---------------------------------------------------------------- Taskflow
# Separate target and separate BINARY from the enkiTS comparison, deliberately -- see the header of
# compare_taskflow.cpp. Taskflow 4.x requires C++20 and this is the only target that moves; the
# library and every other target stay C++17.
#
#   cmake -B build-compare -DCMAKE_BUILD_TYPE=Release -DJLIBSCHED_TASKFLOW_DIR=C:/taskflow
#
# Header-only, so there is nothing to compile but this file. Not vendored, for the same reason
# enkiTS is not: a comparison harness is not a dependency of the library.
if(JLIBSCHED_TASKFLOW_DIR)
    if(NOT EXISTS "${JLIBSCHED_TASKFLOW_DIR}/taskflow/taskflow.hpp")
        message(FATAL_ERROR
            "compare: no Taskflow at ${JLIBSCHED_TASKFLOW_DIR} -- expected "
            "${JLIBSCHED_TASKFLOW_DIR}/taskflow/taskflow.hpp. "
            "Clone it with: git clone https://github.com/taskflow/taskflow")
    endif()

    add_executable(CompareTaskflow "${CMAKE_CURRENT_SOURCE_DIR}/compare_taskflow.cpp")
    target_include_directories(CompareTaskflow PRIVATE "${JLIBSCHED_TASKFLOW_DIR}")
    target_link_libraries(CompareTaskflow PRIVATE Scheduler)
    target_compile_features(CompareTaskflow PRIVATE cxx_std_20)
    message(STATUS "compare: Taskflow comparison target enabled (${JLIBSCHED_TASKFLOW_DIR})")
else()
    message(STATUS "compare: JLIBSCHED_TASKFLOW_DIR not set, skipping the Taskflow comparison target")
endif()

# ---------------------------------------------------------------- marl
# The most interesting of the three and the only true peer: marl runs EVERY task on a fiber, so it
# is the architectural sibling of this scheduler's hybrid rather than a contrast to it. It is also
# ARCHIVED (last commit 2026-04-27), so treat its numbers as calibration for the fiber path and not
# as a recommendation.
#
#   cmake -B build-compare -DCMAKE_BUILD_TYPE=Release -DJLIBSCHED_MARL_DIR=C:/marl
#
# Unlike enkiTS and Taskflow, marl is a real library with per-architecture context-switch sources,
# so it comes in via add_subdirectory rather than a file list. Every MARL_BUILD_* option already
# defaults to OFF, so this builds the library alone -- no tests, benchmarks or examples.
if(JLIBSCHED_MARL_DIR)
    if(NOT EXISTS "${JLIBSCHED_MARL_DIR}/include/marl/scheduler.h")
        message(FATAL_ERROR
            "compare: no marl at ${JLIBSCHED_MARL_DIR} -- expected "
            "${JLIBSCHED_MARL_DIR}/include/marl/scheduler.h. "
            "Clone it with: git clone https://github.com/google/marl")
    endif()

    add_subdirectory("${JLIBSCHED_MARL_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/marl-build" EXCLUDE_FROM_ALL)
    add_executable(CompareMarl "${CMAKE_CURRENT_SOURCE_DIR}/compare_marl.cpp")
    target_link_libraries(CompareMarl PRIVATE Scheduler marl)
    target_compile_features(CompareMarl PRIVATE cxx_std_17)
    message(STATUS "compare: marl comparison target enabled (${JLIBSCHED_MARL_DIR})")
else()
    message(STATUS "compare: JLIBSCHED_MARL_DIR not set, skipping the marl comparison target")
endif()
