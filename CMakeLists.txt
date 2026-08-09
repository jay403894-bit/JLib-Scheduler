# ============================ JLib::Scheduler ============================
#
# ONE file, valid in TWO repositories -- the same trick Scheduler.vcxproj and
# Scheduler.Standalone.props already use on the MSBuild side.
#
#   * Standalone (the JLib-Scheduler repo): this is the top-level CMakeLists. It declares its own
#     project, configurations and install rules.
#   * Inside the JLib umbrella: the top-level file does add_subdirectory(Scheduler) and this
#     inherits the umbrella's configurations and output layout.
#
# The alternative -- copying the scheduler's build rules into both repos -- means two files that
# must agree about the platform split forever. That is precisely the drift this project has been
# bitten by, so there is one definition and both repos consume it.
#
# Supported: Windows x64 (MSVC), and Linux/Android on x86-64 or AArch64 (GCC/Clang). The context
# switch is hand-written assembly per ABI, so a new platform needs a new ContextSwitch, not a
# compiler flag -- hence the two-axis source layout below.

cmake_minimum_required(VERSION 3.21)

set(JLIBSCHED_STANDALONE OFF)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(JLIBSCHED_STANDALONE ON)
endif()

if(JLIBSCHED_STANDALONE)
    # MASM only exists on Windows; the Linux context switch is GAS, which CMake treats as ASM.
    # The language has to be enabled at project() time, which is why this is not simply a
    # per-target property.
    if(WIN32)
        project(JLibScheduler LANGUAGES CXX ASM_MASM)
    else()
        project(JLibScheduler LANGUAGES CXX ASM)
    endif()

    get_property(_multi GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
    if(_multi)
        set(CMAKE_CONFIGURATION_TYPES "Debug;Development;Release" CACHE STRING "" FORCE)
    elseif(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "Development" CACHE STRING "" FORCE)
    endif()

    # Development: optimized, with symbols AND assertions. Deliberately NOT aliased onto
    # RelWithDebInfo, which defines NDEBUG and would compile out every assert -- the one thing this
    # configuration exists to keep.
    set(CMAKE_CXX_FLAGS_DEVELOPMENT        "/O2 /Ob2 /Zi /DJLIB_DEVELOPMENT" CACHE STRING "")
    set(CMAKE_EXE_LINKER_FLAGS_DEVELOPMENT "/DEBUG /INCREMENTAL:NO"          CACHE STRING "")
    set(CMAKE_STATIC_LINKER_FLAGS_DEVELOPMENT ""                             CACHE STRING "")
    if(NOT MSVC)
        set(CMAKE_CXX_FLAGS_DEVELOPMENT "-O2 -g -DJLIB_DEVELOPMENT" CACHE STRING "" FORCE)
        set(CMAKE_EXE_LINKER_FLAGS_DEVELOPMENT "" CACHE STRING "" FORCE)
    endif()

    # /MDd for Debug, /MD everywhere else. Must match across every linked library or the CRT
    # mismatch shows up as heap corruption rather than a clean error.
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

if(WIN32 AND NOT MSVC)
    message(FATAL_ERROR "On Windows, JLib::Scheduler requires MSVC: the context switch is MASM.")
endif()
# Two axes, both of which must name a directory that exists: an OS (which decides the topology and
# thread layer) and an ARCH (which decides the context switch). Android is accepted alongside Linux
# because bionic differs from glibc only in details the POSIX layer already handles -- Termux's
# CMake reports one or the other depending on version, and both are the same target here.
if(NOT WIN32 AND NOT CMAKE_SYSTEM_NAME MATCHES "^(Linux|Android)$")
    message(FATAL_ERROR
        "JLib::Scheduler supports Windows, Linux and Android. The context switch is hand-written "
        "assembly per ABI, so a new platform needs a new ContextSwitch, not a compiler flag.")
endif()

# Normalise the many spellings a toolchain may use for the same instruction set.
if(WIN32)
    set(JLIBSCHED_ARCH "x86_64")   # the MASM switch is x64-only; the MSVC guard above is the gate
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    set(JLIBSCHED_ARCH "x86_64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64|ARM64)$")
    set(JLIBSCHED_ARCH "aarch64")
else()
    message(FATAL_ERROR
        "JLib::Scheduler has no context switch for CMAKE_SYSTEM_PROCESSOR='${CMAKE_SYSTEM_PROCESSOR}'. "
        "Supported: x86-64 and AArch64. Porting means adding src/posix/<arch>/{ContextSwitch.s,"
        "FiberInit.cpp} -- an ABI's worth of work, not a compiler flag.")
endif()

# ---- sources ---------------------------------------------------------------------------------
# Shared translation units, plus EXACTLY ONE OS directory and EXACTLY ONE arch directory beneath
# it. Splitting by directory rather than by filename suffix is what makes these globs safe:
# suffixed variants would both be picked up and collide at link time.
#
# Linux/x86-64 and Linux/AArch64 share everything that is not the context switch -- sysfs topology,
# affinity, the mmap arena -- so src/posix/*.cpp is common and only src/posix/<arch>/ differs.
# win32/ stays flat: the MASM switch is x64-only, and Scheduler.vcxproj references these paths, so
# an arch level there would be churn for a dimension with one member.
file(GLOB JLIBSCHED_SRC CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# A leftover arch file directly under src/posix/ is not a harmless stray. Both it and the real
# arch directory's file define Fiber::Init, both land in libScheduler.a, and static-archive
# linking resolves that silently by taking whichever member comes first -- no duplicate-symbol
# error. That produced an AArch64 build seeding an x86-64 stack frame. Refuse to configure.
foreach(_stale ContextSwitch.s ContextSwitch.S FiberInit.cpp)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${_stale}")
        message(FATAL_ERROR
            "Stale src/posix/${_stale} found. Arch-specific sources moved into src/posix/<arch>/; "
            "this copy would be globbed in ALONGSIDE the correct one and silently win at link "
            "time. Delete it (a sync without --delete is the usual cause).")
    endif()
endforeach()

if(WIN32)
    file(GLOB JLIBSCHED_PLAT CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/*.cpp")
    set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/ContextSwitch.asm")
else()
    file(GLOB JLIBSCHED_PLAT CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${JLIBSCHED_ARCH}/*.cpp")
    # .S, not .s: the capital is what runs the file through the C preprocessor. The AArch64 switch
    # selects its Apple directives with #if defined(__APPLE__), and with a lowercase .s the
    # assembler would treat those lines as comments and assemble BOTH branches -- silently, since a
    # line-leading '#' is a GAS comment. Both arches use .S so the rule has no exceptions.
    set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${JLIBSCHED_ARCH}/ContextSwitch.S")
endif()

message(STATUS "JLib::Scheduler: ${CMAKE_SYSTEM_NAME} / ${JLIBSCHED_ARCH}")

add_library(Scheduler STATIC ${JLIBSCHED_SRC} ${JLIBSCHED_PLAT} ${JLIBSCHED_ASM})
add_library(JLib::Scheduler ALIAS Scheduler)

# BUILD_INTERFACE / INSTALL_INTERFACE rather than a bare path: a bare absolute source path gets
# baked into the exported package, pointing every consumer back at the tree it was built from.
target_include_directories(Scheduler PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_compile_features(Scheduler PUBLIC cxx_std_17)   # the README's compatibility claim, enforced

if(WIN32)
    # Both exist only to tame <windows.h>; defining them elsewhere collides with headers that
    # already #define NOMINMAX themselves.
    target_compile_definitions(Scheduler PUBLIC NOMINMAX WIN32_LEAN_AND_MEAN)
else()
    find_package(Threads REQUIRED)
    target_link_libraries(Scheduler PUBLIC Threads::Threads)
endif()

# ---- bench -----------------------------------------------------------------------------------
# C++20 even though the library is C++17: the crossover sweep accumulates into a
# std::atomic<double>, and atomic<floating-point>::fetch_add is C++20. That is a property of the
# harness, not of the library's compatibility claim.
add_executable(SchedulerBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/bench.cpp")
target_link_libraries(SchedulerBench PRIVATE Scheduler)
target_compile_features(SchedulerBench PRIVATE cxx_std_20)

# ---- install (standalone only) ---------------------------------------------------------------
# Inside the umbrella, JLib's own top-level file owns the export set and installs Scheduler as part
# of it; duplicating that here would produce two packages describing the same target.
if(JLIBSCHED_STANDALONE)
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    install(TARGETS Scheduler EXPORT JLibSchedulerTargets
            ARCHIVE DESTINATION "lib/$<CONFIG>"
            INCLUDES DESTINATION include)
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/" DESTINATION include)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE" DESTINATION .)
    install(EXPORT JLibSchedulerTargets
            FILE JLibSchedulerTargets.cmake NAMESPACE JLib::
            DESTINATION lib/cmake/JLibScheduler)

    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfigVersion.cmake"
        VERSION 0.1.0 COMPATIBILITY SameMajorVersion)
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake.in"
"@PACKAGE_INIT@\ninclude(CMakeFindDependencyMacro)\nif(NOT WIN32)\n  find_dependency(Threads)\nendif()\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/JLibSchedulerTargets.cmake\")\ncheck_required_components(JLibScheduler)\n")
    configure_package_config_file(
        "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake"
        INSTALL_DESTINATION lib/cmake/JLibScheduler)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake"
                  "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfigVersion.cmake"
            DESTINATION lib/cmake/JLibScheduler)
endif()
