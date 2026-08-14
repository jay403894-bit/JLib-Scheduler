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

# ONE place the version lives. Used by project(), by the installed package config, and stamped into
# the benchmark's output -- that last one because third-party results are only useful if you can
# tell which build produced them. Runs came back on an older bench once and had to be discarded:
# the ParallelFor case, the default affinity policy and the crossover reporting had all changed
# underneath them, and nothing in the pasted output said so.
#
# BUMP THIS IN THE SAME COMMIT that changes anything the bench measures or the API exposes, rather
# than waiting for release day. The number's job is to tell two trees apart, and it cannot do that
# while a modified tree still reports the last tag. CMake's project() rejects a "-dev" suffix, so
# there is no pre-release form to hide behind: a tree ahead of a tag simply carries the next number.
set(JLIBSCHED_VERSION "1.3.3")

# Steal-path instrumentation. OFF by design: the counters live in the steal loop, which is precisely
# where contention is being investigated, so leaving them in a shipping build would tax the thing
# they exist to measure. Turn on only for an experiment, and do not compare its numbers against a
# normal build's.
option(JLIBSCHED_STEAL_STATS "Count steal probes and hits (diagnostic; perturbs the steal path)" OFF)

# AArch64 spin hint. Default is isb, which is NOT the architectural analogue of x86 PAUSE -- that is
# YIELD, and YIELD retires as a NOP on non-SMT cores, which is nearly all of them, so it gives a spin
# loop no backoff at all. ISB flushes the pipeline and therefore costs real cycles. Measured on an
# Android AArch64 device: throughput/mp 3.43 -> 5.15 M tasks/sec, ~50%, reproduced in both run orders
# on a cool device. Set to yield to restore the old behaviour, or nop for a fixed sled.
# Ignored on x86-64, which always uses PAUSE.
set(JLIBSCHED_ARM_SPIN_HINT "isb" CACHE STRING "AArch64 spin hint: isb, yield or nop")
set_property(CACHE JLIBSCHED_ARM_SPIN_HINT PROPERTY STRINGS yield isb nop)

set(JLIBSCHED_STANDALONE OFF)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(JLIBSCHED_STANDALONE ON)
endif()

if(JLIBSCHED_STANDALONE)
    # MASM only exists on Windows; the Linux context switch is GAS, which CMake treats as ASM.
    # The language has to be enabled at project() time, which is why this is not simply a
    # per-target property.
    if(WIN32)
        project(JLibScheduler VERSION ${JLIBSCHED_VERSION} LANGUAGES CXX ASM_MASM)
    else()
        project(JLibScheduler VERSION ${JLIBSCHED_VERSION} LANGUAGES CXX ASM)
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
# Opt-in escape hatch for a platform that SHOULD work but has never been run.
#
# Only two categories exist here, and the difference matters. A platform is refused outright when a
# build would produce something BROKEN -- Windows on ARM64 would hand the x64 MASM switch to an ARM
# target, and no flag should let that through. A platform is merely UNVERIFIED when every piece it
# needs already exists and is tested elsewhere, and the only thing missing is that nobody has run
# it. iOS is the motivating case: same AAPCS64 context switch as macOS arm64, same src/darwin/ OS
# layer, same sysctl topology, same 16K pages -- all of it verified on macos-14 in CI. What is
# absent is a device run, and the maintainer has no Mac to produce one.
#
# So this flag exists to let someone who CAN test it do so, without the project claiming support it
# cannot stand behind. It is off by default, it warns loudly when on, and the README does not list
# these platforms. If you turn it on and it works, please say so in an issue; if it does not, a PR
# is very welcome -- you will have hardware nobody here does.
option(JLIBSCHED_ALLOW_UNVERIFIED_PLATFORM
       "Attempt a build on a platform that is expected to work but has never been run (e.g. iOS)" OFF)

set(JLIBSCHED_VERIFIED_OS "^(Linux|Android|Darwin)$")
# Expected to work, never run. Apple's non-macOS platforms are all arm64/Darwin/Mach-O, which is
# exactly the configuration macos-14 already verifies.
set(JLIBSCHED_UNVERIFIED_OS "^(iOS|tvOS|watchOS|visionOS)$")

if(NOT WIN32 AND NOT CMAKE_SYSTEM_NAME MATCHES "${JLIBSCHED_VERIFIED_OS}")
    if(CMAKE_SYSTEM_NAME MATCHES "${JLIBSCHED_UNVERIFIED_OS}" AND JLIBSCHED_ALLOW_UNVERIFIED_PLATFORM)
        message(WARNING
            "Building for ${CMAKE_SYSTEM_NAME}, which is UNVERIFIED. Every component it needs is "
            "tested on macOS arm64 in CI -- the same AAPCS64 context switch and the same src/darwin/ "
            "OS layer -- but nobody has ever run the result on this platform. Do not treat a "
            "successful build as support. Note also that placement is a no-op on Apple platforms, "
            "that an iOS executable must be wrapped in an app bundle before it will run at all, and "
            "that a pool of persistent worker threads fits the iOS app lifecycle poorly -- "
            "backgrounding freezes them mid-execution. Pause()/Resume() are the hook; wiring them "
            "to the lifecycle is the caller's job. Foreground-only unless you do that work.")
    elseif(CMAKE_SYSTEM_NAME MATCHES "${JLIBSCHED_UNVERIFIED_OS}")
        message(FATAL_ERROR
            "${CMAKE_SYSTEM_NAME} is not a supported target, but it is expected to work: it shares "
            "the AAPCS64 context switch and the src/darwin/ OS layer with macOS arm64, which IS "
            "verified in CI. Nobody has run it, so it is not claimed. Configure with "
            "-DJLIBSCHED_ALLOW_UNVERIFIED_PLATFORM=ON to try it, and please report what happens.")
    else()
        message(FATAL_ERROR
            "JLib::Scheduler supports Windows, Linux, Android and macOS. The context switch is "
            "hand-written assembly per ABI, so a new platform needs a new ContextSwitch, not a "
            "compiler flag.")
    endif()
endif()

# Normalise the many spellings a toolchain may use for the same instruction set.
if(WIN32)
    # Windows is x64-only, and this refuses ARM64 rather than quietly handing the x64 MASM file to
    # an ARM64 build (which fails deep inside ml64 with nothing pointing at the real cause).
    #
    # Windows-on-ARM is NOT "the Linux AAPCS64 switch with a different #ifdef". It needs a THIRD
    # hand-written switch, because (1) MSVC's ARM64 assembler is armasm64, whose syntax is unrelated
    # to the GAS syntax src/posix/aarch64/ContextSwitch.S is written in, and (2) a Windows fiber
    # switch must update the TEB's StackBase/StackLimit -- the fixup the x64 MASM does and the
    # Linux/macOS AAPCS64 switch deliberately omits because it has no equivalent. x18 is also
    # reserved as the TEB pointer there (the current switch happens not to touch it).
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ARM64|arm64|aarch64)$" OR CMAKE_GENERATOR_PLATFORM MATCHES "^(ARM64|arm64)$")
        message(FATAL_ERROR
            "Windows on ARM64 is not supported: the context switch is MASM for x64 only. It needs a "
            "third ContextSwitch (armasm64 syntax, plus the TEB stack-bounds fixup), not a compiler "
            "flag. Linux and macOS on AArch64 ARE supported -- see src/posix/aarch64/.")
    endif()
    set(JLIBSCHED_ARCH "x86_64")   # the MASM switch is x64-only; the MSVC guard above is the gate
elseif(APPLE AND CMAKE_OSX_ARCHITECTURES)
    # Apple cross-builds (iOS, or an explicit -DCMAKE_OSX_ARCHITECTURES on macOS) leave
    # CMAKE_SYSTEM_PROCESSOR as the HOST's or empty, so the target arch has to come from here
    # instead. Checked before the generic cases below, which would otherwise read the wrong value.
    if(CMAKE_OSX_ARCHITECTURES MATCHES "arm64")
        set(JLIBSCHED_ARCH "aarch64")
    elseif(CMAKE_OSX_ARCHITECTURES MATCHES "x86_64")
        set(JLIBSCHED_ARCH "x86_64")
    else()
        message(FATAL_ERROR
            "CMAKE_OSX_ARCHITECTURES='${CMAKE_OSX_ARCHITECTURES}' names no architecture this "
            "project has a context switch for. Use arm64 or x86_64, one at a time -- a universal "
            "binary would need both switches in one build, which the one-directory-per-target "
            "source layout deliberately does not allow.")
    endif()
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
    # Pin the language explicitly rather than letting CMake infer it from the .asm extension.
    # Inference works right up until something ELSE in the same build tree enables the generic ASM
    # language -- a third-party add_subdirectory is enough -- at which point .asm becomes ambiguous,
    # CMake stops routing this file through MASM, and the whole library links with ContextSwitch and
    # FiberTrampoline undefined. Found when the marl comparison target was added: it broke every
    # other target in that tree, not just its own, and the error names our symbols rather than
    # anything to do with marl.
    set_source_files_properties("${JLIBSCHED_ASM}" PROPERTIES LANGUAGE ASM_MASM)
else()
    # OS layer: src/posix/ (Linux, Android) or src/darwin/ (macOS). They diverge wholesale --
    # sysfs text versus sysctl, and macOS has no thread-affinity API at all -- so they are separate
    # directories, not #ifdefs.
    #
    # The ARCH layer under src/posix/<arch>/ is shared by BOTH, because the ABI is a property of the
    # instruction set and not of the kernel: Apple arm64 is AAPCS64, the same switch Linux/AArch64
    # uses, and ContextSwitch.S carries the handful of Mach-O directive differences behind
    # #if defined(__APPLE__). (That guard only works because the file is .S -- see below.)
    if(APPLE)
        set(JLIBSCHED_OSDIR "darwin")
    else()
        set(JLIBSCHED_OSDIR "posix")
    endif()
    file(GLOB JLIBSCHED_PLAT CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/${JLIBSCHED_OSDIR}/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${JLIBSCHED_ARCH}/*.cpp")
    # .S, not .s: the capital is what runs the file through the C preprocessor. The AArch64 switch
    # selects its Apple directives with #if defined(__APPLE__), and with a lowercase .s the
    # assembler would treat those lines as comments and assemble BOTH branches -- silently, since a
    # line-leading '#' is a GAS comment. Both arches use .S so the rule has no exceptions.
    set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${JLIBSCHED_ARCH}/ContextSwitch.S")
endif()

if(WIN32)
    message(STATUS "JLib::Scheduler: ${CMAKE_SYSTEM_NAME} / ${JLIBSCHED_ARCH} (os layer: win32)")
else()
    message(STATUS "JLib::Scheduler: ${CMAKE_SYSTEM_NAME} / ${JLIBSCHED_ARCH} (os layer: ${JLIBSCHED_OSDIR})")
endif()

add_library(Scheduler STATIC ${JLIBSCHED_SRC} ${JLIBSCHED_PLAT} ${JLIBSCHED_ASM})
add_library(JLib::Scheduler ALIAS Scheduler)

# BUILD_INTERFACE / INSTALL_INTERFACE rather than a bare path: a bare absolute source path gets
# baked into the exported package, pointing every consumer back at the tree it was built from.
# PUBLIC, not PRIVATE: the counters are inline in Thread.h, so the library and anything reading them
# (the bench) must agree on whether they exist. Compiling half the program with them and half without
# would give two different Thread.h and a silently wrong answer.
if(JLIBSCHED_STEAL_STATS)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_STEAL_STATS=1)
    message(STATUS "JLib::Scheduler: steal-path instrumentation ON (diagnostic build, not for perf numbers)")
endif()

# PUBLIC because CpuRelax() is inline in a header -- every translation unit that spins must agree,
# or the library and its caller compile different spin loops and the measurement is meaningless.
if(JLIBSCHED_ARM_SPIN_HINT STREQUAL "yield")
    target_compile_definitions(Scheduler PUBLIC JLIB_SPIN_HINT_YIELD=1)
    message(STATUS "JLib::Scheduler: AArch64 spin hint = yield (measured ~50%% slower on the contended path than the isb default)")
elseif(JLIBSCHED_ARM_SPIN_HINT STREQUAL "nop")
    target_compile_definitions(Scheduler PUBLIC JLIB_SPIN_HINT_NOP=1)
    message(STATUS "JLib::Scheduler: AArch64 spin hint = nop sled (control; default is yield)")
elseif(NOT JLIBSCHED_ARM_SPIN_HINT STREQUAL "isb")
    message(FATAL_ERROR "JLIBSCHED_ARM_SPIN_HINT must be isb, yield or nop (got: ${JLIBSCHED_ARM_SPIN_HINT})")
endif()

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
# C++17, same as the library. This used to be C++20 solely because the crossover sweep accumulated
# into a std::atomic<double> via fetch_add, which is C++20 (P0020R6). AppleClang's libc++ does not
# ship that overload, so the macOS port replaced it with a compare-exchange loop -- which is what
# fetch_add lowers to anyway -- and the whole C++20 requirement went with it. There is now no
# caveat to attach to the project's C++17 claim.
add_executable(SchedulerBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/bench.cpp")
target_link_libraries(SchedulerBench PRIVATE Scheduler)
target_compile_features(SchedulerBench PRIVATE cxx_std_17)
# Stamped into the header line so a pasted result identifies the build that produced it.
target_compile_definitions(SchedulerBench PRIVATE JLIBSCHED_VERSION="${JLIBSCHED_VERSION}")

# ---- tests ------------------------------------------------------------------------------------
# A CMake target rather than a hand-compiled file, specifically so CI can run it everywhere without
# per-platform compile commands. That matters more than convenience here: this covers the named
# EVENT path (lock-free waiter stack), which the benchmark never touches, and the thing most worth
# knowing about new atomic code is whether it holds on a WEAKLY ORDERED machine. x86 is TSO and
# hides ordering mistakes; the ARM64 runners do not.
#
# The AArch64 ABI harness (tests/fibertest_aarch64.cpp) stays hand-built -- it is arch-specific and
# links the context switch directly rather than the library.
add_executable(SchedulerEventSmoke "${CMAKE_CURRENT_SOURCE_DIR}/tests/event_smoke.cpp")
target_link_libraries(SchedulerEventSmoke PRIVATE Scheduler)
target_compile_features(SchedulerEventSmoke PRIVATE cxx_std_17)

# Pure-logic tests for the >64-CPU path: mask math, the flat-id split, and the worker-to-CPU
# assignment on unevenly sized processor groups. Runs anywhere, which is the point -- it is the only
# check on the wide path that does not require hardware nobody here has.
add_executable(SchedulerTopologyTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/topology_test.cpp")
target_link_libraries(SchedulerTopologyTest PRIVATE Scheduler)
target_compile_features(SchedulerTopologyTest PRIVATE cxx_std_17)

# The fiber-aware locks. Least-tested code in the library and the most recently changed, including a
# regression test for the 1.2.3 self-deadlock. Every blocking case runs under a watchdog, so a
# regression fails in seconds instead of hanging CI for the whole job timeout.
# Third-party comparison harness. Adds nothing unless -DJLIBSCHED_ENKITS_DIR is passed, so a normal
# build and CI never see it -- see bench/compare/CMakeLists.txt for why it is opt-in.
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/bench/compare")

add_executable(SchedulerPrimitivesTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/primitives_test.cpp")
target_link_libraries(SchedulerPrimitivesTest PRIVATE Scheduler)
target_compile_features(SchedulerPrimitivesTest PRIVATE cxx_std_17)

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
        # SameMajorVersion: a consumer writing find_package(JLibScheduler 1.0) accepts any 1.x,
        # which is the semver promise this version number is making -- no breaking API change
        # without a 2.0.
        VERSION ${JLIBSCHED_VERSION} COMPATIBILITY SameMajorVersion)
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
