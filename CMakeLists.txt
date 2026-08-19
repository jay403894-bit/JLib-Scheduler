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
# Supported: Windows on x64 AND ARM64 (MSVC), Linux/Android on x86-64 or AArch64 (GCC/Clang), and
# macOS on arm64 (AppleClang). Every one of those runs the full suite in CI. The context switch is
# hand-written assembly per ABI, so a new platform needs a new ContextSwitch, not a compiler flag --
# hence the two-axis source layout below. Note the axes are NOT fully orthogonal: a calling
# convention belongs to the (OS, ARCH) pair, which is why src/win32/aarch64/ exists alongside
# src/posix/aarch64/ even though both are AArch64.

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
set(JLIBSCHED_VERSION "1.6.0")

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
    #
    # AND ON WINDOWS THERE ARE TWO OF THEM. CMake's ASM_MASM is ml/ml64 -- x86 only. ARM64 assembly
    # goes through a SEPARATE language, ASM_MARMASM (armasm64), added in CMake 3.26. Enabling the
    # wrong one does not fall back gracefully: ml64 does not exist in an ARM64 toolchain, so
    # project() itself fails.
    #
    # The awkward part is that this decision has to be made BEFORE project() runs, and
    # CMAKE_SYSTEM_PROCESSOR does not exist until AFTER it. So the arch is taken from the generator
    # platform (-A ARM64, i.e. a cross build) or, when that is unset, from the host environment
    # (a native build on an ARM64 Windows machine -- what the CI runner does). The real arch
    # selection further down still uses CMAKE_SYSTEM_PROCESSOR; this is only about which assembler
    # to switch on, and the two must agree.
    if(WIN32)
        set(_jlibsched_win_arm OFF)
        if(CMAKE_GENERATOR_PLATFORM MATCHES "^(ARM64|arm64|aarch64)$")
            set(_jlibsched_win_arm ON)
        elseif(NOT CMAKE_GENERATOR_PLATFORM AND
               "$ENV{PROCESSOR_ARCHITECTURE}" MATCHES "^(ARM64|arm64|aarch64)$")
            set(_jlibsched_win_arm ON)
        endif()

        if(_jlibsched_win_arm)
            if(CMAKE_VERSION VERSION_LESS 3.26)
                message(FATAL_ERROR
                    "Windows on ARM64 needs CMake 3.26+ for the ASM_MARMASM language (armasm64). "
                    "Found ${CMAKE_VERSION}.")
            endif()
            project(JLibScheduler VERSION ${JLIBSCHED_VERSION} LANGUAGES CXX ASM_MARMASM)
        else()
            project(JLibScheduler VERSION ${JLIBSCHED_VERSION} LANGUAGES CXX ASM_MASM)
        endif()
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
# build would produce something BROKEN -- i.e. an arch with no context switch of its own, which no
# flag should let through. (Windows on ARM64 WAS the example here until 2026-08-17. It now has
# src/win32/aarch64/ContextSwitch.asm and a REQUIRED CI leg on a windows-11-arm runner, so it is
# neither refused nor unverified -- it is supported.) A platform is merely UNVERIFIED when every
# piece it
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
    # Windows on ARM64 (Snapdragon X) got its own switch in src/win32/aarch64/, so this is now an
    # arch SELECTION rather than the refusal it used to be.
    #
    # It needed a THIRD hand-written switch rather than an #ifdef over the Linux one, because a
    # calling convention belongs to the (OS, ARCH) PAIR and not to the arch. That is the one place
    # this library's OS-orthogonal-to-arch claim does not hold, and it is a documented caveat rather
    # than something to design around. The real difference turned out to be NARROW: MSVC's armasm64
    # syntax is unrelated to GAS, and Windows uses .pdata/.xdata instead of DWARF CFI. The register
    # set and frame layout are identical, which is why FiberInit is shared below.
    #
    # CORRECTION, 2026-08-17: this comment previously claimed a Windows fiber switch "must update
    # the TEB's StackBase/StackLimit -- the fixup the x64 MASM does". The x64 MASM does NOT do that;
    # there is no TEB access anywhere in src/win32/. The ARM64 port matches the shipped x64
    # behaviour instead of inventing a third one. See the header of src/win32/aarch64/ContextSwitch.asm
    # for what that costs (guard-page AV instead of a stack-overflow exception) and why it is the
    # normal trade for lightweight fibers.
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ARM64|arm64|aarch64)$" OR CMAKE_GENERATOR_PLATFORM MATCHES "^(ARM64|arm64)$")
        set(JLIBSCHED_ARCH "aarch64")
    else()
        set(JLIBSCHED_ARCH "x86_64")
    endif()
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
# win32/ is HALF split, and deliberately so. The x86-64 switch and FiberInit stay flat at
# src/win32/ because Scheduler.vcxproj hard-codes those paths and MSBuild is the build that actually
# ships; only the ARM64 switch got a src/win32/aarch64/ subdirectory. The asymmetry is ugly but
# contained -- see the REMOVE_ITEM in the WIN32 branch below, which is what keeps the flat x86-64
# FiberInit from being globbed into an ARM64 build alongside the AAPCS64 one.
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

    if(JLIBSCHED_ARCH STREQUAL "aarch64")
        # win32/ is NOT arch-split the way posix/ is: FiberInit.cpp and ContextSwitch.asm sit
        # directly in src/win32/ and are x86-64, because Scheduler.vcxproj hard-codes those paths
        # and moving them would break the MSBuild build for the one platform that actually ships.
        # So the ARM64 build subtracts rather than restructures.
        #
        # THIS REMOVE IS LOAD-BEARING, and it is the same hazard the src/posix/ stale-file guard
        # above exists for: src/win32/FiberInit.cpp and src/posix/aarch64/FiberInit.cpp BOTH define
        # Fiber::Init. Leave both in and they both land in Scheduler.lib, where static-archive
        # linking picks whichever member comes first WITHOUT a duplicate-symbol error -- an ARM64
        # build seeding an x86-64 stack frame, which would fault on the first switch-in with
        # nothing pointing at the cause.
        list(REMOVE_ITEM JLIBSCHED_PLAT "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/FiberInit.cpp")

        # Reused verbatim, not copied: Windows ARM64 and AAPCS64 agree on the callee-saved set
        # (x19-x28, x29, x30, low 64 bits of v8-v15) and therefore on the frame layout, so this file
        # is ABI-common and only the ASSEMBLER differs. A duplicate would be two files that must
        # stay in step forever -- exactly the trap the two-tree sync keeps producing.
        list(APPEND JLIBSCHED_PLAT "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/aarch64/FiberInit.cpp")

        set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/aarch64/ContextSwitch.asm")
    else()
        set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/ContextSwitch.asm")
    endif()
    # Pin the language explicitly rather than letting CMake infer it from the .asm extension.
    # Inference works right up until something ELSE in the same build tree enables the generic ASM
    # language -- a third-party add_subdirectory is enough -- at which point .asm becomes ambiguous,
    # CMake stops routing this file through MASM, and the whole library links with ContextSwitch and
    # FiberTrampoline undefined. Found when the marl comparison target was added: it broke every
    # other target in that tree, not just its own, and the error names our symbols rather than
    # anything to do with marl.
    # ...and the language must match the one enabled at project() time: ASM_MARMASM (armasm64) on
    # ARM64, ASM_MASM (ml64) on x64. Pinning the wrong one here is not a soft failure -- ml64 cannot
    # assemble AArch64 mnemonics and armasm64 cannot assemble x86.
    if(JLIBSCHED_ARCH STREQUAL "aarch64")
        set_source_files_properties("${JLIBSCHED_ASM}" PROPERTIES LANGUAGE ASM_MARMASM)
    else()
        set_source_files_properties("${JLIBSCHED_ASM}" PROPERTIES LANGUAGE ASM_MASM)
    endif()
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

# SetFiberBudget: does StartPool actually consume the configured per-worker counts, not just the
# setter storing them. A standalone binary, not a case in primitives_test.cpp, because
# TestFiberCapOversubscribed there hardcodes 4*64=256 into its own logic -- changing the
# process-wide default anywhere in that shared binary would silently invalidate it.
add_executable(SchedulerFiberBudgetTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_budget_test.cpp")
target_link_libraries(SchedulerFiberBudgetTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberBudgetTest PRIVATE cxx_std_17)

# SetTaskSlabSize: does construction actually consume the configured slot count, and does the
# allocator genuinely exhaust at it rather than the number being cosmetic. Standalone for the same
# reason as SchedulerFiberBudgetTest -- primitives_test.cpp creates tasks liberally and was never
# written expecting a slab of a few dozen slots.
add_executable(SchedulerTaskSlabSizeTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/task_slab_size_test.cpp")
target_link_libraries(SchedulerTaskSlabSizeTest PRIVATE Scheduler)
target_compile_features(SchedulerTaskSlabSizeTest PRIVATE cxx_std_17)

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

# ABI harness for the Windows/ARM64 context switch -- the Windows counterpart of
# tests/fibertest_aarch64.cpp, which the POSIX ABI job builds directly with `c++` and cannot be
# reused here (see that file's header: MSVC has no inline assembly on ARM64 at all).
#
# It is a CMake TARGET rather than a hand-rolled compile line because it needs armasm64 for its
# probe file, and the ASM_MARMASM language is already enabled for this configuration. Reproducing
# that by hand in a CI step would mean re-deriving the assembler invocation.
#
# Links against Scheduler for ContextSwitch/FiberTrampoline only; it starts no pool and includes no
# scheduler header, so a failure here is the assembly's fault and nothing else's -- which is the
# entire value of it over inferring a bad switch from a primitives-suite crash.
if(WIN32 AND JLIBSCHED_ARCH STREQUAL "aarch64")
    add_executable(SchedulerFiberAbiTest
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/fibertest_win_aarch64.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/win32/fibertest_probe_aarch64.asm")
    set_source_files_properties(
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/win32/fibertest_probe_aarch64.asm"
        PROPERTIES LANGUAGE ASM_MARMASM)
    target_link_libraries(SchedulerFiberAbiTest PRIVATE Scheduler)
    target_compile_features(SchedulerFiberAbiTest PRIVATE cxx_std_17)
endif()

# ---- install (standalone only) ---------------------------------------------------------------
# Inside the umbrella, JLib's own top-level file owns the export set and installs Scheduler as part
# of it; duplicating that here would produce two packages describing the same target.
if(JLIBSCHED_STANDALONE)
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    # Headers go in a SUBDIRECTORY, and the exported target carries the include path so that
    # `#include <TaskScheduler.h>` keeps working unchanged. Flat was untenable for a system install:
    # this library ships Task.h, Thread.h, Event.h, Fiber.h, Context.h, Topology.h and platform.h,
    # plus moodycamel's concurrentqueue.h -- dropping those into /usr/include is hostile to whatever
    # else is on the machine, and the moodycamel ones would fight any other library vendoring them.
    #
    # Costs consumers NOTHING: INCLUDES DESTINATION below puts <prefix>/include/JLibScheduler on the
    # imported target's INTERFACE_INCLUDE_DIRECTORIES, so find_package + target_link_libraries gives
    # the same short include paths as before, and the internal #include "concurrentqueue.h" still
    # resolves. Only someone hand-writing -I<prefix>/include instead of using the package breaks --
    # and they were already colliding with everything else there.
    set(JLIBSCHED_INCDEST "${CMAKE_INSTALL_INCLUDEDIR}/JLibScheduler")

    # $<CONFIG> ONLY for multi-config generators. It expands on single-config ones too, which is how
    # a Linux install ended up putting the archive in lib/Release/ -- wrong for any distro layout.
    # Windows genuinely wants lib/Debug and lib/Release side by side, so keep it there.
    if(CMAKE_CONFIGURATION_TYPES)
        set(JLIBSCHED_LIBDEST "${CMAKE_INSTALL_LIBDIR}/$<CONFIG>")
    else()
        set(JLIBSCHED_LIBDEST "${CMAKE_INSTALL_LIBDIR}")
    endif()

    install(TARGETS Scheduler EXPORT JLibSchedulerTargets
            ARCHIVE DESTINATION "${JLIBSCHED_LIBDEST}"
            INCLUDES DESTINATION "${JLIBSCHED_INCDEST}")
    # FILES_MATCHING *.h: include/ also holds moodycamel's LICENSE.md and README.md, which were
    # being installed into the include directory as if they were headers.
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/"
            DESTINATION "${JLIBSCHED_INCDEST}"
            FILES_MATCHING PATTERN "*.h")
    # share/doc/JLibScheduler/, not the prefix ROOT -- this used to land as a bare /usr/LICENSE.
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE" DESTINATION "${CMAKE_INSTALL_DOCDIR}")
    install(EXPORT JLibSchedulerTargets
            FILE JLibSchedulerTargets.cmake NAMESPACE JLib::
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/JLibScheduler")

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
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/JLibScheduler")
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake"
                  "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfigVersion.cmake"
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/JLibScheduler")
endif()
