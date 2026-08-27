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
# 4.0.1 -- BETA. Not a patch bump in spirit: the 4.0 line has gained lane wakes, a
# fourth sleep-predicate input, and dynamic K, and dynamic K BREAKS AN INVARIANT the
# pool used to hold. PushImmediate's contract ("this core is mine until my task
# returns") was enforced by a point-in-time check that a moving hot set can grow past.
# Guarded for now; the design decision is open. See the note in MaybeAdjustHotWorkers.
set(JLIBSCHED_VERSION "4.0.1")

# Steal-path instrumentation. OFF by design: the counters live in the steal loop, which is precisely
# where contention is being investigated, so leaving them in a shipping build would tax the thing
# they exist to measure. Turn on only for an experiment, and do not compare its numbers against a
# normal build's.
option(JLIBSCHED_STEAL_STATS "Count steal probes and hits (diagnostic; perturbs the steal path)" OFF)
option(JLIBSCHED_HOT_OCCUPANCY_STATS "Witness whether an idle hot worker has a backlogged sibling (diagnostic; scans hot deques)" OFF)
option(JLIBSCHED_LANE_WAKE_STATS "Count lane-hint edges and the wakes they send (diagnostic; two relaxed counters)" OFF)

# Latency breakdown: where a push->run->wait round trip actually spends its time -- OS wake vs
# Worker()'s own loop order (local deque + steal scan run before the inbox a cold wake was FOR).
# Same OFF-by-default rule as JLIBSCHED_STEAL_STATS and for the same reason: a clock read on every
# wake is not free, and this is instrumenting a ~us-scale measurement.
option(JLIBSCHED_LATENCY_STATS "Record wake/preSteal/found timestamps (diagnostic; perturbs latency)" OFF)

# Bare-thread contended-lock fast spin: how many plain CpuRelax retries SchedulerMutex::Lock and
# SchedulerSemaphore::Wait take before escalating to the steal-and-help path (ContendedSpinStep).
#
# 0 IS THE MEASURED OPTIMUM AND THE ONLY VALUE THAT SHOULD SHIP. It is not a tuning opportunity: the
# 2026-08-23 sweep found the metric monotonic in this number, with 0 winning lock latency, lock
# throughput and pool throughput simultaneously at both 8 and 31 workers. It stays configurable only
# so that result can be re-derived rather than taken on trust. See the block comment on
# kFastSpinTries in src/TaskScheduler.cpp and the RESULTS in bench/lock_contention.cpp.
set(JLIBSCHED_FAST_SPIN_TRIES "0" CACHE STRING "Plain-spin retries before a contended bare-thread lock starts helping (0 = measured optimum)")

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
endif()
if(JLIBSCHED_STEAL_STATS)
    message(STATUS "JLib::Scheduler: steal-path instrumentation ON (diagnostic build, not for perf numbers)")
endif()
if(JLIBSCHED_HOT_OCCUPANCY_STATS)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_HOT_OCCUPANCY_STATS=1)
    message(STATUS "JLib::Scheduler: hot-occupancy witness ON (diagnostic build, not for perf numbers)")
endif()
if(JLIBSCHED_LANE_WAKE_STATS)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_LANE_WAKE_STATS=1)
    message(STATUS "JLib::Scheduler: lane-wake rate counters ON (two relaxed counters; cost side of SetLaneWake)")
endif()

if(JLIBSCHED_LATENCY_STATS)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_LATENCY_STATS=1)
    message(STATUS "JLib::Scheduler: latency breakdown instrumentation ON (diagnostic build, not for perf numbers)")
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

# PRIVATE, unlike the two above: kFastSpinTries lives in an anonymous namespace in
# src/TaskScheduler.cpp, not in a header, so no consumer translation unit can disagree with the
# library about it and nothing outside needs to see it. (bench/lock_contention.cpp gets it passed
# separately, purely so it can stamp the value it was built against into its own output.)
if(NOT JLIBSCHED_FAST_SPIN_TRIES MATCHES "^[0-9]+$")
    message(FATAL_ERROR "JLIBSCHED_FAST_SPIN_TRIES must be a non-negative integer (got: ${JLIBSCHED_FAST_SPIN_TRIES})")
endif()
target_compile_definitions(Scheduler PRIVATE JLIBSCHED_FAST_SPIN_TRIES=${JLIBSCHED_FAST_SPIN_TRIES})

# Makes that constant settable at RUNTIME, for bench/lock_contention.cpp only. OFF by design and for
# the same reason as the two instrumentation options above: a shipping build folds the constant into
# the spin loop, and a diagnostic build must never be mistaken for one. PUBLIC rather than PRIVATE
# because it gates a DECLARATION in TaskScheduler.h -- the library and the bench have to agree on
# whether JLib::detail::SetFastSpinTries exists at all, and disagreeing is a link error at best.
option(JLIBSCHED_TUNABLE_FAST_SPIN "Expose the fast-spin bound as a runtime setting (diagnostic; for the lock bench)" OFF)
if(JLIBSCHED_TUNABLE_FAST_SPIN)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_TUNABLE_FAST_SPIN=1)
    message(STATUS "JLib::Scheduler: fast-spin bound is RUNTIME-TUNABLE (diagnostic build, not for shipping)")
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

# Measures what an IDLE NoSleep pool costs the main thread, and how much of that the steal hint
# removes. Both arms run inside one process against one pool -- see the file header.
add_executable(SchedulerEpochVsMutex "${CMAKE_CURRENT_SOURCE_DIR}/bench/epoch_vs_mutex.cpp")
target_link_libraries(SchedulerEpochVsMutex PRIVATE Scheduler)
target_compile_features(SchedulerEpochVsMutex PRIVATE cxx_std_17)

add_executable(SchedulerEpochMechBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/epoch_mechanisms.cpp")
target_link_libraries(SchedulerEpochMechBench PRIVATE Scheduler)
target_compile_features(SchedulerEpochMechBench PRIVATE cxx_std_17)

add_executable(SchedulerNoSleepTax "${CMAKE_CURRENT_SOURCE_DIR}/bench/nosleep_tax.cpp")
target_link_libraries(SchedulerNoSleepTax PRIVATE Scheduler)
target_compile_features(SchedulerNoSleepTax PRIVATE cxx_std_17)

# The lock primitives had no benchmark at all until 2.7.0 -- bench.cpp covers push/steal/ParallelFor
# and never takes a SchedulerMutex. This one measures them, and it is what established that the
# bare-thread fast spin had to be 0.
#
# ONLY BUILT IN A TUNABLE BUILD, and it must stay that way: the source #errors without
# JLIBSCHED_TUNABLE_FAST_SPIN because without a runtime-settable bound its arms would all be the
# same code, and a benchmark that silently compares a value against itself is worse than no
# benchmark. Adding it unconditionally would also break ALL_BUILD -- and therefore CI -- in every
# normal configuration.
if(JLIBSCHED_TUNABLE_FAST_SPIN)
    add_executable(SchedulerLockBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/lock_contention.cpp")
    target_link_libraries(SchedulerLockBench PRIVATE Scheduler)
    target_compile_features(SchedulerLockBench PRIVATE cxx_std_17)
endif()

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

# Worker()'s immediate/fork inbox drain moved from a per-task Requeue() loop to a single
# PushBatch() call in 2.2.0. PushImmediate() had zero test coverage before this -- standalone for
# the same isolation reason as the two tests above, and because it needs an exact 2-worker pool to
# make the drain deterministic, which a shared-binary test can't assume.
# Pure-logic tests for the >64-CPU path: mask math, the flat-id split, and the worker-to-CPU
# assignment on unevenly sized processor groups. Runs anywhere, which is the point -- it is the only
# check on the wide path that does not require hardware nobody here has.
# A compile guard, not a behavioural test: it DEFINES the Windows macros rather than including
# <windows.h>, so it stays inside the convention that windows.h lives only behind platform.h --
# and it runs on every platform, so a Linux leg catches a Windows-only break first.
# 3.0.0 shipped a member named `small`, which rpcndr.h defines as `char`, and this repository
# had no translation unit that could see it. See tests/windows_header_guard.cpp.
add_executable(SchedulerWindowsHeaderGuard "${CMAKE_CURRENT_SOURCE_DIR}/tests/windows_header_guard.cpp")
target_link_libraries(SchedulerWindowsHeaderGuard PRIVATE Scheduler)
target_compile_features(SchedulerWindowsHeaderGuard PRIVATE cxx_std_17)


add_executable(SchedulerTopologyTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/topology_test.cpp")
target_link_libraries(SchedulerTopologyTest PRIVATE Scheduler)
target_compile_features(SchedulerTopologyTest PRIVATE cxx_std_17)

# The fiber-aware locks. Least-tested code in the library and the most recently changed, including a
# regression test for the 1.2.3 self-deadlock. Every blocking case runs under a watchdog, so a
# regression fails in seconds instead of hanging CI for the whole job timeout.
# Third-party comparison harness. Adds nothing unless -DJLIBSCHED_ENKITS_DIR is passed, so a normal
# build and CI never see it -- see bench/compare/CMakeLists.txt for why it is opt-in.
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/bench/compare")

add_executable(SchedulerDagCancelTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/dag_cancel_test.cpp")
target_link_libraries(SchedulerDagCancelTest PRIVATE Scheduler)
target_compile_features(SchedulerDagCancelTest PRIVATE cxx_std_17)

# WaitFor(wg, token): cancelling a WAIT, not the work. Mostly a test of what must NOT change --
# the count is untouched, the abandoned tasks still finish, and an uncancellable waiter on the
# same group is not collateral.
add_executable(SchedulerWaitGroupCancelTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/waitgroup_cancel_test.cpp")
target_link_libraries(SchedulerWaitGroupCancelTest PRIVATE Scheduler)
target_compile_features(SchedulerWaitGroupCancelTest PRIVATE cxx_std_17)

add_executable(SchedulerTimerTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/timer_test.cpp")
target_link_libraries(SchedulerTimerTest PRIVATE Scheduler)
target_compile_features(SchedulerTimerTest PRIVATE cxx_std_17)

add_executable(SchedulerDynamicKTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/dynamic_k_test.cpp")
target_link_libraries(SchedulerDynamicKTest PRIVATE Scheduler)
target_compile_features(SchedulerDynamicKTest PRIVATE cxx_std_17)

add_executable(SchedulerReservedCoresTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/reserved_cores_test.cpp")
target_link_libraries(SchedulerReservedCoresTest PRIVATE Scheduler)
target_compile_features(SchedulerReservedCoresTest PRIVATE cxx_std_17)

if(WIN32)
    add_executable(SchedulerHotPriorityTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/hot_priority_test.cpp")
    target_link_libraries(SchedulerHotPriorityTest PRIVATE Scheduler)
    target_compile_features(SchedulerHotPriorityTest PRIVATE cxx_std_17)
endif()

add_executable(SchedulerDagBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/dag_scaling.cpp")
target_link_libraries(SchedulerDagBench PRIVATE Scheduler)
target_compile_features(SchedulerDagBench PRIVATE cxx_std_17)

add_executable(SchedulerDagExternalTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/dag_external_test.cpp")
target_link_libraries(SchedulerDagExternalTest PRIVATE Scheduler)
target_compile_features(SchedulerDagExternalTest PRIVATE cxx_std_17)

add_executable(SchedulerPrimitivesTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/primitives_test.cpp")
target_link_libraries(SchedulerPrimitivesTest PRIVATE Scheduler)
target_compile_features(SchedulerPrimitivesTest PRIVATE cxx_std_17)

# The C++20 coroutine mode. Compiled at cxx_std_20 while the Scheduler target stays cxx_std_17 --
# which is the point, and is checked by the build rather than by discipline: if anything from
# Coroutine.h ever leaks into the core, the library target fails to compile at C++17.
#
# OFF by default. Not because coroutine support is experimental, but because requiring C++20 to
# build the test suite would quietly make C++20 the project's real floor, and a C++17 job system
# this complete is worth keeping buildable on C++17.
option(JLIBSCHED_COROUTINES "Build the C++20 coroutine mode's tests (the header itself is always shipped)" OFF)
if(JLIBSCHED_COROUTINES)
    add_executable(SchedulerCoroutineTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/coroutine_test.cpp")
    target_link_libraries(SchedulerCoroutineTest PRIVATE Scheduler)
    target_compile_features(SchedulerCoroutineTest PRIVATE cxx_std_20)

    add_executable(SchedulerFutureTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/future_test.cpp")
    target_link_libraries(SchedulerFutureTest PRIVATE Scheduler)
    target_compile_features(SchedulerFutureTest PRIVATE cxx_std_20)

    add_executable(SchedulerCoroEpochTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/coro_epoch_test.cpp")
    target_link_libraries(SchedulerCoroEpochTest PRIVATE Scheduler)
    target_compile_features(SchedulerCoroEpochTest PRIVATE cxx_std_20)
    message(STATUS "JLib::Scheduler: C++20 coroutine tests ENABLED (core still builds as C++17)")

    add_executable(SchedulerCoroBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/coroutine_bench.cpp")
    target_link_libraries(SchedulerCoroBench PRIVATE Scheduler)
    target_compile_features(SchedulerCoroBench PRIVATE cxx_std_20)

    add_executable(SchedulerFrameClassBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/frame_class.cpp")
    target_link_libraries(SchedulerFrameClassBench PRIVATE Scheduler)
    target_compile_features(SchedulerFrameClassBench PRIVATE cxx_std_20)
endif()

# Records a histogram of real coroutine FRAME SIZES and live/peak counts. Exists to answer one
# question with data instead of intuition: is pooling coroutine frames worth it, and what slot size
# would cover them? Both inputs are chosen by the compiler from whichever locals live across suspend
# points, so neither can be read off the source. OFF by default, same discipline as
# JLIBSCHED_STEAL_STATS -- it adds a counter to every frame allocation, so timings taken with it on
# are inflated and must not be compared against timings taken with it off.
#
# INTERFACE, not PRIVATE: it changes code in Coroutine.h, which the library never includes and
# consumers do. Putting it on the library's own compile line would do nothing at all.
option(JLIBSCHED_TASK_STATS "Record a task-size histogram at CreateTask (diagnostic; run it on a real app)" OFF)
if(JLIBSCHED_TASK_STATS)
    # PUBLIC: CreateTask's function-pointer overload lives in the library's own TaskScheduler.cpp,
    # so an INTERFACE-only define would count the template half and silently miss the rest.
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_TASK_STATS=1)
    message(STATUS "JLib::Scheduler: task-size histogram ENABLED (diagnostic)")
endif()

option(JLIBSCHED_ALLOC_STATS "Count slab refill/flush and shared-tier lock contention (diagnostic)" OFF)
if(JLIBSCHED_ALLOC_STATS)
    # PUBLIC, not INTERFACE. The library's OWN sources allocate and free (Thread.cpp returns every
    # finished task to the slab), so an INTERFACE-only define counts the consumer half and silently
    # misses the rest -- which is exactly what happened: allocs 292,565, frees 0.
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_ALLOC_STATS=1)
    message(STATUS "JLib::Scheduler: allocator contention stats ENABLED (diagnostic)")
endif()

option(JLIBSCHED_CORO_STATS "Record coroutine frame-size histogram (diagnostic; see bench/coroutine_bench.cpp)" OFF)
if(JLIBSCHED_CORO_STATS)
    target_compile_definitions(Scheduler INTERFACE JLIBSCHED_CORO_STATS=1)
    message(STATUS "JLib::Scheduler: coroutine frame-size instrumentation ON (diagnostic build)")
endif()

# NOTE: there is deliberately no option for coroutine frame pooling. It is ON, always, as of 2.12.0 --
# frames come from the scheduler's task slab, and the escape hatch is the RUNTIME call
# JLib::SetCoroFramePooling(false), not a build flag. A build flag would have meant two ABIs and no
# way to A/B it in one process; the runtime switch is what let the two arms be interleaved to measure
# it in the first place. See the frame-allocation section in include/Coroutine.h.

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

# The reactor. TWO tests on purpose: the engine driven from PLAIN C++17 with a continuation task, and
# the co_await surface at C++20. The first is what keeps IoReactor.h's "usable from C++17 in callback
# form" claim honest -- it is the one that would silently stop being true.
if(WIN32)
    add_executable(SchedulerIoC17Test "${CMAKE_CURRENT_SOURCE_DIR}/tests/io_c17_test.cpp")
    target_link_libraries(SchedulerIoC17Test PRIVATE Scheduler)
    target_compile_features(SchedulerIoC17Test PRIVATE cxx_std_17)

    if(JLIBSCHED_COROUTINES)
        add_executable(SchedulerIoAsyncTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/io_async_test.cpp")
        target_link_libraries(SchedulerIoAsyncTest PRIVATE Scheduler)
        target_compile_features(SchedulerIoAsyncTest PRIVATE cxx_std_20)
    endif()
endif()

# Sockets need the co_await surface, so this one is C++20-only like the async test.
if(WIN32 AND JLIBSCHED_COROUTINES)
    add_executable(SchedulerIoSocketTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/io_socket_test.cpp")
    target_link_libraries(SchedulerIoSocketTest PRIVATE Scheduler ws2_32)
    target_compile_features(SchedulerIoSocketTest PRIVATE cxx_std_20)
endif()

# The service layers are opt-in. This process enables nothing and checks that the pool keeps every
# core and that both layers refuse to run -- the check that a job-system-only app pays for neither.
add_executable(SchedulerIoOptInTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/io_optin_test.cpp")
target_link_libraries(SchedulerIoOptInTest PRIVATE Scheduler)
target_compile_features(SchedulerIoOptInTest PRIVATE cxx_std_17)

# Does the reactor's per-operation mutex matter? Answers it with a number. The counters are OFF
# unless JLIBSCHED_IO_LOCK_STATS is set, because counting means a try_lock before every real lock --
# exactly the tax that perturbs what is being measured. Same rule as the steal and latency counters.
option(JLIBSCHED_IO_LOCK_STATS "Count reactor mutex contention (diagnostic; perturbs the I/O path)" OFF)
if(WIN32 AND JLIBSCHED_COROUTINES)
    add_executable(SchedulerIoLockBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/io_lock_contention.cpp")
    target_link_libraries(SchedulerIoLockBench PRIVATE Scheduler ws2_32)
    target_compile_features(SchedulerIoLockBench PRIVATE cxx_std_20)
endif()
if(JLIBSCHED_IO_LOCK_STATS)
    target_compile_definitions(Scheduler PRIVATE JLIBSCHED_IO_LOCK_STATS)
    message(STATUS "JLib::Scheduler: reactor lock stats ENABLED (perturbs the I/O path)")
endif()

# The dispatch seam: reactor-done to coroutine-running. Needs JLIBSCHED_IO_LOCK_STATS for the
# completion timestamp; without it the bench says so rather than printing a flattering zero.
if(WIN32 AND JLIBSCHED_COROUTINES)
    add_executable(SchedulerIoLatencyBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/io_dispatch_latency.cpp")
    target_link_libraries(SchedulerIoLatencyBench PRIVATE Scheduler ws2_32)
    target_compile_features(SchedulerIoLatencyBench PRIVATE cxx_std_20)
endif()

# ---- samples -------------------------------------------------------------------------------------
# udp_rect is the receive-LOOP counterpart to io_socket_test's single datagram: one binary that is
# both ends of a UDP link on loopback, so there is nothing to configure to try it. C++20 because it
# co_awaits the reactor. WIN32 executable would hide the console the sender needs, so it stays a
# console app and creates its window by hand.
if(WIN32 AND JLIBSCHED_COROUTINES)
    add_executable(SchedulerUdpRect "${CMAKE_CURRENT_SOURCE_DIR}/samples/udp_rect.cpp")
    target_link_libraries(SchedulerUdpRect PRIVATE Scheduler ws2_32)
    target_compile_features(SchedulerUdpRect PRIVATE cxx_std_20)
    set_target_properties(SchedulerUdpRect PROPERTIES OUTPUT_NAME udp_rect)
endif()

# Staged-timestamp latency bench for the udp_rect pipeline. --plain runs the same pipeline with a
# blocking recvfrom thread instead of the reactor, which is the control the numbers are read against.
if(WIN32 AND JLIBSCHED_COROUTINES)
    add_executable(SchedulerUdpLatencyBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/udp_latency_bench.cpp")
    target_link_libraries(SchedulerUdpLatencyBench PRIVATE Scheduler ws2_32)
    target_compile_features(SchedulerUdpLatencyBench PRIVATE cxx_std_20)
    set_target_properties(SchedulerUdpLatencyBench PROPERTIES OUTPUT_NAME udp_latency_bench)
endif()

# The KPolicy pair. A PAIRED test: WaitTime must promote where QueueLoad provably does not, on the
# workload measured to defeat volume-based signals. C++17 -- no coroutines involved.
add_executable(SchedulerKPolicyTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/kpolicy_test.cpp")
target_link_libraries(SchedulerKPolicyTest PRIVATE Scheduler)
target_compile_features(SchedulerKPolicyTest PRIVATE cxx_std_17)
