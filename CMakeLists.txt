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
# 5.0.0 -- a MAJOR bump, and the 4.0 line's patch numbers were already understating it. 4.0.1
# removed PushImmediate, 4.0.2 deleted the hiPri Chase-Lev deques and rewrote the two band
# controllers into a single packed word. Each of those changed what the API means, not just what it
# does, and calling them patches made two trees that behave differently report nearly the same
# number -- the exact failure the paragraph above says this field exists to prevent.
#
# What forces the major is Join() leaving the public surface. It advertised a teardown that could be
# followed by a restart, and no such restart ever existed (Init() throws on a non-null instance,
# StartPool is private). Removing an API that never worked is still removing an API.
set(JLIBSCHED_VERSION "5.0.0")

# Steal-path instrumentation. OFF by design: the counters live in the steal loop, which is precisely
# where contention is being investigated, so leaving them in a shipping build would tax the thing
# they exist to measure. Turn on only for an experiment, and do not compare its numbers against a
# normal build's.
option(JLIBSCHED_STEAL_STATS "Count steal probes and hits (diagnostic; perturbs the steal path)" OFF)
option(JLIBSCHED_RETRY_STATS "Record CAS retries per call -- max and buckets, not an average (diagnostic; sits inside CAS loops)" OFF)
option(JLIBSCHED_REQUEUE_TRACE "Count which of Requeue's three exits each resume takes (diagnostic; sits on the resume path)" OFF)
# ---- THREAD SANITIZER ------------------------------------------------------------------------
#
# Finds data races by watching actual memory accesses and computing happens-before. The reason it is
# worth having is that it shares NO ASSUMPTIONS with this codebase: every other check here was
# written by the same people who wrote the code, and an instrument built from a misunderstanding
# agrees with the code for the wrong reason. TSan cannot do that -- it has no model of what this
# scheduler is supposed to do.
#
# ############ IT DOES NOT UNDERSTAND FIBERS. READ THIS BEFORE BELIEVING A RESULT. ############
#
# TSan tracks happens-before PER THREAD. A ContextSwitch swaps the stack out from under it, so two
# fibers running on one worker are attributed to one thread: real races between them are MISSED, and
# accesses correctly ordered by a fiber handoff can be reported as races that do not exist. It ships
# an annotation API for this -- __tsan_create_fiber / __tsan_destroy_fiber / __tsan_switch_to_fiber
# -- and until Context.cpp/FiberInit call it, ANY finding that involves a fiber stack is unproven in
# both directions.
#
# So: trust findings on NATIVE task paths, the push path, the permit machine, the bands word, the
# inboxes and the deques. Treat anything naming a fiber as needing the annotations first. This is the
# same trap as ASan-plus-fibers, where "stack-use-after-return" turned out to be shadow leftovers
# from fiber reuse.
#
# LINUX ONLY (WSL is fine). MSVC has no thread sanitizer. Everything must be built with it,
# including the library, which is why this is a whole-tree option rather than a per-target flag.
#
#   wsl -e bash -lc "cmake -S /mnt/c/T_Threads -B ~/tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo \
#                          -DJLIBSCHED_TSAN=ON && cmake --build ~/tsan -j"
#
# Build OUT of /mnt/c -- a Linux-native build dir is far faster and avoids the stale-object trap
# that a shared Windows path has produced here before.
option(JLIBSCHED_TSAN "Build with -fsanitize=thread (Linux/WSL only; see the fiber caveat above)" OFF)

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

# TSan BEFORE the targets, and as a DIRECTORY property, because every object in the program must be
# instrumented -- a library compiled without it makes the runtime blind to exactly the code under
# test, and the result is a clean report that means nothing. Link flags too: the runtime is linked
# in, not just compiled against.
if(JLIBSCHED_TSAN)
    if(MSVC)
        message(FATAL_ERROR "JLIBSCHED_TSAN: MSVC has no thread sanitizer. Build under WSL/Linux.")
    endif()
    # -O1 and frame pointers: TSan slows execution 5-15x and inflates memory ~5-10x, so an
    # unoptimised build can turn a race window that needs microseconds into one that never opens.
    # -g so a report names a line rather than an address.
    add_compile_options(-fsanitize=thread -g -O1 -fno-omit-frame-pointer)
    # Drives include/TsanFiber.h. Defined by CMake rather than sniffing __SANITIZE_THREAD__ or
    # __has_feature, so GCC and Clang behave identically and the switch lives in one place.
    add_compile_definitions(JLIB_TSAN=1)
    add_link_options(-fsanitize=thread)
    message(STATUS "JLib::Scheduler: THREAD SANITIZER ON -- findings on FIBER paths are unproven "
                   "until __tsan_switch_to_fiber annotations exist; see the option's comment")
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
# PUBLIC for the same reason as the steal counters: RetryStats.h is a header, so the library and
# anything reading the report must agree on whether the cells exist. Half the program compiled with
# them and half without is two different structs and a silently wrong answer.
if(JLIBSCHED_RETRY_STATS)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_RETRY_STATS=1)
    message(STATUS "JLib::Scheduler: CAS retry distribution ON (diagnostic build, not for perf numbers)")
endif()
if(JLIBSCHED_REQUEUE_TRACE)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_REQUEUE_TRACE=1)
    message(STATUS "JLib::Scheduler: Requeue branch trace ON (diagnostic build, not for perf numbers)")
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
    # WaitOnAddress / WakeByAddressSingle -- the worker park. PUBLIC because the symbols are
    # referenced from Thread.obj inside Scheduler.lib, so every consumer linking the static library
    # needs it too. An import library for API-set forwarders, not a DLL dependency: the calls
    # resolve to kernel32/ntdll at run time on Windows 8 and later.
    target_link_libraries(Scheduler PUBLIC Synchronization)
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
# THE LANE AS IT IS NOW. bench.cpp and io_dispatch_latency.cpp still carry arms for mechanisms that
# no longer exist -- adaptive K, the lane deque, the producer-side spill, K=4 rungs against a K
# clamped to 2 -- and rows measuring a mechanism the code does not have are worse than no rows: two
# arms of the same configuration read as a finding about the difference between them. This file
# measures only the shared intake, K's never-park-and-steal policy, and Lane routing.
add_executable(SchedulerLaneBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/lane_bench.cpp")
target_link_libraries(SchedulerLaneBench PRIVATE Scheduler)
target_compile_features(SchedulerLaneBench PRIVATE cxx_std_17)

add_executable(SchedulerBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/bench.cpp")
target_link_libraries(SchedulerBench PRIVATE Scheduler)
target_compile_features(SchedulerBench PRIVATE cxx_std_17)
# Stamped into the header line so a pasted result identifies the build that produced it.
target_compile_definitions(SchedulerBench PRIVATE JLIBSCHED_VERSION="${JLIBSCHED_VERSION}")

# What one raw fiber context switch costs, and whether the legacy-SSE `movdqa` block in
# src/win32/ContextSwitch.asm pays an SSE/AVX transition when a fiber parks with dirty YMM state.
#
# Windows x86-64 ONLY, and that is not a portability gap: the whole question is about the encoding
# of one x86 instruction. AArch64 has no SSE/AVX domains and nothing to measure.
#
# The variants live in the BENCH, not in src/. Three context switches shipping "just in case" is
# exactly what this bench exists to avoid -- src/win32/ContextSwitch.asm is not touched unless the
# timer says it should be.
# Decomposes the serial submit-and-wait round trip that compare_marl.cpp reports at ~4.5 us against
# marl's ~0.82 us: how much is the WORKER being woken, and how much is the SUBMITTER being woken.
# All arms in one process -- idle policy and K are both runtime settings.
# WINDOWS-ONLY, because the source includes <windows.h> unconditionally -- it reads thread times
# through GetThreadTimes to split "the worker was woken" from "the submitter was woken", and there is
# no portable equivalent it currently falls back to.
#
# GUARDED HERE RATHER THAN #ifdef'd IN THE SOURCE, because the failure was not that this bench does
# not run on Linux -- it is that it does not COMPILE, and an unconditional target meant
# `cmake --build .` aborted the whole tree at this file. Three tests after it in the build order were
# simply never produced, which reads as "those tests do not exist on Linux" rather than as one bench
# being unportable.
if(WIN32)
    add_executable(SchedulerWaitForBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/waitfor_participation.cpp")
    target_link_libraries(SchedulerWaitForBench PRIVATE Scheduler)
    target_compile_features(SchedulerWaitForBench PRIVATE cxx_std_17)
endif()

if(WIN32 AND NOT JLIBSCHED_ARCH STREQUAL "aarch64")
    set(JLIBSCHED_CSBENCH_ASM "${CMAKE_CURRENT_SOURCE_DIR}/bench/win32/context_switch_variants.asm")
    # Pinned for the same reason the library's own assembly is pinned a few hundred lines up: let
    # CMake infer the language from the extension and any third-party add_subdirectory that enables
    # the generic ASM language breaks this target -- and names our symbols in the error, not theirs.
    set_source_files_properties("${JLIBSCHED_CSBENCH_ASM}" PROPERTIES LANGUAGE ASM_MASM)
    add_executable(SchedulerContextSwitchBench
        "${CMAKE_CURRENT_SOURCE_DIR}/bench/context_switch.cpp"
        "${JLIBSCHED_CSBENCH_ASM}")
    target_link_libraries(SchedulerContextSwitchBench PRIVATE Scheduler)
    target_compile_features(SchedulerContextSwitchBench PRIVATE cxx_std_17)
endif()

# Measures what an IDLE SPINNING pool costs the main thread, and how much of that the steal hint
# removes. Both arms run inside one process against one pool -- see the file header.
add_executable(SchedulerEpochVsMutex "${CMAKE_CURRENT_SOURCE_DIR}/bench/epoch_vs_mutex.cpp")
target_link_libraries(SchedulerEpochVsMutex PRIVATE Scheduler)
target_compile_features(SchedulerEpochVsMutex PRIVATE cxx_std_17)

add_executable(SchedulerEpochMechBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/epoch_mechanisms.cpp")
target_link_libraries(SchedulerEpochMechBench PRIVATE Scheduler)
target_compile_features(SchedulerEpochMechBench PRIVATE cxx_std_17)

add_executable(SchedulerSpinTax "${CMAKE_CURRENT_SOURCE_DIR}/bench/spin_tax.cpp")
target_link_libraries(SchedulerSpinTax PRIVATE Scheduler)
target_compile_features(SchedulerSpinTax PRIVATE cxx_std_17)

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

add_executable(SchedulerTimerTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/timer_test.cpp")
target_link_libraries(SchedulerTimerTest PRIVATE Scheduler)
target_compile_features(SchedulerTimerTest PRIVATE cxx_std_17)


add_executable(SchedulerReservedCoresTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/reserved_cores_test.cpp")
target_link_libraries(SchedulerReservedCoresTest PRIVATE Scheduler)
target_compile_features(SchedulerReservedCoresTest PRIVATE cxx_std_17)


add_executable(SchedulerDagBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/dag_scaling.cpp")
target_link_libraries(SchedulerDagBench PRIVATE Scheduler)
target_compile_features(SchedulerDagBench PRIVATE cxx_std_17)

add_executable(SchedulerDagExternalTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/dag_external_test.cpp")
target_link_libraries(SchedulerDagExternalTest PRIVATE Scheduler)
target_compile_features(SchedulerDagExternalTest PRIVATE cxx_std_17)

add_executable(SchedulerPrimitivesTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/primitives_test.cpp")
target_link_libraries(SchedulerPrimitivesTest PRIVATE Scheduler)
target_compile_features(SchedulerPrimitivesTest PRIVATE cxx_std_17)

# THE C++20 COROUTINE TIER IS GONE. 5.0 made this a fibers-only runtime, I/O included, so there
# is no second language mode to build: no JLIBSCHED_COROUTINES option, no cxx_std_20 targets, and
# no split standard to police. Everything here is C++17, which used to be a property the build
# enforced and is now simply what the project is.
#
# The old layer lives unbuilt under extras/abandoned/ -- see its README.

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

# ONE REACTOR TEST NOW, and it is the C++17 one. The co_await surface it used to be paired with is
# gone with the coroutine layer, so what was "the arm that keeps the C++17 claim honest" is simply
# the only arm -- there is no other language mode for it to be honest against.
#
# THE SOCKET SUITE WENT WITH IT, and that is a real loss worth naming rather than burying: 1003 lines
# covering accept/connect/send/recv/vectored/peer-close/the acceptor pool/Stop drain/cancellation/
# deadlines/IoStream chaining, and the only thing that exercised the Linux io_uring backend. It was
# written entirely on co_await. Until it is ported to the fiber path, IoReactor::IsAvailable() on
# Linux is asserted by nothing. See extras/abandoned/io_socket_test.cpp.
if(WIN32)
    add_executable(SchedulerIoC17Test "${CMAKE_CURRENT_SOURCE_DIR}/tests/io_c17_test.cpp")
    target_link_libraries(SchedulerIoC17Test PRIVATE Scheduler)
    target_compile_features(SchedulerIoC17Test PRIVATE cxx_std_17)

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
if(JLIBSCHED_IO_LOCK_STATS)
    target_compile_definitions(Scheduler PRIVATE JLIBSCHED_IO_LOCK_STATS)
    message(STATUS "JLib::Scheduler: reactor lock stats ENABLED (perturbs the I/O path)")
endif()

# The dispatch seam: reactor-done to coroutine-running. Needs JLIBSCHED_IO_LOCK_STATS for the
# completion timestamp; without it the bench says so rather than printing a flattering zero.

# ---- samples -------------------------------------------------------------------------------------
# udp_rect is the receive-LOOP counterpart to io_socket_test's single datagram: one binary that is
# both ends of a UDP link on loopback, so there is nothing to configure to try it. C++20 because it
# co_awaits the reactor. WIN32 executable would hide the console the sender needs, so it stays a
# console app and creates its window by hand.

# Staged-timestamp latency bench for the udp_rect pipeline. --plain runs the same pipeline with a
# blocking recvfrom thread instead of the reactor, which is the control the numbers are read against.


# Eager mutex cancellation. Every case holds the lock for its whole duration and never unlocks before
# asserting, so a waiter that comes back came back because it was EJECTED.
add_executable(SchedulerMutexCancelTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/mutex_cancel_test.cpp")
target_link_libraries(SchedulerMutexCancelTest PRIVATE Scheduler)
target_compile_features(SchedulerMutexCancelTest PRIVATE cxx_std_17)

# Teardown drains parked work instead of abandoning it. The assertion is an RAII destructor running
# on a parked fiber's stack -- which can only happen if that stack was actually unwound.
# A full lane must not lose a task -- it GROWS. Covers the two push_bottom call sites that used to
# ignore the bool, both of which failed as a HANG rather than a visible drop, which is why they
# survived. Named deque_overflow_test until growth replaced the overflow lanes it was checking.


# Ingress backpressure: a flooding external producer is slowed, a WORKER never is (that would
# deadlock -- a task pushing chunks is a consumer of the inbox it would wait on).

# THE GROWTH CEILING MUST ACTUALLY ABORT. grow() replaced push_bottom's `return false` and the
# overflow lanes are gone, so this is the only thing between an infinitely self-spawning task and
# unbounded memory. A death test: the binary re-executes itself and checks how the child died.
add_executable(SchedulerDequeCeilingTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/deque_ceiling_test.cpp")
target_link_libraries(SchedulerDequeCeilingTest PRIVATE Scheduler)
target_compile_features(SchedulerDequeCeilingTest PRIVATE cxx_std_17)

# The steal hint past bit 63. Oversubscribes deliberately (100 workers) because that is the only way
# to reach a two-word hint bitmap without a 128-thread machine -- every other test on a normal box
# runs the one-word path and would pass with the second word wired up backwards.
# Reachability, not throughput: can the selection step NAME the victim holding the work? Both bugs
# it exists for were invisible to the rest of the suite -- work still ran, just never anywhere else.
# Does 256-bit vector state survive a fiber suspend? ContextSwitch.asm saves only the LOW 128 bits
# of XMM6-15, which the Win64 ABI says is complete -- but the failure mode if that reasoning is wrong
# is SILENT wrong arithmetic, not a crash, so it gets a test rather than a paragraph.
# AVX2 is required to build it; the runtime check is the CPU's, not CMake's.
add_executable(SchedulerAvxSuspendTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/avx_suspend_test.cpp")
target_link_libraries(SchedulerAvxSuspendTest PRIVATE Scheduler)
target_compile_features(SchedulerAvxSuspendTest PRIVATE cxx_std_17)
if(MSVC)
    target_compile_options(SchedulerAvxSuspendTest PRIVATE /arch:AVX2)
else()
    target_compile_options(SchedulerAvxSuspendTest PRIVATE -mavx2)
endif()

# Does Join() let cancelled frames UNWIND? Deleted once during the refactor as "no longer needed",
# then restored: with the quiescence step in Join it passes, and it is the only thing that measures
# whether shutdown runs destructors or merely stops running. See the quiesce block in Join().
add_executable(SchedulerTeardownDrainTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/teardown_drain_test.cpp")
target_link_libraries(SchedulerTeardownDrainTest PRIVATE Scheduler)
target_compile_features(SchedulerTeardownDrainTest PRIVATE cxx_std_17)


add_executable(SchedulerReservedBandLiveKTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/reserved_band_live_k_test.cpp")
target_link_libraries(SchedulerReservedBandLiveKTest PRIVATE Scheduler)
target_compile_features(SchedulerReservedBandLiveKTest PRIVATE cxx_std_17)


add_executable(SchedulerLaneNoStealTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/lane_no_steal_test.cpp")
target_link_libraries(SchedulerLaneNoStealTest PRIVATE Scheduler)
target_compile_features(SchedulerLaneNoStealTest PRIVATE cxx_std_17)


add_executable(SchedulerStealHintWordsTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/steal_hint_words_test.cpp")
target_link_libraries(SchedulerStealHintWordsTest PRIVATE Scheduler)
target_compile_features(SchedulerStealHintWordsTest PRIVATE cxx_std_17)


# Hazard pointers -- Michael's algorithm over a reader-indexed table (fibers by poolIndex, native
# tasks by worker, external threads by a reserved block). NO COROUTINE PATH: a HazardGuard in a
# coroutine frame is refused at Protect, because a frame has no dense stable index and the record
# registry that used to cover it had a grace period nobody could state or test. Use counted epochs
# for a reader that suspends. See design/hazard-pointers.md.
add_executable(SchedulerHazardTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/hazard_test.cpp")
target_link_libraries(SchedulerHazardTest PRIVATE Scheduler)
target_compile_features(SchedulerHazardTest PRIVATE cxx_std_17)

add_executable(SchedulerHazardOrphanTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/hazard_orphan_test.cpp")
target_link_libraries(SchedulerHazardOrphanTest PRIVATE Scheduler)
target_compile_features(SchedulerHazardOrphanTest PRIVATE cxx_std_17)

add_executable(SchedulerHazardCeilingTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/hazard_ceiling_test.cpp")
target_link_libraries(SchedulerHazardCeilingTest PRIVATE Scheduler)
target_compile_features(SchedulerHazardCeilingTest PRIVATE cxx_std_17)

# THE SUSPENSION CHECK MUST ACTUALLY FIRE. Nothing else in the suite holds a hazard guard across a
# co_await, so a green suite says nothing about it. C++20: it needs a real coroutine to suspend.

# The awake floor must grow under a wave AND shed back to base when idle. Shedding failed silently
# for a whole bench run before this existed -- a one-way controller reads as a latency regression.
add_executable(SchedulerAwakeFloorShedTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/awake_floor_shed_test.cpp")
target_link_libraries(SchedulerAwakeFloorShedTest PRIVATE Scheduler)
target_compile_features(SchedulerAwakeFloorShedTest PRIVATE cxx_std_17)

# Standalone wake-latency variance harness: condvar vs WaitOnAddress, isolated 2-thread ping-pong.
# See memory: futex was rejected twice on this measurement; do not re-propose it without a fresh run.
# WINDOWS-ONLY: it compares WaitOnAddress against a condvar and links winmm for
# timeBeginPeriod/timeEndPeriod, none of which exist elsewhere. Guarded for the same reason as
# SchedulerWaitForBench above -- an unconditional target here aborted the whole Linux build, so
# everything after it in the build order silently never got produced.
if(WIN32)
    add_executable(SchedulerFutexVariance "${CMAKE_CURRENT_SOURCE_DIR}/bench/futex_variance.cpp")
    target_link_libraries(SchedulerFutexVariance PRIVATE Scheduler)
    target_link_libraries(SchedulerFutexVariance PRIVATE winmm)   # timeBeginPeriod/timeEndPeriod
    target_compile_features(SchedulerFutexVariance PRIVATE cxx_std_17)
endif()

# Completion latency against a saturated pool, swept over bulk-body length. Reconstructs the
# harness behind PickNextWorker's "hiPri was briefly steered at the unreserved floor, and it lost",
# whose numbers outlived the file that produced them. Takes K as argv[1] (0 = no reserved band).
add_executable(SchedulerIoOverlapTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/io_overlap_test.cpp")
target_link_libraries(SchedulerIoOverlapTest PRIVATE Scheduler)
target_compile_features(SchedulerIoOverlapTest PRIVATE cxx_std_17)

# Runs ~TaskScheduler and every member destructor beneath it -- the code production deliberately
# never executes, because Init() news the instance and AtExitDestroyer leaks it. See the file header:
# an unexecuted destructor already hid a heap-corruption bug here for the life of the project.
add_executable(SchedulerTeardownDestroyTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/teardown_destroy_test.cpp")
target_link_libraries(SchedulerTeardownDestroyTest PRIVATE Scheduler)
target_compile_features(SchedulerTeardownDestroyTest PRIVATE cxx_std_17)


# ORDINARY WORK MUST NEVER BE PLACED IN A RESERVED WORKER'S LOPRI INBOX. Two arms -- floor base 0
# (the configuration the placement bug lived in) and floor base >= K (the path that was already
# masked). See the header of the test for why it pauses the pool instead of sampling.
add_executable(SchedulerReservedLoPriPlacementTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/reserved_lopri_placement_test.cpp")
target_link_libraries(SchedulerReservedLoPriPlacementTest PRIVATE Scheduler)
target_compile_features(SchedulerReservedLoPriPlacementTest PRIVATE cxx_std_17)

# K RUNS FIBER JOBS, THE FLOOR STAYS FIBERLESS. The reserved lane carries I/O completions, which are
# Native (CreateInternalTask) and want to suspend -- so K binds a fiber. The FLOOR arm is the control
# that stops "K got fibers" being confused with "everyone got fibers", which would put a
# ContextSwitch on every task in the pool. Asserts the capability (a Native task suspending on an
# event) and not just the binding.
# FIBER-LOCAL STORAGE. The answer to the one cost of migratable mode: TLS read before a suspension
# point belongs to a thread the fiber may no longer be on. Asserts both halves -- FLS survives a
# migration, thread_local does NOT -- because arm 1 alone is also consistent with "nothing migrated".
# The TLS accessor is noinline: MSVC caches a thread_local base across an opaque call, which once
# made migratable_fiber_test report 0 migrations out of 206.
# THE DEATH/REAPER CONTRACT. The creditor chain is append-only for one fiber life, which is safe only
# if owners are unique per worker (not one node per hop), cleanup is idempotent, and the chain is a
# real reuse fence. Every one of those failures produces a PLAUSIBLE run -- a flooded inbox still
# completes, a double release still returns -- so the counts are the only thing that separates them.
add_executable(SchedulerFiberReaperContractTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_reaper_contract_test.cpp")
target_link_libraries(SchedulerFiberReaperContractTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberReaperContractTest PRIVATE cxx_std_17)

# FIBER DEBTS: several kinds owed by one fiber, each released EXACTLY once. The reaper reaches each
# creditor once -- that is what makes double-release impossible -- so that visit must discharge every
# kind, which is why it is a list and a loop. Counts are asserted PER KIND, not summed: a total is
# also satisfied by one-missed cancelling one-doubled, which are opposite bugs.
add_executable(SchedulerFiberDebtTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_debt_test.cpp")
target_link_libraries(SchedulerFiberDebtTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberDebtTest PRIVATE cxx_std_17)

# A PARKED RESERVED WORKER MUST BE WOKEN BY A LANE PUSH. The shared intake belongs to no worker, so
# nothing else will look at it -- PushLaneIntake's notify is the only thing between a full intake and
# a permanently asleep lane. The race is closed by that NOTIFY and not by the park predicate, which
# reads an approximate size_approx(), so this pins the notify against a future reader deleting it as
# redundant. Uses SetHotWorkers rather than SetIoHotLane: the shipped config pins K awake and never
# exercises this path at all.
add_executable(SchedulerLaneIntakeWakeTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/lane_intake_wake_test.cpp")
target_link_libraries(SchedulerLaneIntakeWakeTest PRIVATE Scheduler)
target_compile_features(SchedulerLaneIntakeWakeTest PRIVATE cxx_std_17)

add_executable(SchedulerFiberLocalTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_local_test.cpp")
target_link_libraries(SchedulerFiberLocalTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberLocalTest PRIVATE cxx_std_17)

add_executable(SchedulerReservedFiberJobTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/reserved_fiber_job_test.cpp")
target_link_libraries(SchedulerReservedFiberJobTest PRIVATE Scheduler)
target_compile_features(SchedulerReservedFiberJobTest PRIVATE cxx_std_17)

# FIBER REGISTRY, STRUCTURE IN ISOLATION. Drives RegisterFiberHome/TakeFiberHomeList against fibers
# the test owns, rather than routing tasks through the scheduler to reach them -- see the test
# header for why, and for the four -D negative controls that must each go RED.
add_executable(SchedulerFiberRegistryTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_registry_test.cpp")
target_link_libraries(SchedulerFiberRegistryTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberRegistryTest PRIVATE cxx_std_17)
# The four negative controls, driven by a CACHE variable so the define actually reaches the compiler
# and MSBuild sees a changed command line. Passing /D through CL_ADDITIONALOPTIONS does NOT work:
# with no source change MSBuild skips the compile and silently reruns the OLD binary, so every
# control reports as passing -- which is indistinguishable from the controls being weak, and is the
# same stale-binary trap that has produced false results here before.
#   cmake -S . -B build-src -DJLIB_FIBERREG_CTL=CTL_DROP_ONE
#   cmake --build build-src --config Release --target SchedulerFiberRegistryTest
#   ./build-src/bin/Release/SchedulerFiberRegistryTest.exe     # MUST FAIL
if(JLIB_FIBERREG_CTL)
  target_compile_definitions(SchedulerFiberRegistryTest PRIVATE ${JLIB_FIBERREG_CTL})
endif()

# THE CLEANUP CHAIN, IN ISOLATION. Drives FiberRegistry::AdvanceCleanup through its test seams so
# the assertion is about the chain rather than about whether a pool came up -- see the test header,
# and the three -D negative controls that must each go RED.
add_executable(SchedulerFiberCleanupChainTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_cleanup_chain_test.cpp")
target_link_libraries(SchedulerFiberCleanupChainTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberCleanupChainTest PRIVATE cxx_std_17)
if(JLIB_FIBERCHAIN_CTL)
  target_compile_definitions(SchedulerFiberCleanupChainTest PRIVATE ${JLIB_FIBERCHAIN_CTL})
endif()

# THE HOLDER SIDE OF FiberRegistry, IN ISOLATION. Two spaces, two arrays: `table` by
# Fiber::poolIndex (debtors), `inbound` by holder (threads). Thread 3 is not fiber 3, and aliasing
# them would be silent, so the separation is asserted rather than assumed. No pool and no scheduler
# -- the seams exist so the assertion is about the structure.
add_executable(SchedulerFiberHolderTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_holder_test.cpp")
target_link_libraries(SchedulerFiberHolderTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberHolderTest PRIVATE cxx_std_17)

# THE THREE DRAIN POINTS, AGAINST A LIVE POOL. The test above proves the structure; this proves the
# WIRING -- that a fiber delivered to holder N is released BY WORKER N, that a parked worker is woken
# to do it, and that main and Join() settle what no worker can. Every assertion is about WHICH THREAD
# ran the release, because a test that only counted releases would pass with the routing deleted.
add_executable(SchedulerFiberDrainLiveTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/fiber_drain_live_test.cpp")
target_link_libraries(SchedulerFiberDrainLiveTest PRIVATE Scheduler)
target_compile_features(SchedulerFiberDrainLiveTest PRIVATE cxx_std_17)

# ---- NEGATIVE CONTROLS FOR BOTH, ONE SWITCH -------------------------------------------------
#
# Every one of these must go RED. They live in the LIBRARY (Deliver, AdvanceCleanup, ClaimExternal,
# the worker drain), so the define is PUBLIC on Scheduler -- reaching only the test would link it
# against an unmodified registry and the control would silently pass.
#
#   cmake -S . -B build-ctl -DJLIBSCHED_FIBERHOLDER_CTL=FANOUT
#   cmake --build build-ctl --config Release --target SchedulerFiberHolderTest
#   ./build-ctl/bin/Release/SchedulerFiberHolderTest.exe          # MUST FAIL
#
#   NO_EXHAUST_GUARD   external claim wraps          -> holder test: distinctness
#   FANOUT             deliver-all then recycle      -> holder test: "ran before first recycle"
#   DROP_CREDITOR      one NoteCreditor skipped      -> holder test: creditor census
#   NO_NOTIFY          Deliver stops waking          -> live test: the PARKED case
#   NO_WORKER_DRAIN    worker never drains its own   -> live test: the PARKED case
#
# A CACHE variable, not CL_ADDITIONALOPTIONS: with no source change MSBuild skips the compile and
# reruns the OLD binary, so every control reports as passing -- indistinguishable from the controls
# being weak, and the same stale-binary trap that has produced false results here before. VERIFY the
# define reached the compiler (grep the generated .vcxproj) before believing a control that passes;
# a PowerShell loop once expanded nothing and ran two "controls" against the unmodified build.
# The DAG Fire() default-push control: removes the else arm, so dag_priority_node_test MUST time
# out on the priority cases. Same PUBLIC-define reasoning as the holder controls below.
# DAG TERMINAL TRACE: counts arrivals at OnTaskFinished per node and reports a SECOND one. Seven
# call sites reach that path and nothing guards "exactly one wins", so 0 terminals (zombie node,
# graph hangs) and 2 terminals (dependents fired twice, node retired twice) are indistinguishable
# from the outside -- both look like "the test hangs sometimes". Diagnostic; default OFF.
if(JLIBSCHED_DAG_TERMINAL_TRACE)
  target_compile_definitions(Scheduler PUBLIC JLIBSCHED_DAG_TERMINAL_TRACE=1)
  message(STATUS "JLib::Scheduler: DAG terminal trace ON (diagnostic build)")
endif()

if(JLIBSCHED_DAGFIRE_CTL)
  target_compile_definitions(Scheduler PUBLIC JLIB_DAGFIRE_CTL_${JLIBSCHED_DAGFIRE_CTL}=1)
  message(STATUS "JLib::Scheduler: NEGATIVE CONTROL DAGFIRE ${JLIBSCHED_DAGFIRE_CTL} -- dag_priority_node_test MUST fail")
endif()

if(JLIBSCHED_FIBERHOLDER_CTL)
  target_compile_definitions(Scheduler PUBLIC JLIB_FIBERHOLDER_CTL_${JLIBSCHED_FIBERHOLDER_CTL}=1)
  message(STATUS "JLib::Scheduler: NEGATIVE CONTROL ${JLIBSCHED_FIBERHOLDER_CTL} -- the matching test MUST fail")
endif()

# THE PUBLIC JOB IS A FIBER. Asserts the BEHAVIOUR, not the type tag: a plain CreateTask must be
# able to suspend and resume (illegal under the old Native default), and ParallelFor grains must
# still take no fiber, or grain concurrency silently becomes bounded by the fiber pool.
# A DAG NODE WITH AN EXPLICIT PRIORITY MUST RUN. Fire() dispatched on isMain/isFork/isLocal with
# no else, and CreateNode sets isLocal = (priority == NONE) -- so any explicit priority stranded the
# node: marked submitted, never pushed, dependents deadlocked. Reachable through public API and
# invisible to a suite whose callers all take the default.
add_executable(SchedulerDagPriorityNodeTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/dag_priority_node_test.cpp")
target_link_libraries(SchedulerDagPriorityNodeTest PRIVATE Scheduler)
target_compile_features(SchedulerDagPriorityNodeTest PRIVATE cxx_std_17)

# WHO MAY BLOCK ON A CONTENDED SchedulerMutex. Measures CPU TIME rather than acquisition, because
# "main got the lock" passes equally against the spin this replaced; a deliberate-spin arm keeps the
# instrument honest. Also pins the may-not-block refusal for fiberless tasks, controls first.
add_executable(SchedulerMutexBlockPolicyTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/mutex_block_policy_test.cpp")
target_link_libraries(SchedulerMutexBlockPolicyTest PRIVATE Scheduler)
target_compile_features(SchedulerMutexBlockPolicyTest PRIVATE cxx_std_17)

# THREE STACK CLASSES. Tiny and Deep default to 0 per worker (the 127 MB lesson), so a green suite
# says nothing about them -- this is the only thing that turns them on. Asserts the class bound from
# INSIDE the task, that poolIndex stays dense across classes (Event's waiter hash and
# FiberRegistry's table both depend on it), and that a released fiber goes home to its own class.
add_executable(SchedulerStackClassesTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/stack_classes_test.cpp")
target_link_libraries(SchedulerStackClassesTest PRIVATE Scheduler)
target_compile_features(SchedulerStackClassesTest PRIVATE cxx_std_17)

# EPOCH RECLAMATION ACTUALLY FREES. Nothing asserted this before, which was survivable while a
# global queue meant any reclaimer drained every retirement. Per-thread bags mean only the OWNER
# drains its own, so a green suite no longer implies memory comes back. Carries its own negative
# control (no tick -> nothing freed) and asserts the behaviour change explicitly: another thread's
# tick does not drain a live thread's bag, and a thread that exits hands its bag off rather than
# stranding it.
add_executable(SchedulerEpochReclaimTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/epoch_reclaim_test.cpp")
target_link_libraries(SchedulerEpochReclaimTest PRIVATE Scheduler)
target_compile_features(SchedulerEpochReclaimTest PRIVATE cxx_std_17)

add_executable(SchedulerEverythingIsAFiberTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/everything_is_a_fiber_test.cpp")
target_link_libraries(SchedulerEverythingIsAFiberTest PRIVATE Scheduler)
target_compile_features(SchedulerEverythingIsAFiberTest PRIVATE cxx_std_17)

# THREAD LAYOUT: how many cache lines one Push touches on the consumer's Thread. Prints the offsets
# rather than reasoning about them from source order -- see the test header.
add_executable(SchedulerThreadLayoutTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/thread_layout_test.cpp")
target_link_libraries(SchedulerThreadLayoutTest PRIVATE Scheduler)
target_compile_features(SchedulerThreadLayoutTest PRIVATE cxx_std_17)

# DO FIBERS ACTUALLY MIGRATE? Runs both modes in one process: pinned is the control and must report
# ZERO resumes on another worker, migratable must report some. See the test header for why a
# migratable-only run could not tell "broken" from "never given the chance".
add_executable(SchedulerMigratableFiberTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/migratable_fiber_test.cpp")
target_link_libraries(SchedulerMigratableFiberTest PRIVATE Scheduler)
target_compile_features(SchedulerMigratableFiberTest PRIVATE cxx_std_17)

# QUIESCE: push, STOP, and check nothing was left behind. The only test that can distinguish a lost
# wakeup (TERMINAL) from a stall (RESOLVES) -- every timing row runs under continuous load, where a
# lost wake is covered by the next push. Assertion is completed == pushed; no classifier, and it
# deliberately never uses WaitFor, which would spin-help and run the work under test itself.
add_executable(SchedulerQuiesceTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/quiesce_test.cpp")
target_link_libraries(SchedulerQuiesceTest PRIVATE Scheduler)
target_compile_features(SchedulerQuiesceTest PRIVATE cxx_std_17)

# THE OTHER HALF OF io_c17_test'S CLAIM. That file asserts Submit STAMPS a continuation Tiny; this
# one asserts the stamp survives to a BOUND FIBER -- steered to the reserved band, where a Native
# task actually binds one. On the floor a Native continuation is fiberless, so io_c17_test never
# reaches the per-class cache at all and the whole tiny pool could be broken without it noticing.
# Windows-only for the same reason io_c17_test is: it drives CreateFile + SubmitRead.
if(WIN32)
    add_executable(SchedulerIoTinyStackTest "${CMAKE_CURRENT_SOURCE_DIR}/tests/io_tiny_stack_test.cpp")
    target_link_libraries(SchedulerIoTinyStackTest PRIVATE Scheduler)
    target_compile_features(SchedulerIoTinyStackTest PRIVATE cxx_std_17)
endif()
