// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// WINSOCK SPELLING, ON BOTH PLATFORMS -- so the I/O tests are one file rather than two.
//
// WHY THIS EXISTS. The I/O tests were written against Winsock and gated `if(WIN32)` in CMake, so
// the Linux reactor had no verification at all: 45 uses of SOCKET, 30 of closesocket, WSAStartup,
// winsock2.h. Porting them by hand would produce a second copy that drifts from the first, and the
// two would then disagree about what the reactor promises -- which is the failure this library's
// one-CMakeLists-for-two-repos note already describes.
//
// SO THE TESTS ARE NOT PORTED; the spelling is. Winsock's names win because they are the ones
// already written down, not because they are better: SOCKET, INVALID_SOCKET, closesocket and a
// WSAStartup that does nothing on POSIX. A test reads identically on both platforms and the diff
// against the Windows-only version stays empty.
//
// NOT A GENERAL COMPATIBILITY LAYER, and deliberately not shipped in include/. It covers exactly
// what the tests use and nothing else. A library-grade shim would have to answer for error-code
// mapping, non-blocking modes and address families across four platforms; a test shim only has to
// let these tests compile and mean the same thing. Growing it past that is a sign the tests want a
// real abstraction, not a bigger header.

#pragma once

#if defined(_WIN32)

  #include <winsock2.h>
  #include <ws2tcpip.h>
  // Everything is already spelled this way. Nothing to do.

#else

  #include <arpa/inet.h>
  #include <errno.h>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <sys/socket.h>
  #include <sys/types.h>
  #include <unistd.h>

  // A POSIX socket is an int fd; Winsock's SOCKET is a UINT_PTR. IoReactor::IoSocket is already
  // uintptr_t precisely so it holds either without a cast at the call site, and this matches that
  // choice rather than inventing a third convention.
  using SOCKET = int;

  // INVALID_SOCKET IS -1 HERE, NOT ~0. Winsock returns an unsigned sentinel; POSIX returns -1, and
  // the tests compare against this name rather than against a literal. Getting it wrong would make
  // every failed socket() read as success on Linux -- and the tests would then proceed to use fd -1,
  // which fails later and somewhere else.
  static constexpr SOCKET INVALID_SOCKET = -1;
  static constexpr int    SOCKET_ERROR   = -1;

  inline int closesocket(SOCKET s) { return ::close(s); }

  // WINSOCK STARTUP HAS NO POSIX EQUIVALENT, and that is the whole content of these two. They
  // succeed so the test's own assertion ("WSAStartup succeeded -- the app's job, not the library's")
  // still says something true on both platforms: on Windows the app must call it, on POSIX there is
  // nothing to call. Returning 0 is the honest answer, not a stub.
  struct WSADATA { int unused; };
  inline int WSAStartup(unsigned short, WSADATA* d) { if (d) d->unused = 0; return 0; }
  inline int WSACleanup() { return 0; }
  #define MAKEWORD(lo, hi) ((unsigned short)(((unsigned char)(lo)) | ((unsigned short)((unsigned char)(hi))) << 8))

  // Winsock reports through a separate call; POSIX uses errno. Same shape at the call site.
  inline int WSAGetLastError() { return errno; }

  // shutdown() direction constants. POSIX spells them SHUT_*; the tests were written against
  // Winsock, so the Winsock names map onto them rather than the other way round.
  #define SD_RECEIVE SHUT_RD
  #define SD_SEND    SHUT_WR
  #define SD_BOTH    SHUT_RDWR

  // IPPROTO_TCP, AF_INET, SOCK_STREAM, htons/ntohs, inet_pton, bind, listen, connect, accept,
  // getsockname, setsockopt, send, recv and shutdown are all POSIX already and need no mapping --
  // which is why this header is short. The Winsock-only surface really is just the handle type,
  // close, and startup.

#endif
