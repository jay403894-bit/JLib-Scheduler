// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE PLATFORM SEAM UNDER IoAcceptor AND IoStream -- four functions and one constant.
//
// WHY IT EXISTS. IoAcceptor and IoStream are queueing and ordering logic: accept backlog with
// waiters, send/recv chaining so a connection needs no lock. Almost none of that is
// platform-specific -- across both classes the Windows-only surface was 18 uses of a TYPE (SOCKET),
// four closesocket calls, one socket-creation call, one error constant and one descriptor
// conversion. But they lived in src/win32/IoReactor.cpp, so a Linux build had no IoAcceptor at all,
// and the port looked like it required reimplementing waiter queueing.
//
// It does not. It requires THIS -- a seam narrow enough that both platforms run the same queueing
// code, which is the only version of this that stays correct. A second implementation of "accept
// backlog with cancellable waiters" would drift from the first, and the two would then disagree
// about what the reactor promises on one platform but not the other. That is precisely the failure
// this repository already refuses elsewhere (one CMakeLists for two repos, one Event model for one
// shipping structure).
//
// WHAT DOES NOT BELONG HERE. Anything an operation needs -- opcodes, submission, completion -- is
// the backend's job and lives in win32/IoReactor.cpp or posix/IoReactor.cpp. This header is only
// what the SHARED layer above them cannot express portably. If it grows past a screen, the split is
// in the wrong place.

#pragma once

#include "../include/IoReactor.h"

#if defined(_WIN32)
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <errno.h>
  #include <netinet/in.h>   // IPPROTO_TCP -- IoAcceptor's default protocol, and NOT in sys/socket.h
  #include <sys/socket.h>
  #include <sys/types.h>
  #include <unistd.h>
#endif

namespace JLib { namespace ioplat {

// "The caller handed us more buffers than a request can hold." Reported through IoResult::error, so
// it has to be a real platform error number rather than a sentinel -- a caller logging errno or
// WSAGetLastError-style codes must get something that means what it says.
#if defined(_WIN32)
    inline constexpr std::int32_t kErrMsgSize = WSAEMSGSIZE;
#else
    inline constexpr std::int32_t kErrMsgSize = EMSGSIZE;
#endif

inline void CloseSocket(IoSocket s) noexcept {
#if defined(_WIN32)
    ::closesocket(static_cast<SOCKET>(s));
#else
    ::close(static_cast<int>(s));
#endif
}

// ---- THE ACCEPT BACKLOG'S TWO SOCKET CALLS, split the way the caller already uses them ---------
//
// AcceptEx requires a socket to ALREADY EXIST before the accept is posted, and it must match the
// listener's family, type and protocol -- a mismatch fails inside AcceptEx with an error that does
// not say which argument was wrong. So IoAcceptor pre-creates its backlog, and it asks the listener
// once rather than making the caller repeat itself.
//
// TWO FUNCTIONS, NOT ONE, because that is how IoAcceptor already works: it queries the triple ONCE
// at Start and then creates a socket per slot from the cached values. A single
// MakeSocketLike(listener) would have been tidier to declare and would have added a getsockopt to
// every socket creation and every re-post -- turning a startup cost into a per-connection one.
//
// Both return false / 0 on failure. Zero is not a valid socket in this codebase's usage: IoSocket is
// uintptr_t, and fd 0 is stdin, which a listener is not.

// Ask a listener what it is. Windows has one call that answers all three; POSIX needs the bound
// address for the family (SO_DOMAIN is Linux-only and this may reach other POSIX targets) plus two
// socket options.
inline bool QuerySocketTriple(IoSocket listener, int& family, int& type, int& proto) noexcept {
#if defined(_WIN32)
    WSAPROTOCOL_INFOW info{};
    int len = sizeof info;
    if (::getsockopt(static_cast<SOCKET>(listener), SOL_SOCKET, SO_PROTOCOL_INFOW,
                     reinterpret_cast<char*>(&info), &len) != 0)
        return false;
    family = info.iAddressFamily;
    type   = info.iSocketType;
    proto  = info.iProtocol;
    return true;
#else
    sockaddr_storage ss{};
    socklen_t sslen = sizeof ss;
    if (::getsockname(static_cast<int>(listener), reinterpret_cast<sockaddr*>(&ss), &sslen) != 0)
        return false;
    family = ss.ss_family;

    socklen_t tlen = sizeof type;
    if (::getsockopt(static_cast<int>(listener), SOL_SOCKET, SO_TYPE, &type, &tlen) != 0)
        return false;

    proto = 0;      // 0 = "the default for this family/type", which is correct for TCP and UDP
#if defined(SO_PROTOCOL)
    socklen_t plen = sizeof proto;
    (void)::getsockopt(static_cast<int>(listener), SOL_SOCKET, SO_PROTOCOL, &proto, &plen);
#endif
    return true;
#endif
}

// Create one socket from a triple obtained above.
inline IoSocket MakeSocket(int family, int type, int proto) noexcept {
#if defined(_WIN32)
    // WSA_FLAG_OVERLAPPED is not optional: a socket created without it cannot be used with the
    // completion port at all, and the failure surfaces later at the first submit.
    const SOCKET s = ::WSASocketW(family, type, proto, nullptr, 0, WSA_FLAG_OVERLAPPED);
    return (s == INVALID_SOCKET) ? 0 : static_cast<IoSocket>(s);
#else
    const int fd = ::socket(family, type, proto);
    return (fd < 0) ? 0 : static_cast<IoSocket>(fd);
#endif
}

// The "no socket" value in seam terms. IoAcceptor compares against this rather than INVALID_SOCKET
// so the shared code needs no Winsock spelling.
inline constexpr IoSocket kNoSocket = 0;

// DESCRIPTOR CONVERSION, into the request's own storage. The array must outlive the CALL, not just
// the submission -- both platforms keep reading it while the operation is in flight, so building it
// on the submitting stack is a use-after-free that usually appears to work.
//
// Returns false when the caller passed more segments than IoRequest::kMaxVectors, which the shared
// layer reports as kErrMsgSize rather than truncating. Truncating would send less data than asked
// and report success.
bool FillBufs(IoRequest* r, const IoBuffer* bufs, std::uint32_t count) noexcept;

}} // namespace JLib::ioplat
