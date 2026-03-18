#include "net.h"

#include <cerrno>
#include <cstring>

namespace net {

namespace {

int ResolveFamily(const std::string& host) {
    if (host.empty() || host == "0.0.0.0") {
        return AF_INET;
    }
    if (host.find(':') != std::string::npos && host.find('.') == std::string::npos) {
        return AF_INET6;
    }
    return AF_INET;
}

}  // namespace

bool Initialize(std::string& error) {
#ifdef _WIN32
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
        error = LastError();
        return false;
    }
#else
    (void)error;
#endif
    return true;
}

void Cleanup() {
#ifdef _WIN32
    WSACleanup();
#endif
}

std::string LastError() {
#ifdef _WIN32
    return "winsock error " + std::to_string(WSAGetLastError());
#else
    return std::strerror(errno);
#endif
}

bool CreateListener(const std::string& host, std::uint16_t port, SocketHandle& out, std::string& error) {
    out = kInvalidSocket;

    addrinfo hints{};
    hints.ai_family = ResolveFamily(host);
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    addrinfo* result = nullptr;
    const std::string port_string = std::to_string(port);
    const char* node = host.empty() || host == "0.0.0.0" ? nullptr : host.c_str();
    const int gai = getaddrinfo(node, port_string.c_str(), &hints, &result);
    if (gai != 0) {
#ifdef _WIN32
        error = "getaddrinfo failed: " + std::to_string(gai);
#else
        error = gai_strerror(gai);
#endif
        return false;
    }

    std::string last_error = "bind failed";
    for (addrinfo* current = result; current != nullptr; current = current->ai_next) {
        SocketHandle socket = ::socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        if (socket == kInvalidSocket) {
            last_error = LastError();
            continue;
        }

        int reuse = 1;
        setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

        if (::bind(socket, current->ai_addr, static_cast<int>(current->ai_addrlen)) == 0 &&
            ::listen(socket, SOMAXCONN) == 0) {
            out = socket;
            freeaddrinfo(result);
            return true;
        }

        last_error = LastError();
        Close(socket);
    }

    freeaddrinfo(result);
    error = last_error;
    return false;
}

bool Accept(SocketHandle listener, SocketHandle& out, std::string& client_ip, std::string& error) {
    out = kInvalidSocket;
    sockaddr_storage address{};
    socklen_t length = sizeof(address);
    SocketHandle socket = ::accept(listener, reinterpret_cast<sockaddr*>(&address), &length);
    if (socket == kInvalidSocket) {
        error = LastError();
        return false;
    }

    char host[NI_MAXHOST] = {0};
    if (getnameinfo(reinterpret_cast<const sockaddr*>(&address), length, host, sizeof(host), nullptr, 0, NI_NUMERICHOST) == 0) {
        client_ip = host;
    } else {
        client_ip = "unknown";
    }

    out = socket;
    return true;
}

bool SendAll(SocketHandle socket, const std::uint8_t* data, std::size_t size) {
    std::size_t offset = 0;
    while (offset < size) {
        const int sent = ::send(socket, reinterpret_cast<const char*>(data + offset), static_cast<int>(size - offset), 0);
        if (sent <= 0) {
            return false;
        }
        offset += static_cast<std::size_t>(sent);
    }
    return true;
}

bool RecvAll(SocketHandle socket, std::uint8_t* data, std::size_t size) {
    std::size_t offset = 0;
    while (offset < size) {
        const int received = ::recv(socket, reinterpret_cast<char*>(data + offset), static_cast<int>(size - offset), 0);
        if (received <= 0) {
            return false;
        }
        offset += static_cast<std::size_t>(received);
    }
    return true;
}

int RecvSome(SocketHandle socket, std::uint8_t* data, std::size_t size) {
    return ::recv(socket, reinterpret_cast<char*>(data), static_cast<int>(size), 0);
}

void Close(SocketHandle& socket) {
    if (socket == kInvalidSocket) {
        return;
    }
#ifdef _WIN32
    closesocket(socket);
#else
    close(socket);
#endif
    socket = kInvalidSocket;
}

}  // namespace net
