#pragma once

#include <cstdint>
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

namespace net {

bool Initialize(std::string& error);
void Cleanup();

bool CreateListener(const std::string& host, std::uint16_t port, SocketHandle& out, std::string& error);
bool Accept(SocketHandle listener, SocketHandle& out, std::string& client_ip, std::string& error);
bool SendAll(SocketHandle socket, const std::uint8_t* data, std::size_t size);
bool RecvAll(SocketHandle socket, std::uint8_t* data, std::size_t size);
int RecvSome(SocketHandle socket, std::uint8_t* data, std::size_t size);
void Close(SocketHandle& socket);
std::string LastError();

}  // namespace net
