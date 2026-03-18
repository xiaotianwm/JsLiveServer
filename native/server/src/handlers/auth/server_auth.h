#pragma once

#include "../../config/config.h"
#include "../../net/net.h"
#include "../../storage/persistence/store.h"
#include "../../shared/model/server_types.h"

#include <string>

bool ResolveSession(const HttpRequest& request, PersistentStore& store, UserRecord& user, std::string& error);
bool TryHandleAuthPublicRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store);
bool TryHandleSelfRoute(SocketHandle socket, const HttpRequest& request, PersistentStore& store, bool authed, const std::string& session_error,
                        const UserRecord& session_user);
