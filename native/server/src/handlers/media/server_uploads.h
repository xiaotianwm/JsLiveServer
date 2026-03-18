#pragma once

#include "../../config/config.h"
#include "../../net/net.h"
#include "../../storage/persistence/store.h"
#include "../../shared/model/server_types.h"

#include <string>

bool TryHandleAdminUploadsRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                const UserRecord& session_user);
bool TryHandleUserUploadsRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                               const UserRecord& session_user);

void HandleInitUploadRequest(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                             const UserRecord& session_user);
void HandleGetUploadRequest(SocketHandle socket, PersistentStore& store, const UserRecord& session_user, const std::string& upload_id);
void HandleUploadPartRequest(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                             const UserRecord& session_user, const std::string& upload_id, int part_number);
void HandleCompleteUploadRequest(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const UserRecord& session_user,
                                 const std::string& upload_id, bool admin_scope);
void HandleDeleteUploadRequest(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const UserRecord& session_user,
                               const std::string& upload_id);
void HandleUploadRequest(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                         const UserRecord& session_user, bool admin_scope);
