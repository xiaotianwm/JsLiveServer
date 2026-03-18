#pragma once

#include "../../config/config.h"
#include "../../net/net.h"
#include "../../storage/persistence/store.h"
#include "../../shared/model/server_types.h"

#include <string>
#include <vector>

std::string BuildFileJson(const FileRecord& file, const std::vector<RoomRecord>& all_rooms);

bool TryHandleAdminFilesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store);
bool TryHandleUserFilesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                             const UserRecord& session_user);

void HandleAdminListFiles(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store);
void HandleAdminPreviewFile(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                            const std::string& file_id);
void HandleAdminDeleteFile(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const std::string& file_id);

void HandleUserListFiles(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                         const UserRecord& session_user);
void HandleUserPreviewFile(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                           const UserRecord& session_user, const std::string& file_id);
void HandleUserDeleteFile(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const UserRecord& session_user,
                          const std::string& file_id);
