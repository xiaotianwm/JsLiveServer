#pragma once

#include "../../config/config.h"
#include "../../net/net.h"
#include "../../storage/persistence/store.h"
#include "../../runtime/rtmp/stream_manager.h"
#include "../../shared/model/server_types.h"

bool TryHandleAdminRtmpServicesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                     StreamManager& stream_manager, const UserRecord& session_user);
bool TryHandleUserRtmpServicesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                    StreamManager& stream_manager, const UserRecord& session_user);
