#pragma once

#include "../../net/net.h"
#include "../../runtime/rooms/room_task_manager.h"
#include "../../storage/persistence/store.h"
#include "../../runtime/rtmp/stream_manager.h"
#include "../../shared/model/server_types.h"

bool TryHandleAdminUsersRoute(SocketHandle socket, const HttpRequest& request, PersistentStore& store, StreamManager& stream_manager,
                              RoomTaskManager& room_task_manager, const UserRecord& session_user);
