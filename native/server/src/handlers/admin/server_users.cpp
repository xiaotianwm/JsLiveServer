#include "server_users.h"

#include "../../shared/http/server_shared.h"
#include "../../shared/users/server_user_utils.h"

#include <map>

namespace {

bool StopManagedUserResources(PersistentStore& store, StreamManager& stream_manager, RoomTaskManager& room_task_manager,
                              const std::string& user_id, const std::string& reason, std::string& error) {
    const auto rooms = store.ListRooms(true, "");
    for (const auto& room : rooms) {
        if (room.owner_id != user_id || room.managed_status != "active") {
            continue;
        }
        RoomRecord stopped_room;
        std::string room_error;
        if (!room_task_manager.StopRoom(room.id, stopped_room, room_error) && room_error != "room is not active") {
            error = room_error;
            return false;
        }
        stream_manager.KickRoom("live", room.stream_name, reason);
    }

    const auto services = store.ListRtmpServices(true, "");
    for (const auto& service : services) {
        if (service.owner_id != user_id || service.managed_status != "active") {
            continue;
        }
        RoomStatePatch patch;
        patch.set_last_error = true;
        patch.last_error.clear();
        patch.set_last_exit_at = true;
        patch.last_exit_at = NowUnix();
        patch.set_stopped_at = true;
        patch.stopped_at = patch.last_exit_at;
        patch.append_log = true;
        patch.log_line = reason;

        RtmpServiceRecord updated;
        std::string service_error;
        if (!store.SetRtmpServiceState(service.id, "inactive", "idle", patch, updated, service_error)) {
            error = service_error;
            return false;
        }
        stream_manager.KickRoom("live", service.stream_name, reason);
    }

    return true;
}

void HandleListUsers(SocketHandle socket, const HttpRequest& request, PersistentStore& store) {
    const auto users = store.ListUsers();
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(users.size()));
    SendAndClose(socket, 200, BuildPaginatedItemsJson(users, page, [&store](const UserRecord& item) {
        return BuildUserJson(item, store.UsageForUser(item));
    }));
}

void HandleCreateUser(SocketHandle socket, const HttpRequest& request, PersistentStore& store) {
    std::map<std::string, std::string> payload;
    std::string parse_error;
    if (!ParseSimpleJsonObject(request.body, payload, parse_error)) {
        SendAndClose(socket, 400, ErrorJson(parse_error));
        return;
    }

    UserRecord user;
    user.username = Trim(payload["username"]);
    user.password = NormalizeStoredPassword(payload["password"]);
    user.role = Trim(payload.count("role") != 0 ? payload["role"] : "user");
    user.status = Trim(payload.count("status") != 0 ? payload["status"] : "active");

    if (user.username.empty()) {
        SendAndClose(socket, 400, ErrorJson("username is required"));
        return;
    }
    if (Trim(payload["password"]).empty()) {
        SendAndClose(socket, 400, ErrorJson("password is required"));
        return;
    }
    if (!IsValidUserRole(user.role)) {
        SendAndClose(socket, 400, ErrorJson("role must be admin or user"));
        return;
    }
    if (!IsValidUserStatus(user.status) || user.status == "deleted") {
        SendAndClose(socket, 400, ErrorJson("status must be active or inactive"));
        return;
    }

    const std::string max_storage_raw = Trim(payload.count("max_storage_bytes") != 0 ? payload["max_storage_bytes"] : "0");
    if (!ParseInt64Strict(max_storage_raw, user.max_storage_bytes) || user.max_storage_bytes < 0) {
        SendAndClose(socket, 400, ErrorJson("max_storage_bytes must be greater than or equal to 0"));
        return;
    }
    const std::string max_rooms_raw = Trim(payload.count("max_active_rooms") != 0 ? payload["max_active_rooms"] : "0");
    if (!ParseIntStrict(max_rooms_raw, user.max_active_rooms) || user.max_active_rooms < 0) {
        SendAndClose(socket, 400, ErrorJson("max_active_rooms must be greater than or equal to 0"));
        return;
    }
    if (!ParseUserDate(payload["subscription_ends_at"], user.subscription_ends_at)) {
        SendAndClose(socket, 400, ErrorJson("subscription_ends_at is invalid"));
        return;
    }
    user.created_at = NowUnix();

    std::string store_error;
    if (!store.CreateUser(user, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 201, BuildUserJson(user, store.UsageForUser(user)));
}

bool LoadManagedUser(SocketHandle socket, PersistentStore& store, const std::string& user_id, UserRecord& managed_user) {
    if (store.GetUserByID(user_id, managed_user)) {
        return true;
    }
    SendAndClose(socket, 404, ErrorJson("user not found"));
    return false;
}

void HandleGetUser(SocketHandle socket, PersistentStore& store, const UserRecord& managed_user) {
    SendAndClose(socket, 200, BuildUserJson(managed_user, store.UsageForUser(managed_user)));
}

void HandlePatchUser(SocketHandle socket, const HttpRequest& request, PersistentStore& store, StreamManager& stream_manager,
                     RoomTaskManager& room_task_manager, const UserRecord& session_user, const UserRecord& managed_user) {
    if (managed_user.status == "deleted") {
        SendAndClose(socket, 409, ErrorJson("deleted users cannot be updated"));
        return;
    }

    std::map<std::string, std::string> payload;
    std::string parse_error;
    if (!ParseSimpleJsonObject(request.body, payload, parse_error)) {
        SendAndClose(socket, 400, ErrorJson(parse_error));
        return;
    }

    UserRecord updated_user = managed_user;
    bool changed = false;
    bool revoke_sessions = false;
    bool disable_account = false;

    if (payload.count("password") != 0) {
        const std::string password = Trim(payload["password"]);
        if (password.empty()) {
            SendAndClose(socket, 400, ErrorJson("password cannot be empty"));
            return;
        }
        updated_user.password = NormalizeStoredPassword(password);
        changed = true;
        revoke_sessions = true;
    }
    if (payload.count("status") != 0) {
        const std::string status = Trim(payload["status"]);
        if (!IsValidUserStatus(status) || status == "deleted") {
            SendAndClose(socket, 400, ErrorJson("status must be active or inactive"));
            return;
        }
        if (managed_user.id == session_user.id && status != "active") {
            SendAndClose(socket, 400, ErrorJson("cannot disable your own account"));
            return;
        }
        updated_user.status = status;
        changed = true;
        disable_account = status != "active";
    }
    if (payload.count("max_storage_bytes") != 0) {
        const std::string raw = Trim(payload["max_storage_bytes"]);
        if (!ParseInt64Strict(raw, updated_user.max_storage_bytes) || updated_user.max_storage_bytes < 0) {
            SendAndClose(socket, 400, ErrorJson("max_storage_bytes must be greater than or equal to 0"));
            return;
        }
        changed = true;
    }
    if (payload.count("max_active_rooms") != 0) {
        const std::string raw = Trim(payload["max_active_rooms"]);
        if (!ParseIntStrict(raw, updated_user.max_active_rooms) || updated_user.max_active_rooms < 0) {
            SendAndClose(socket, 400, ErrorJson("max_active_rooms must be greater than or equal to 0"));
            return;
        }
        changed = true;
    }
    if (payload.count("subscription_ends_at") != 0) {
        if (!ParseUserDate(payload["subscription_ends_at"], updated_user.subscription_ends_at)) {
            SendAndClose(socket, 400, ErrorJson("subscription_ends_at is invalid"));
            return;
        }
        changed = true;
    }

    if (!changed) {
        SendAndClose(socket, 400, ErrorJson("no fields to update"));
        return;
    }

    if (disable_account) {
        std::string resource_error;
        if (!StopManagedUserResources(store, stream_manager, room_task_manager, managed_user.id, "owner account disabled",
                                      resource_error)) {
            SendAndClose(socket, 500, ErrorJson(resource_error));
            return;
        }
        revoke_sessions = true;
    }

    std::string store_error;
    if (!store.UpdateUser(updated_user, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    if (revoke_sessions && !store.DeleteAuthSessionsForUser(updated_user.id, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, BuildUserJson(updated_user, store.UsageForUser(updated_user)));
}

void HandleDeleteUser(SocketHandle socket, PersistentStore& store, StreamManager& stream_manager, RoomTaskManager& room_task_manager,
                      const UserRecord& session_user, UserRecord managed_user) {
    if (managed_user.id == session_user.id) {
        SendAndClose(socket, 400, ErrorJson("cannot delete your own account"));
        return;
    }
    if (managed_user.status == "deleted") {
        SendAndClose(socket, 409, ErrorJson("user is already deleted"));
        return;
    }

    std::string resource_error;
    if (!StopManagedUserResources(store, stream_manager, room_task_manager, managed_user.id, "owner account deleted", resource_error)) {
        SendAndClose(socket, 500, ErrorJson(resource_error));
        return;
    }

    managed_user.status = "deleted";
    std::string store_error;
    if (!store.UpdateUser(managed_user, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    if (!store.DeleteAuthSessionsForUser(managed_user.id, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, BuildUserJson(managed_user, store.UsageForUser(managed_user)));
}

}  // namespace

bool TryHandleAdminUsersRoute(SocketHandle socket, const HttpRequest& request, PersistentStore& store, StreamManager& stream_manager,
                              RoomTaskManager& room_task_manager, const UserRecord& session_user) {
    if (request.method == "GET" && request.path == "/api/v1/admin/users") {
        HandleListUsers(socket, request, store);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/admin/users") {
        HandleCreateUser(socket, request, store);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if (parts.size() != 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "admin" || parts[3] != "users") {
        return false;
    }

    UserRecord managed_user;
    if (!LoadManagedUser(socket, store, parts[4], managed_user)) {
        return true;
    }

    if (request.method == "GET") {
        HandleGetUser(socket, store, managed_user);
        return true;
    }
    if (request.method == "PATCH") {
        HandlePatchUser(socket, request, store, stream_manager, room_task_manager, session_user, managed_user);
        return true;
    }
    if (request.method == "DELETE") {
        HandleDeleteUser(socket, store, stream_manager, room_task_manager, session_user, managed_user);
        return true;
    }

    return false;
}
