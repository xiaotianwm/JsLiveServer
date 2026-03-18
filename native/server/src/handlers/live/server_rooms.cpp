#include "server_rooms.h"

#include "../../shared/http/server_shared.h"

#include <algorithm>
#include <ctime>
#include <iomanip>
#include <map>
#include <sstream>
#include <vector>

namespace {

struct RoomPayload {
    std::string name;
    std::string mode;
    std::string rtmp_url;
    std::string input_url;
    std::string file_id;
};

std::string JsonEscape(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char ch : value) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    return out;
}

std::string JsonString(const std::string& value) {
    return "\"" + JsonEscape(value) + "\"";
}

std::string JsonBool(bool value) {
    return value ? "true" : "false";
}

std::string FormatTime(std::int64_t unix_time) {
    if (unix_time <= 0) {
        return "null";
    }
    const std::time_t value = static_cast<std::time_t>(unix_time);
    std::tm tm_value{};
#ifdef _WIN32
    gmtime_s(&tm_value, &value);
#else
    gmtime_r(&value, &tm_value);
#endif
    std::ostringstream out;
    out << "\"" << std::put_time(&tm_value, "%Y-%m-%dT%H:%M:%SZ") << "\"";
    return out.str();
}

std::string NullableTime(std::int64_t unix_time) {
    return unix_time > 0 ? FormatTime(unix_time) : "null";
}

std::string BuildStorageUsageJson(const StorageUsage& usage) {
    std::ostringstream out;
    out << "{"
        << "\"used_storage_bytes\":" << usage.used_storage_bytes << ","
        << "\"reserved_storage_bytes\":" << usage.reserved_storage_bytes << ","
        << "\"max_storage_bytes\":" << usage.max_storage_bytes << ","
        << "\"available_storage_bytes\":" << usage.available_storage_bytes << ","
        << "\"active_room_count\":" << usage.active_room_count << ","
        << "\"max_active_rooms\":" << usage.max_active_rooms << ","
        << "\"available_room_slots\":" << usage.available_room_slots
        << "}";
    return out.str();
}

bool RoomIsConfigured(const RoomRecord& room) {
    if (Trim(room.name).empty() || Trim(room.rtmp_url).empty()) {
        return false;
    }
    if (room.mode == "network") {
        return !Trim(room.input_url).empty();
    }
    if (room.mode == "file") {
        return !Trim(room.file_id).empty();
    }
    return false;
}

std::string RoomStatusText(const RoomRecord& room) {
    if (!RoomIsConfigured(room)) {
        return "not configured";
    }
    if (room.managed_status == "inactive") {
        return room.runtime_status == "stopping" ? "stopping" : "stopped";
    }
    if (room.runtime_status == "running") {
        return "running";
    }
    if (room.runtime_status == "starting") {
        return "starting";
    }
    if (room.runtime_status == "retry_wait") {
        return room.next_retry_at > 0 ? "retrying" : "waiting to retry";
    }
    if (room.runtime_status == "stopping") {
        return "stopping";
    }
    if (room.runtime_status == "failed") {
        return room.last_error.empty() ? "failed" : "failed: " + room.last_error;
    }
    return room.last_error.empty() ? room.runtime_status : room.last_error;
}

std::string ResolveRoomFileName(const RoomRecord& room, PersistentStore* store) {
    if (store == nullptr || room.file_id.empty()) {
        return {};
    }
    FileRecord file;
    if (!store->GetFileByID(room.file_id, file)) {
        return {};
    }
    return file.display_name.empty() ? file.original_name : file.display_name;
}

std::string BuildRoomJson(const RoomRecord& room, PersistentStore* store = nullptr) {
    std::ostringstream out;
    out << "{"
        << "\"id\":" << JsonString(room.id) << ","
        << "\"name\":" << JsonString(room.name) << ","
        << "\"owner_id\":" << JsonString(room.owner_id) << ","
        << "\"owner_name\":" << JsonString(room.owner_name) << ","
        << "\"stream_name\":" << JsonString(room.stream_name) << ","
        << "\"mode\":" << JsonString(room.mode) << ","
        << "\"configured\":" << JsonBool(RoomIsConfigured(room)) << ","
        << "\"input_url\":" << JsonString(room.input_url) << ","
        << "\"file_id\":" << JsonString(room.file_id) << ","
        << "\"file_name\":" << JsonString(ResolveRoomFileName(room, store)) << ","
        << "\"rtmp_url\":" << JsonString(room.rtmp_url) << ","
        << "\"managed_status\":" << JsonString(room.managed_status) << ","
        << "\"runtime_status\":" << JsonString(room.runtime_status) << ","
        << "\"last_error\":" << JsonString(room.last_error) << ","
        << "\"retry_count\":" << room.retry_count << ","
        << "\"next_retry_at\":" << NullableTime(room.next_retry_at) << ","
        << "\"last_start_attempt_at\":" << NullableTime(room.last_start_attempt_at) << ","
        << "\"last_running_at\":" << NullableTime(room.last_running_at) << ","
        << "\"last_exit_at\":" << NullableTime(room.last_exit_at) << ","
        << "\"created_at\":" << FormatTime(room.created_at) << ","
        << "\"updated_at\":" << FormatTime(room.updated_at) << ","
        << "\"activated_at\":" << NullableTime(room.activated_at) << ","
        << "\"stopped_at\":" << NullableTime(room.stopped_at) << ","
        << "\"log_line_count\":" << room.log_line_count << ","
        << "\"latest_log\":" << JsonString(room.latest_log) << ","
        << "\"status_text\":" << JsonString(RoomStatusText(room)) << ","
        << "\"slot_occupied\":" << JsonBool(room.managed_status == "active")
        << "}";
    return out.str();
}

std::string BuildRoomSummaryJson(const std::vector<RoomRecord>& items) {
    int active_rooms = 0;
    int inactive_rooms = 0;
    int running_rooms = 0;
    int retry_wait_rooms = 0;
    int starting_rooms = 0;
    int stopping_rooms = 0;
    int idle_rooms = 0;
    int failed_rooms = 0;
    int network_mode_rooms = 0;
    int file_mode_rooms = 0;
    int rooms_with_error = 0;
    std::map<std::string, bool> owners;

    for (const auto& room : items) {
        owners[room.owner_id] = true;
        if (room.managed_status == "active") {
            ++active_rooms;
        } else {
            ++inactive_rooms;
        }
        if (room.runtime_status == "running") {
            ++running_rooms;
        } else if (room.runtime_status == "retry_wait") {
            ++retry_wait_rooms;
        } else if (room.runtime_status == "starting") {
            ++starting_rooms;
        } else if (room.runtime_status == "stopping") {
            ++stopping_rooms;
        } else if (room.runtime_status == "idle") {
            ++idle_rooms;
        } else if (room.runtime_status == "failed") {
            ++failed_rooms;
        }
        if (room.mode == "network") {
            ++network_mode_rooms;
        } else if (room.mode == "file") {
            ++file_mode_rooms;
        }
        if (!room.last_error.empty()) {
            ++rooms_with_error;
        }
    }

    std::ostringstream out;
    out << "{"
        << "\"total_rooms\":" << items.size() << ","
        << "\"filtered_rooms\":" << items.size() << ","
        << "\"active_rooms\":" << active_rooms << ","
        << "\"inactive_rooms\":" << inactive_rooms << ","
        << "\"running_rooms\":" << running_rooms << ","
        << "\"retry_wait_rooms\":" << retry_wait_rooms << ","
        << "\"starting_rooms\":" << starting_rooms << ","
        << "\"stopping_rooms\":" << stopping_rooms << ","
        << "\"idle_rooms\":" << idle_rooms << ","
        << "\"failed_rooms\":" << failed_rooms << ","
        << "\"network_mode_rooms\":" << network_mode_rooms << ","
        << "\"file_mode_rooms\":" << file_mode_rooms << ","
        << "\"rooms_with_error\":" << rooms_with_error << ","
        << "\"unique_owner_count\":" << owners.size()
        << "}";
    return out.str();
}

std::string BuildRoomActionJson(const RoomRecord& room, const StorageUsage& usage, PersistentStore* store = nullptr) {
    return std::string("{\"room\":") + BuildRoomJson(room, store) + ",\"usage\":" + BuildStorageUsageJson(usage) + "}";
}

std::string BuildRoomLogsJson(const RoomRecord& room, int offset, int limit) {
    const int total = static_cast<int>(room.recent_logs.size());
    if (offset < 0) {
        offset = 0;
    }
    if (limit <= 0) {
        limit = 50;
    }
    if (offset > total) {
        offset = total;
    }
    const int end = total - offset;
    const int start = std::max(0, end - limit);
    std::ostringstream out;
    out << "{"
        << "\"room_id\":" << JsonString(room.id) << ","
        << "\"total\":" << total << ","
        << "\"offset\":" << offset << ","
        << "\"limit\":" << limit << ","
        << "\"has_more\":" << JsonBool(start > 0) << ","
        << "\"items\":[";
    for (int i = start; i < end; ++i) {
        if (i != start) {
            out << ",";
        }
        out << JsonString(room.recent_logs[static_cast<std::size_t>(i)]);
    }
    out << "],"
        << "\"latest_log\":" << JsonString(room.latest_log) << ","
        << "\"status_text\":" << JsonString(RoomStatusText(room)) << ","
        << "\"updated_at\":" << FormatTime(room.updated_at)
        << "}";
    return out.str();
}

bool CanAccessRoom(const UserRecord& user, const RoomRecord& room) {
    return user.role == "admin" || room.owner_id == user.id;
}

bool HasRoomStreamConfig(const RoomPayload& payload) {
    return !Trim(payload.mode).empty() || !Trim(payload.input_url).empty() || !Trim(payload.file_id).empty() ||
           !Trim(payload.rtmp_url).empty();
}

bool ValidateRoomPayload(const RoomPayload& payload, bool allow_draft, std::string& error) {
    if (Trim(payload.name).empty()) {
        error = "name is required";
        return false;
    }
    if (allow_draft && !HasRoomStreamConfig(payload)) {
        return true;
    }
    if (Trim(payload.rtmp_url).empty()) {
        error = "rtmp_url is required";
        return false;
    }
    if (payload.mode == "network") {
        if (Trim(payload.input_url).empty()) {
            error = "input_url is required for network mode";
            return false;
        }
        return true;
    }
    if (payload.mode == "file") {
        if (Trim(payload.file_id).empty()) {
            error = "file_id is required for file mode";
            return false;
        }
        return true;
    }
    error = "mode must be network or file";
    return false;
}

bool ParseRoomPayload(const HttpRequest& request, bool allow_draft, RoomPayload& payload, std::string& error) {
    std::map<std::string, std::string> object;
    if (!ParseSimpleJsonObject(request.body, object, error)) {
        return false;
    }
    payload.name = Trim(object["name"]);
    payload.mode = Trim(object["mode"]);
    payload.rtmp_url = Trim(object["rtmp_url"]);
    payload.input_url = Trim(object["input_url"]);
    payload.file_id = Trim(object["file_id"]);
    return ValidateRoomPayload(payload, allow_draft, error);
}

void ApplyRoomPayload(RoomRecord& room, const RoomPayload& payload) {
    room.name = payload.name;
    room.mode = payload.mode;
    room.input_url = payload.input_url;
    room.file_id = payload.file_id;
    room.rtmp_url = payload.rtmp_url;
    room.updated_at = NowUnix();
}

void HandleAdminListRooms(SocketHandle socket, const HttpRequest& request, PersistentStore& store) {
    const auto rooms = store.ListRooms(true, "");
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(rooms.size()));
    SendAndClose(socket, 200, BuildPaginatedItemsJson(rooms, page, [&store](const RoomRecord& item) {
        return BuildRoomJson(item, &store);
    }, "\"filters\":{\"owner_id\":\"\",\"owner_username\":\"\",\"managed_status\":\"\",\"runtime_status\":\"\",\"mode\":\"\",\"query\":\"\"},\"summary\":" +
            BuildRoomSummaryJson(rooms)));
}

void HandleUserListRooms(SocketHandle socket, const HttpRequest& request, PersistentStore& store, const UserRecord& session_user) {
    const auto rooms = store.ListRooms(false, session_user.id);
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(rooms.size()));
    SendAndClose(socket, 200, BuildPaginatedItemsJson(rooms, page, [&store](const RoomRecord& item) {
        return BuildRoomJson(item, &store);
    }));
}

void HandleAdminCreateRoom(SocketHandle socket, const HttpRequest& request, PersistentStore& store, const UserRecord& session_user) {
    std::map<std::string, std::string> object;
    std::string parse_error;
    if (!ParseSimpleJsonObject(request.body, object, parse_error)) {
        SendAndClose(socket, 400, ErrorJson(parse_error));
        return;
    }

    UserRecord owner = session_user;
    const std::string owner_id = Trim(object["owner_id"]);
    if (!owner_id.empty() && !store.GetUserByID(owner_id, owner)) {
        SendAndClose(socket, 404, ErrorJson("owner not found"));
        return;
    }

    RoomPayload payload;
    payload.name = Trim(object["name"]);
    payload.mode = Trim(object["mode"]);
    payload.rtmp_url = Trim(object["rtmp_url"]);
    payload.input_url = Trim(object["input_url"]);
    payload.file_id = Trim(object["file_id"]);
    std::string validation_error;
    if (!ValidateRoomPayload(payload, true, validation_error)) {
        SendAndClose(socket, 400, ErrorJson(validation_error));
        return;
    }

    RoomRecord room;
    std::string store_error;
    if (!store.CreateRoom(owner, payload.name, payload.mode, payload.input_url, payload.file_id, payload.rtmp_url, room, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 201, BuildRoomJson(room, &store));
}

void HandleUserCreateRoom(SocketHandle socket, const HttpRequest& request, PersistentStore& store, const UserRecord& session_user) {
    RoomPayload payload;
    std::string error;
    if (!ParseRoomPayload(request, true, payload, error)) {
        SendAndClose(socket, 400, ErrorJson(error));
        return;
    }

    RoomRecord room;
    std::string store_error;
    if (!store.CreateRoom(session_user, payload.name, payload.mode, payload.input_url, payload.file_id, payload.rtmp_url, room, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 201, BuildRoomJson(room, &store));
}

bool LoadAdminRoom(SocketHandle socket, PersistentStore& store, const std::string& room_id, RoomRecord& room) {
    if (store.GetRoomByID(room_id, room)) {
        return true;
    }
    SendAndClose(socket, 404, ErrorJson("room not found"));
    return false;
}

bool LoadUserRoom(SocketHandle socket, PersistentStore& store, const UserRecord& session_user, const std::string& room_id, RoomRecord& room) {
    if (store.GetRoomByID(room_id, room) && CanAccessRoom(session_user, room)) {
        return true;
    }
    SendAndClose(socket, 404, ErrorJson("room not found"));
    return false;
}

void HandleRoomUpdate(SocketHandle socket, const HttpRequest& request, PersistentStore& store, RoomRecord& room) {
    if (room.managed_status != "inactive" || room.runtime_status != "idle") {
        SendAndClose(socket, 409, ErrorJson("room must be stopped before editing"));
        return;
    }

    RoomPayload payload;
    std::string error;
    if (!ParseRoomPayload(request, false, payload, error)) {
        SendAndClose(socket, 400, ErrorJson(error));
        return;
    }

    ApplyRoomPayload(room, payload);
    std::string store_error;
    if (!store.UpdateRoom(room, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    if (!store.AppendRoomLog(room.id, "Room updated", room, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, BuildRoomJson(room, &store));
}

void HandleRoomDelete(SocketHandle socket, StreamManager& stream_manager, RoomTaskManager& room_task_manager,
                      const RoomRecord& room) {
    std::string manager_error;
    if (!room_task_manager.DeleteRoom(room.id, manager_error)) {
        SendAndClose(socket, 409, ErrorJson(manager_error));
        return;
    }
    const std::size_t disconnected = stream_manager.KickRoom("live", room.stream_name, "room deleted");
    SendAndClose(socket, 200, std::string("{\"deleted\":true,\"disconnected_sessions\":") + std::to_string(disconnected) + "}");
}

void HandleAdminRoomStart(SocketHandle socket, PersistentStore& store, RoomTaskManager& room_task_manager, RoomRecord& room) {
    std::string manager_error;
    if (!room_task_manager.StartRoom(room.id, room, manager_error)) {
        SendAndClose(socket, 409, ErrorJson(manager_error));
        return;
    }
    UserRecord owner;
    if (!store.GetUserByID(room.owner_id, owner)) {
        SendAndClose(socket, 404, ErrorJson("room owner not found"));
        return;
    }
    SendAndClose(socket, 200, BuildRoomActionJson(room, store.UsageForUser(owner), &store));
}

void HandleUserRoomStart(SocketHandle socket, PersistentStore& store, RoomTaskManager& room_task_manager, const UserRecord& session_user,
                         RoomRecord& room) {
    std::string manager_error;
    if (!room_task_manager.StartRoom(room.id, room, manager_error)) {
        SendAndClose(socket, 409, ErrorJson(manager_error));
        return;
    }
    SendAndClose(socket, 200, BuildRoomActionJson(room, store.UsageForUser(session_user), &store));
}

void HandleAdminRoomStop(SocketHandle socket, PersistentStore& store, StreamManager& stream_manager, RoomTaskManager& room_task_manager,
                         RoomRecord& room) {
    std::string manager_error;
    if (!room_task_manager.StopRoom(room.id, room, manager_error)) {
        SendAndClose(socket, 409, ErrorJson(manager_error));
        return;
    }
    const std::size_t disconnected = stream_manager.KickRoom("live", room.stream_name, "room stopped");
    UserRecord owner;
    if (!store.GetUserByID(room.owner_id, owner)) {
        SendAndClose(socket, 200, std::string("{\"room\":") + BuildRoomJson(room, &store) + ",\"disconnected_sessions\":" +
                                        std::to_string(disconnected) + "}");
        return;
    }
    const std::string body = BuildRoomActionJson(room, store.UsageForUser(owner), &store);
    SendAndClose(socket, 200, body.substr(0, body.size() - 1) + ",\"disconnected_sessions\":" + std::to_string(disconnected) + "}");
}

void HandleUserRoomStop(SocketHandle socket, PersistentStore& store, StreamManager& stream_manager, RoomTaskManager& room_task_manager,
                        const UserRecord& session_user, RoomRecord& room) {
    std::string manager_error;
    if (!room_task_manager.StopRoom(room.id, room, manager_error)) {
        SendAndClose(socket, 409, ErrorJson(manager_error));
        return;
    }
    const std::size_t disconnected = stream_manager.KickRoom("live", room.stream_name, "room stopped");
    const std::string body = BuildRoomActionJson(room, store.UsageForUser(session_user), &store);
    SendAndClose(socket, 200, body.substr(0, body.size() - 1) + ",\"disconnected_sessions\":" + std::to_string(disconnected) + "}");
}

}  // namespace

bool TryHandleAdminRoomsRoute(SocketHandle socket, const HttpRequest& request, PersistentStore& store, StreamManager& stream_manager,
                              RoomTaskManager& room_task_manager, const UserRecord& session_user) {
    if (request.method == "GET" && request.path == "/api/v1/admin/rooms") {
        HandleAdminListRooms(socket, request, store);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/admin/rooms") {
        HandleAdminCreateRoom(socket, request, store, session_user);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if (parts.size() < 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "admin" || parts[3] != "rooms") {
        return false;
    }

    RoomRecord room;
    if (!LoadAdminRoom(socket, store, parts[4], room)) {
        return true;
    }

    if (request.method == "POST" && parts.size() == 6 && parts[5] == "update") {
        HandleRoomUpdate(socket, request, store, room);
        return true;
    }
    if (request.method == "GET" && parts.size() == 6 && parts[5] == "logs") {
        const int offset = ParseNonNegativeInt(request.query.count("offset") == 0 ? std::string() : request.query.at("offset"), 0);
        const int limit = ParseNonNegativeInt(request.query.count("limit") == 0 ? std::string() : request.query.at("limit"), 50);
        SendAndClose(socket, 200, BuildRoomLogsJson(room, offset, limit));
        return true;
    }
    if (request.method == "DELETE" && parts.size() == 5) {
        HandleRoomDelete(socket, stream_manager, room_task_manager, room);
        return true;
    }
    if (request.method == "POST" && parts.size() == 6 && parts[5] == "start") {
        HandleAdminRoomStart(socket, store, room_task_manager, room);
        return true;
    }
    if (request.method == "POST" && parts.size() == 6 && parts[5] == "stop") {
        HandleAdminRoomStop(socket, store, stream_manager, room_task_manager, room);
        return true;
    }

    return false;
}

bool TryHandleUserRoomsRoute(SocketHandle socket, const HttpRequest& request, PersistentStore& store, StreamManager& stream_manager,
                             RoomTaskManager& room_task_manager, const UserRecord& session_user) {
    if (request.method == "GET" && request.path == "/api/v1/rooms") {
        HandleUserListRooms(socket, request, store, session_user);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/rooms") {
        HandleUserCreateRoom(socket, request, store, session_user);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if (parts.size() < 4 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "rooms") {
        return false;
    }

    RoomRecord room;
    if (!LoadUserRoom(socket, store, session_user, parts[3], room)) {
        return true;
    }

    if (request.method == "POST" && parts.size() == 5 && parts[4] == "update") {
        HandleRoomUpdate(socket, request, store, room);
        return true;
    }
    if (request.method == "GET" && parts.size() == 5 && parts[4] == "logs") {
        const int offset = ParseNonNegativeInt(request.query.count("offset") == 0 ? std::string() : request.query.at("offset"), 0);
        const int limit = ParseNonNegativeInt(request.query.count("limit") == 0 ? std::string() : request.query.at("limit"), 50);
        SendAndClose(socket, 200, BuildRoomLogsJson(room, offset, limit));
        return true;
    }
    if (request.method == "DELETE" && parts.size() == 4) {
        HandleRoomDelete(socket, stream_manager, room_task_manager, room);
        return true;
    }
    if (request.method == "POST" && parts.size() == 5 && parts[4] == "start") {
        HandleUserRoomStart(socket, store, room_task_manager, session_user, room);
        return true;
    }
    if (request.method == "POST" && parts.size() == 5 && parts[4] == "stop") {
        HandleUserRoomStop(socket, store, stream_manager, room_task_manager, session_user, room);
        return true;
    }

    return false;
}
