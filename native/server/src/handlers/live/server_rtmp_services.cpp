#include "server_rtmp_services.h"

#include "../../runtime/rooms/room_urls.h"
#include "../../shared/http/server_shared.h"

#include <ctime>
#include <iomanip>
#include <map>
#include <sstream>
#include <vector>

namespace {

struct RtmpServicePayload {
    std::string name;
    std::string source_url;
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

std::string RtmpServiceStatusText(const RtmpServiceRecord& service) {
    if (service.managed_status != "active") {
        return "disabled";
    }
    if (service.runtime_status == "running") {
        return "publishing";
    }
    return "waiting for publisher";
}

std::string BuildRtmpServiceJson(const ServerConfig& config, const RtmpServiceRecord& service) {
    std::ostringstream out;
    out << "{"
        << "\"id\":" << JsonString(service.id) << ","
        << "\"name\":" << JsonString(service.name) << ","
        << "\"owner_id\":" << JsonString(service.owner_id) << ","
        << "\"owner_name\":" << JsonString(service.owner_name) << ","
        << "\"stream_name\":" << JsonString(service.stream_name) << ","
        << "\"rtmp_url\":" << JsonString(BuildServiceUrl(config, service)) << ","
        << "\"source_url\":" << JsonString(service.source_url) << ","
        << "\"managed_status\":" << JsonString(service.managed_status) << ","
        << "\"runtime_status\":" << JsonString(service.runtime_status) << ","
        << "\"last_error\":" << JsonString(service.last_error) << ","
        << "\"created_at\":" << FormatTime(service.created_at) << ","
        << "\"updated_at\":" << FormatTime(service.updated_at) << ","
        << "\"activated_at\":" << NullableTime(service.activated_at) << ","
        << "\"stopped_at\":" << NullableTime(service.stopped_at) << ","
        << "\"last_publisher_connected_at\":" << NullableTime(service.last_publisher_connected_at) << ","
        << "\"last_publisher_disconnected_at\":" << NullableTime(service.last_publisher_disconnected_at) << ","
        << "\"log_line_count\":" << service.log_line_count << ","
        << "\"latest_log\":" << JsonString(service.latest_log) << ","
        << "\"status_text\":" << JsonString(RtmpServiceStatusText(service))
        << "}";
    return out.str();
}

bool CanAccessRtmpService(const UserRecord& user, const RtmpServiceRecord& service) {
    return user.role == "admin" || service.owner_id == user.id;
}

bool ParseRtmpServicePayload(const HttpRequest& request, RtmpServicePayload& payload, std::string& error) {
    std::map<std::string, std::string> object;
    if (!ParseSimpleJsonObject(request.body, object, error)) {
        return false;
    }
    payload.name = Trim(object["name"]);
    payload.source_url = Trim(object["source_url"]);
    if (payload.name.empty()) {
        error = "invalid rtmp service payload";
        return false;
    }
    return true;
}

bool LoadAdminService(SocketHandle socket, PersistentStore& store, const std::string& service_id, RtmpServiceRecord& service) {
    if (store.GetRtmpServiceByID(service_id, service)) {
        return true;
    }
    SendAndClose(socket, 404, ErrorJson("rtmp service not found"));
    return false;
}

bool LoadUserService(SocketHandle socket, PersistentStore& store, const UserRecord& session_user, const std::string& service_id,
                     RtmpServiceRecord& service) {
    if (store.GetRtmpServiceByID(service_id, service) && CanAccessRtmpService(session_user, service)) {
        return true;
    }
    SendAndClose(socket, 404, ErrorJson("rtmp service not found"));
    return false;
}

void HandleListAdminRtmpServices(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store) {
    const auto services = store.ListRtmpServices(true, "");
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(services.size()));
    SendAndClose(socket, 200, BuildPaginatedItemsJson(services, page, [&config](const RtmpServiceRecord& item) {
        return BuildRtmpServiceJson(config, item);
    }));
}

void HandleListUserRtmpServices(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                const UserRecord& session_user) {
    const auto services = store.ListRtmpServices(false, session_user.id);
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(services.size()));
    SendAndClose(socket, 200, BuildPaginatedItemsJson(services, page, [&config](const RtmpServiceRecord& item) {
        return BuildRtmpServiceJson(config, item);
    }));
}

void HandleCreateAdminRtmpService(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                  const UserRecord& session_user) {
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

    RtmpServicePayload payload;
    payload.name = Trim(object["name"]);
    payload.source_url = Trim(object["source_url"]);
    if (payload.name.empty()) {
        SendAndClose(socket, 400, ErrorJson("invalid rtmp service payload"));
        return;
    }

    RtmpServiceRecord service;
    std::string store_error;
    if (!store.CreateRtmpService(owner, payload.name, payload.source_url, service, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 201, BuildRtmpServiceJson(config, service));
}

void HandleCreateUserRtmpService(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                 const UserRecord& session_user) {
    RtmpServicePayload payload;
    std::string error;
    if (!ParseRtmpServicePayload(request, payload, error)) {
        SendAndClose(socket, 400, ErrorJson(error));
        return;
    }

    RtmpServiceRecord service;
    std::string store_error;
    if (!store.CreateRtmpService(session_user, payload.name, payload.source_url, service, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 201, BuildRtmpServiceJson(config, service));
}

void HandleUpdateRtmpService(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                             RtmpServiceRecord& service) {
    if (service.managed_status == "active") {
        SendAndClose(socket, 409, ErrorJson("disable the rtmp service before editing"));
        return;
    }

    RtmpServicePayload payload;
    std::string error;
    if (!ParseRtmpServicePayload(request, payload, error)) {
        SendAndClose(socket, 400, ErrorJson(error));
        return;
    }

    service.name = payload.name;
    service.source_url = payload.source_url;
    service.updated_at = NowUnix();

    std::string store_error;
    if (!store.UpdateRtmpService(service, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    if (!store.AppendRtmpServiceLog(service.id, "RTMP service updated", service, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, BuildRtmpServiceJson(config, service));
}

void HandleStartRtmpService(SocketHandle socket, const ServerConfig& config, PersistentStore& store, RtmpServiceRecord service) {
    if (service.managed_status == "active") {
        SendAndClose(socket, 409, ErrorJson("rtmp service is already active"));
        return;
    }
    const std::int64_t now = NowUnix();
    RoomStatePatch patch;
    patch.set_last_error = true;
    patch.last_error.clear();
    patch.set_activated_at = true;
    patch.activated_at = now;
    patch.set_stopped_at = true;
    patch.stopped_at = 0;
    patch.append_log = true;
    patch.log_line = "RTMP service enabled";

    std::string store_error;
    if (!store.SetRtmpServiceState(service.id, "active", "idle", patch, service, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, BuildRtmpServiceJson(config, service));
}

void HandleStopRtmpService(SocketHandle socket, const ServerConfig& config, PersistentStore& store, StreamManager& stream_manager,
                           RtmpServiceRecord service) {
    if (service.managed_status != "active") {
        SendAndClose(socket, 409, ErrorJson("rtmp service is not active"));
        return;
    }
    const std::int64_t now = NowUnix();
    RoomStatePatch patch;
    patch.set_last_error = true;
    patch.last_error.clear();
    patch.set_last_exit_at = true;
    patch.last_exit_at = now;
    patch.set_stopped_at = true;
    patch.stopped_at = now;
    patch.append_log = true;
    patch.log_line = "RTMP service disabled";

    std::string store_error;
    if (!store.SetRtmpServiceState(service.id, "inactive", "idle", patch, service, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    const std::size_t disconnected = stream_manager.KickRoom("live", service.stream_name, "rtmp service disabled");
    const std::string body = BuildRtmpServiceJson(config, service);
    SendAndClose(socket, 200, body.substr(0, body.size() - 1) + ",\"disconnected_sessions\":" + std::to_string(disconnected) + "}");
}

void HandleDeleteRtmpService(SocketHandle socket, PersistentStore& store, StreamManager& stream_manager, RtmpServiceRecord service) {
    if (service.managed_status == "active") {
        SendAndClose(socket, 409, ErrorJson("disable the rtmp service before deleting"));
        return;
    }
    std::string store_error;
    if (!store.DeleteRtmpService(service.id, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    const std::size_t disconnected = stream_manager.KickRoom("live", service.stream_name, "rtmp service deleted");
    SendAndClose(socket, 200, std::string("{\"deleted\":true,\"disconnected_sessions\":") + std::to_string(disconnected) + "}");
}

}  // namespace

bool TryHandleAdminRtmpServicesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                     StreamManager& stream_manager, const UserRecord& session_user) {
    if (request.method == "GET" && request.path == "/api/v1/admin/rtmp-services") {
        HandleListAdminRtmpServices(socket, request, config, store);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/admin/rtmp-services") {
        HandleCreateAdminRtmpService(socket, request, config, store, session_user);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if (parts.size() < 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "admin" || parts[3] != "rtmp-services") {
        return false;
    }

    RtmpServiceRecord service;
    if (!LoadAdminService(socket, store, parts[4], service)) {
        return true;
    }

    if (request.method == "POST" && parts.size() == 6 && parts[5] == "update") {
        HandleUpdateRtmpService(socket, request, config, store, service);
        return true;
    }
    if (request.method == "DELETE" && parts.size() == 5) {
        HandleDeleteRtmpService(socket, store, stream_manager, service);
        return true;
    }
    if (request.method == "POST" && parts.size() == 6 && parts[5] == "start") {
        HandleStartRtmpService(socket, config, store, service);
        return true;
    }
    if (request.method == "POST" && parts.size() == 6 && parts[5] == "stop") {
        HandleStopRtmpService(socket, config, store, stream_manager, service);
        return true;
    }

    return false;
}

bool TryHandleUserRtmpServicesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                    StreamManager& stream_manager, const UserRecord& session_user) {
    if (request.method == "GET" && request.path == "/api/v1/rtmp-services") {
        HandleListUserRtmpServices(socket, request, config, store, session_user);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/rtmp-services") {
        HandleCreateUserRtmpService(socket, request, config, store, session_user);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if (parts.size() < 4 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "rtmp-services") {
        return false;
    }

    RtmpServiceRecord service;
    if (!LoadUserService(socket, store, session_user, parts[3], service)) {
        return true;
    }

    if (request.method == "POST" && parts.size() == 5 && parts[4] == "update") {
        HandleUpdateRtmpService(socket, request, config, store, service);
        return true;
    }
    if (request.method == "DELETE" && parts.size() == 4) {
        HandleDeleteRtmpService(socket, store, stream_manager, service);
        return true;
    }
    if (request.method == "POST" && parts.size() == 5 && parts[4] == "start") {
        HandleStartRtmpService(socket, config, store, service);
        return true;
    }
    if (request.method == "POST" && parts.size() == 5 && parts[4] == "stop") {
        HandleStopRtmpService(socket, config, store, stream_manager, service);
        return true;
    }

    return false;
}
