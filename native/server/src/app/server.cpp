#include "server.h"

#include "../net/net.h"
#include "../handlers/admin/server_admin_monitoring.h"
#include "../handlers/auth/server_auth.h"
#include "../handlers/media/server_files.h"
#include "../handlers/live/server_rooms.h"
#include "../handlers/live/server_rtmp_services.h"
#include "../handlers/media/server_uploads.h"
#include "../handlers/admin/server_users.h"
#include "../shared/http/server_shared.h"
#include "../shared/users/server_user_utils.h"

#include <thread>

namespace {

class RequestCounterGuard {
public:
    explicit RequestCounterGuard(std::atomic<int>& counter) : counter_(counter) { ++counter_; }
    ~RequestCounterGuard() { --counter_; }

private:
    std::atomic<int>& counter_;
};

void HandleClient(SocketHandle socket, const ServerConfig& config, PersistentStore& store, StreamManager& stream_manager,
                  RoomTaskManager& room_task_manager, std::int64_t started_at, std::atomic<int>& active_requests) {
    RequestCounterGuard guard(active_requests);

    HttpRequest request;
    if (!ReadHttpRequest(socket, request)) {
        net::Close(socket);
        return;
    }

    if (request.method == "GET" && request.path == "/healthz") {
        SendAndClose(socket, 200, "{\"status\":\"ok\"}");
        return;
    }
    if (request.method == "GET" && request.path == "/readyz") {
        SendAndClose(socket, 200, "{\"status\":\"ready\"}");
        return;
    }

    if (TryHandleAuthPublicRoute(socket, request, config, store)) {
        return;
    }

    UserRecord session_user;
    std::string session_error;
    const bool authed = ResolveSession(request, store, session_user, session_error);
    const bool admin = authed && session_user.role == "admin";

    if (TryHandleSelfRoute(socket, request, store, authed, session_error, session_user)) {
        return;
    }

    if (request.path.rfind("/api/v1/admin/", 0) == 0) {
        if (!authed) {
            SendAndClose(socket, 401, ErrorJson(session_error));
            return;
        }
        if (!admin) {
            SendAndClose(socket, 403, ErrorJson("admin role required"));
            return;
        }

        if (TryHandleAdminMonitoringRoute(socket, request, config, store, stream_manager, started_at)) {
            return;
        }
        if (TryHandleAdminUsersRoute(socket, request, store, stream_manager, room_task_manager, session_user)) {
            return;
        }
        if (TryHandleAdminRtmpServicesRoute(socket, request, config, store, stream_manager, session_user)) {
            return;
        }
        if (TryHandleAdminRoomsRoute(socket, request, store, stream_manager, room_task_manager, session_user)) {
            return;
        }
        if (TryHandleAdminFilesRoute(socket, request, config, store)) {
            return;
        }
        if (TryHandleAdminUploadsRoute(socket, request, config, store, session_user)) {
            return;
        }
    }

    if (request.path.rfind("/api/v1/", 0) == 0) {
        if (!authed) {
            SendAndClose(socket, 401, ErrorJson(session_error));
            return;
        }
        if (TryHandleUserRoomsRoute(socket, request, store, stream_manager, room_task_manager, session_user)) {
            return;
        }
        if (TryHandleUserRtmpServicesRoute(socket, request, config, store, stream_manager, session_user)) {
            return;
        }
        if (TryHandleUserFilesRoute(socket, request, config, store, session_user)) {
            return;
        }
        if (TryHandleUserUploadsRoute(socket, request, config, store, session_user)) {
            return;
        }
    }

    SendAndClose(socket, 404, ErrorJson("not found"));
}

}  // namespace

NativeServer::NativeServer(const ServerConfig& config, PersistentStore& store, StreamManager& stream_manager, RoomTaskManager& room_task_manager)
    : config_(config), store_(store), stream_manager_(stream_manager), room_task_manager_(room_task_manager), started_at_(NowUnix()) {}

bool NativeServer::Run(std::string& error) {
    SocketHandle listener = kInvalidSocket;
    if (!net::CreateListener(config_.host, config_.port, listener, error)) {
        return false;
    }

    while (true) {
        SocketHandle client = kInvalidSocket;
        std::string client_ip;
        if (!net::Accept(listener, client, client_ip, error)) {
            net::Close(listener);
            return false;
        }
        std::thread([client, config = config_, this]() {
            HandleClient(client, config, store_, stream_manager_, room_task_manager_, started_at_, active_requests_);
        }).detach();
    }
}
