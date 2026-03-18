#include "server_auth.h"

#include "../../shared/http/server_shared.h"
#include "../../shared/users/server_user_utils.h"

#include <map>
#include <random>
#include <sstream>

namespace {

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

bool BearerTokenFromRequest(const HttpRequest& request, std::string& token) {
    const auto it = request.headers.find("authorization");
    if (it == request.headers.end()) {
        return false;
    }
    const std::string prefix = "Bearer ";
    if (it->second.rfind(prefix, 0) != 0) {
        return false;
    }
    token = Trim(it->second.substr(prefix.size()));
    return !token.empty();
}

std::string GenerateToken() {
    static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    thread_local std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<std::size_t> dist(0, sizeof(alphabet) - 2);
    std::string out = "tok_";
    for (int i = 0; i < 32; ++i) {
        out.push_back(alphabet[dist(rng)]);
    }
    return out;
}

void HandleLogin(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store) {
    std::map<std::string, std::string> payload;
    std::string parse_error;
    if (!ParseSimpleJsonObject(request.body, payload, parse_error)) {
        SendAndClose(socket, 400, ErrorJson(parse_error));
        return;
    }

    const std::string username = Trim(payload["username"]);
    const std::string password = payload["password"];
    if (username.empty() || password.empty()) {
        SendAndClose(socket, 400, ErrorJson("username and password are required"));
        return;
    }

    UserRecord user;
    if (!store.FindUserByCredentials(username, password, user)) {
        SendAndClose(socket, 401, ErrorJson("invalid username or password"));
        return;
    }
    if (user.status != "active") {
        SendAndClose(socket, 403, ErrorJson("account is not active"));
        return;
    }
    if (user.subscription_ends_at > 0 && user.subscription_ends_at < NowUnix()) {
        SendAndClose(socket, 403, ErrorJson("subscription expired"));
        return;
    }

    AuthSessionRecord session;
    session.token = GenerateToken();
    session.user_id = user.id;
    session.created_at = NowUnix();
    session.expires_at = session.created_at + config.auth_session_ttl_seconds;

    std::string store_error;
    if (!store.CreateAuthSession(session, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error.empty() ? "failed to create auth session" : store_error));
        return;
    }

    const StorageUsage usage = store.UsageForUser(user);
    SendAndClose(socket, 200, std::string("{\"token\":") + JsonString(session.token) + ",\"user\":" + BuildUserJson(user, usage) + "}");
}

void HandleLogout(SocketHandle socket, PersistentStore& store, const std::string& session_error, bool authed, const HttpRequest& request) {
    if (!authed) {
        SendAndClose(socket, 401, ErrorJson(session_error));
        return;
    }

    std::string token;
    BearerTokenFromRequest(request, token);

    std::string store_error;
    if (!store.DeleteAuthSession(token, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, "{\"status\":\"ok\"}");
}

void HandleGetMe(SocketHandle socket, PersistentStore& store, const std::string& session_error, bool authed, const UserRecord& session_user) {
    if (!authed) {
        SendAndClose(socket, 401, ErrorJson(session_error));
        return;
    }

    const StorageUsage usage = store.UsageForUser(session_user);
    SendAndClose(socket, 200, std::string("{\"user\":") + BuildUserJson(session_user, usage) + "}");
}

void HandleChangePassword(SocketHandle socket, PersistentStore& store, const std::string& session_error, bool authed,
                          const HttpRequest& request, const UserRecord& session_user) {
    if (!authed) {
        SendAndClose(socket, 401, ErrorJson(session_error));
        return;
    }

    std::map<std::string, std::string> payload;
    std::string parse_error;
    if (!ParseSimpleJsonObject(request.body, payload, parse_error)) {
        SendAndClose(socket, 400, ErrorJson(parse_error));
        return;
    }

    const std::string current_password = payload["current_password"];
    const std::string new_password = Trim(payload["new_password"]);
    if (current_password.empty() || new_password.empty()) {
        SendAndClose(socket, 400, ErrorJson("current_password and new_password are required"));
        return;
    }

    UserRecord user;
    if (!store.GetUserByID(session_user.id, user)) {
        SendAndClose(socket, 404, ErrorJson("user not found"));
        return;
    }
    if (!store.VerifyUserPassword(user.id, current_password)) {
        SendAndClose(socket, 401, ErrorJson("current password is incorrect"));
        return;
    }

    user.password = NormalizeStoredPassword(new_password);
    std::string store_error;
    if (!store.UpdateUser(user, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, std::string("{\"status\":\"ok\",\"user\":") + BuildUserJson(user, store.UsageForUser(user)) + "}");
}

}  // namespace

bool ResolveSession(const HttpRequest& request, PersistentStore& store, UserRecord& user, std::string& error) {
    std::string token;
    if (!BearerTokenFromRequest(request, token)) {
        error = "missing bearer token";
        return false;
    }

    AuthSessionRecord session;
    std::string store_error;
    if (!store.GetAuthSession(token, session, store_error)) {
        error = store_error.empty() ? "invalid token" : store_error;
        return false;
    }
    if (!store.GetUserByID(session.user_id, user)) {
        error = "user not found";
        return false;
    }
    if (user.status != "active") {
        error = "account is not active";
        return false;
    }
    if (user.subscription_ends_at > 0 && user.subscription_ends_at < NowUnix()) {
        error = "subscription expired";
        return false;
    }
    return true;
}

bool TryHandleAuthPublicRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store) {
    if (request.method == "POST" && request.path == "/api/v1/login") {
        HandleLogin(socket, request, config, store);
        return true;
    }
    return false;
}

bool TryHandleSelfRoute(SocketHandle socket, const HttpRequest& request, PersistentStore& store, bool authed, const std::string& session_error,
                        const UserRecord& session_user) {
    if (request.method == "POST" && request.path == "/api/v1/logout") {
        HandleLogout(socket, store, session_error, authed, request);
        return true;
    }
    if (request.method == "GET" && request.path == "/api/v1/me") {
        HandleGetMe(socket, store, session_error, authed, session_user);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/me/password") {
        HandleChangePassword(socket, store, session_error, authed, request, session_user);
        return true;
    }
    return false;
}
