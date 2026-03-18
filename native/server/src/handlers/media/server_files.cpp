#include "server_files.h"

#include "../../shared/http/server_shared.h"

#include <filesystem>
#include <map>
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

int ActiveRoomCountForFile(const FileRecord& file, const std::vector<RoomRecord>& rooms) {
    int count = 0;
    for (const auto& room : rooms) {
        if (room.file_id == file.id && room.managed_status == "active") {
            ++count;
        }
    }
    return count;
}

bool CanAccessFile(const UserRecord& user, const FileRecord& file) {
    return user.role == "admin" || file.user_id == user.id;
}

std::filesystem::path ResolveStoredFilePath(const ServerConfig& config, const FileRecord& file) {
    if (file.stored_path.empty()) {
        return std::filesystem::path(config.storage_root) / file.id / file.original_name;
    }
    const std::filesystem::path stored(file.stored_path);
    if (stored.is_absolute()) {
        return stored;
    }
    return std::filesystem::path(config.storage_root) / stored;
}

std::string BuildStorageUsageJson(const StorageUsage& usage) {
    std::ostringstream out;
    out << "{"
        << "\"used_storage_bytes\":" << usage.used_storage_bytes << ","
        << "\"reserved_storage_bytes\":" << usage.reserved_storage_bytes << ","
        << "\"max_storage_bytes\":" << usage.max_storage_bytes << ","
        << "\"available_storage_bytes\":" << usage.available_storage_bytes << ","
        << "\"active_room_count\":" << usage.active_room_count << ","
        << "\"max_active_rooms\":" << usage.max_active_rooms
        << "}";
    return out.str();
}

std::string BuildFileSummaryJson(const std::vector<FileRecord>& items, const std::vector<RoomRecord>& rooms) {
    std::int64_t total_size = 0;
    int in_use_files = 0;
    std::map<std::string, bool> owners;
    for (const auto& file : items) {
        total_size += file.size_bytes;
        owners[file.user_id] = true;
        if (ActiveRoomCountForFile(file, rooms) > 0) {
            ++in_use_files;
        }
    }
    std::ostringstream out;
    out << "{"
        << "\"total_files\":" << items.size() << ","
        << "\"filtered_files\":" << items.size() << ","
        << "\"total_size_bytes\":" << total_size << ","
        << "\"filtered_size_bytes\":" << total_size << ","
        << "\"in_use_files\":" << in_use_files << ","
        << "\"filtered_in_use_files\":" << in_use_files << ","
        << "\"unique_owner_count\":" << owners.size() << ","
        << "\"filtered_owner_count\":" << owners.size()
        << "}";
    return out.str();
}

}  // namespace

std::string BuildFileJson(const FileRecord& file, const std::vector<RoomRecord>& all_rooms) {
    const int active_room_count = ActiveRoomCountForFile(file, all_rooms);
    std::ostringstream out;
    out << "{"
        << "\"id\":" << JsonString(file.id) << ","
        << "\"user_id\":" << JsonString(file.user_id) << ","
        << "\"owner_name\":" << JsonString(file.owner_name) << ","
        << "\"original_name\":" << JsonString(file.original_name) << ","
        << "\"display_name\":" << JsonString(file.display_name) << ","
        << "\"remark\":" << JsonString(file.remark) << ","
        << "\"size_bytes\":" << file.size_bytes << ","
        << "\"content_hash\":" << JsonString(file.content_hash) << ","
        << "\"status\":" << JsonString(file.status) << ","
        << "\"created_at\":" << FormatTime(file.created_at) << ","
        << "\"updated_at\":" << FormatTime(file.updated_at) << ","
        << "\"active_room_count\":" << active_room_count << ","
        << "\"in_use\":" << JsonBool(active_room_count > 0)
        << "}";
    return out.str();
}

void HandleAdminListFiles(SocketHandle socket, const HttpRequest& request, const ServerConfig&, PersistentStore& store) {
    const auto rooms = store.ListRooms(true, "");
    const auto files = store.ListFiles(true, "");
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(files.size()));
    SendAndClose(socket, 200, BuildPaginatedItemsJson(files, page, [&rooms](const FileRecord& item) {
        return BuildFileJson(item, rooms);
    }, "\"filters\":{\"owner_id\":\"\",\"owner_username\":\"\",\"status\":\"\",\"in_use\":\"\",\"query\":\"\"},\"summary\":" +
            BuildFileSummaryJson(files, rooms)));
}

void HandleAdminPreviewFile(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                            const std::string& file_id) {
    FileRecord file;
    if (!store.GetFileByID(file_id, file)) {
        SendAndClose(socket, 404, ErrorJson("file not found"));
        return;
    }
    const std::filesystem::path stored_path = ResolveStoredFilePath(config, file);
    if (!std::filesystem::exists(stored_path)) {
        SendAndClose(socket, 404, ErrorJson("file not found"));
        return;
    }
    SendFileAndClose(socket, stored_path, request.method, request.headers);
}

void HandleAdminDeleteFile(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const std::string& file_id) {
    FileRecord file;
    if (!store.GetFileByID(file_id, file)) {
        SendAndClose(socket, 404, ErrorJson("file not found"));
        return;
    }
    std::string store_error;
    if (!store.RemoveFileRecord(file.id, file, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    const std::filesystem::path stored_path = ResolveStoredFilePath(config, file);
    std::error_code remove_error;
    std::filesystem::remove(stored_path, remove_error);
    SendAndClose(socket, 200, "{\"deleted\":true}");
}

void HandleUserListFiles(SocketHandle socket, const HttpRequest& request, const ServerConfig&, PersistentStore& store,
                         const UserRecord& session_user) {
    const auto all_rooms = store.ListRooms(true, "");
    const auto files = store.ListFiles(false, session_user.id);
    const StorageUsage usage = store.UsageForUser(session_user);
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(files.size()));
    SendAndClose(socket, 200, BuildPaginatedItemsJson(files, page, [&all_rooms](const FileRecord& item) {
        return BuildFileJson(item, all_rooms);
    }, "\"usage\":" + BuildStorageUsageJson(usage)));
}

void HandleUserPreviewFile(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                           const UserRecord& session_user, const std::string& file_id) {
    FileRecord file;
    if (!store.GetFileByID(file_id, file) || !CanAccessFile(session_user, file)) {
        SendAndClose(socket, 404, ErrorJson("file not found"));
        return;
    }
    const std::filesystem::path stored_path = ResolveStoredFilePath(config, file);
    if (!std::filesystem::exists(stored_path)) {
        SendAndClose(socket, 404, ErrorJson("file not found"));
        return;
    }
    SendFileAndClose(socket, stored_path, request.method, request.headers);
}

void HandleUserDeleteFile(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const UserRecord& session_user,
                          const std::string& file_id) {
    FileRecord file;
    if (!store.GetFileByID(file_id, file) || !CanAccessFile(session_user, file)) {
        SendAndClose(socket, 404, ErrorJson("file not found"));
        return;
    }
    std::string store_error;
    if (!store.RemoveFileRecord(file.id, file, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    const std::filesystem::path stored_path = ResolveStoredFilePath(config, file);
    std::error_code remove_error;
    std::filesystem::remove(stored_path, remove_error);
    SendAndClose(socket, 200, "{\"deleted\":true}");
}

bool TryHandleAdminFilesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store) {
    if (request.method == "GET" && request.path == "/api/v1/admin/files") {
        HandleAdminListFiles(socket, request, config, store);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if ((request.method == "GET" || request.method == "HEAD") && parts.size() == 6 && parts[0] == "api" && parts[1] == "v1" &&
        parts[2] == "admin" && parts[3] == "files" && parts[5] == "preview") {
        HandleAdminPreviewFile(socket, request, config, store, parts[4]);
        return true;
    }
    if (request.method == "DELETE" && parts.size() == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "admin" &&
        parts[3] == "files") {
        HandleAdminDeleteFile(socket, config, store, parts[4]);
        return true;
    }
    return false;
}

bool TryHandleUserFilesRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                             const UserRecord& session_user) {
    if (request.method == "GET" && request.path == "/api/v1/files") {
        HandleUserListFiles(socket, request, config, store, session_user);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if ((request.method == "GET" || request.method == "HEAD") && parts.size() == 5 && parts[0] == "api" && parts[1] == "v1" &&
        parts[2] == "files" && parts[4] == "preview") {
        HandleUserPreviewFile(socket, request, config, store, session_user, parts[3]);
        return true;
    }
    if (request.method == "DELETE" && parts.size() == 4 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "files") {
        HandleUserDeleteFile(socket, config, store, session_user, parts[3]);
        return true;
    }
    return false;
}
