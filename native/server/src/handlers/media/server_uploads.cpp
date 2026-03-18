#include "server_uploads.h"

#include "server_files.h"
#include "../../shared/http/server_shared.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
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

}  // namespace

std::string BuildUploadJson(const UploadRecord& upload) {
    const bool expired = upload.status == "pending" && upload.expires_at > 0 && upload.expires_at < NowUnix();
    const std::string derived_status = expired ? "expired" : upload.status;
    const double progress = upload.total_chunks > 0 ? (100.0 * upload.uploaded_chunk_count / upload.total_chunks) : 0.0;
    std::ostringstream out;
    out << "{"
        << "\"id\":" << JsonString(upload.id) << ","
        << "\"user_id\":" << JsonString(upload.user_id) << ","
        << "\"owner_name\":" << JsonString(upload.owner_name) << ","
        << "\"original_name\":" << JsonString(upload.original_name) << ","
        << "\"content_hash\":" << JsonString(upload.content_hash) << ","
        << "\"size_bytes\":" << upload.size_bytes << ","
        << "\"chunk_size\":" << upload.chunk_size << ","
        << "\"total_chunks\":" << upload.total_chunks << ","
        << "\"uploaded_chunk_count\":" << upload.uploaded_chunk_count << ","
        << "\"progress_percent\":" << std::fixed << std::setprecision(2) << progress << ","
        << "\"status\":" << JsonString(upload.status) << ","
        << "\"derived_status\":" << JsonString(derived_status) << ","
        << "\"expired\":" << JsonBool(expired) << ","
        << "\"completed_file_id\":" << JsonString(upload.completed_file_id) << ","
        << "\"created_at\":" << FormatTime(upload.created_at) << ","
        << "\"updated_at\":" << FormatTime(upload.updated_at) << ","
        << "\"expires_at\":" << FormatTime(upload.expires_at)
        << "}";
    return out.str();
}

std::string BuildUploadSessionJson(const UploadRecord& upload) {
    std::ostringstream parts;
    parts << "[";
    for (std::size_t index = 0; index < upload.uploaded_parts.size(); ++index) {
        if (index != 0) {
            parts << ",";
        }
        parts << upload.uploaded_parts[index];
    }
    parts << "]";

    std::ostringstream out;
    out << "{"
        << "\"id\":" << JsonString(upload.id) << ","
        << "\"user_id\":" << JsonString(upload.user_id) << ","
        << "\"owner_name\":" << JsonString(upload.owner_name) << ","
        << "\"original_name\":" << JsonString(upload.original_name) << ","
        << "\"display_name\":" << JsonString(upload.display_name) << ","
        << "\"remark\":" << JsonString(upload.remark) << ","
        << "\"content_hash\":" << JsonString(upload.content_hash) << ","
        << "\"size_bytes\":" << upload.size_bytes << ","
        << "\"chunk_size\":" << upload.chunk_size << ","
        << "\"total_chunks\":" << upload.total_chunks << ","
        << "\"uploaded_parts\":" << parts.str() << ","
        << "\"status\":" << JsonString(upload.status) << ","
        << "\"completed_file_id\":" << JsonString(upload.completed_file_id) << ","
        << "\"created_at\":" << FormatTime(upload.created_at) << ","
        << "\"updated_at\":" << FormatTime(upload.updated_at) << ","
        << "\"expires_at\":" << FormatTime(upload.expires_at)
        << "}";
    return out.str();
}

std::string BuildUploadSummaryJson(const std::vector<UploadRecord>& items) {
    int pending = 0;
    int completed = 0;
    int aborted = 0;
    int expired_pending = 0;
    std::map<std::string, bool> owners;
    for (const auto& upload : items) {
        owners[upload.user_id] = true;
        if (upload.status == "pending") {
            ++pending;
            if (upload.expires_at > 0 && upload.expires_at < NowUnix()) {
                ++expired_pending;
            }
        } else if (upload.status == "completed") {
            ++completed;
        } else if (upload.status == "aborted") {
            ++aborted;
        }
    }

    std::ostringstream out;
    out << "{"
        << "\"total_sessions\":" << items.size() << ","
        << "\"filtered_sessions\":" << items.size() << ","
        << "\"pending_sessions\":" << pending << ","
        << "\"completed_sessions\":" << completed << ","
        << "\"aborted_sessions\":" << aborted << ","
        << "\"expired_pending_sessions\":" << expired_pending << ","
        << "\"filtered_pending_sessions\":" << pending << ","
        << "\"filtered_completed_sessions\":" << completed << ","
        << "\"filtered_aborted_sessions\":" << aborted << ","
        << "\"filtered_expired_pending_sessions\":" << expired_pending << ","
        << "\"unique_owner_count\":" << owners.size() << ","
        << "\"filtered_owner_count\":" << owners.size()
        << "}";
    return out.str();
}

void HandleListUploads(SocketHandle socket, const HttpRequest& request, PersistentStore& store, bool admin_scope,
                       const UserRecord& session_user) {
    const auto uploads = store.ListUploads(admin_scope, admin_scope ? std::string() : session_user.id);
    const PageSpec page = ResolvePageSpec(request.query, static_cast<int>(uploads.size()));
    const std::string extra_fields = admin_scope
                                         ? "\"filters\":{\"owner_id\":\"\",\"owner_username\":\"\",\"status\":\"\",\"expired\":\"\",\"query\":\"\"},\"summary\":" +
                                               BuildUploadSummaryJson(uploads)
                                         : std::string();
    SendAndClose(socket, 200, BuildPaginatedItemsJson(uploads, page, [](const UploadRecord& item) {
        return BuildUploadJson(item);
    }, extra_fields));
}

void HandleAbortUploadRequest(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const UserRecord& session_user,
                              const std::string& upload_id, bool admin_scope) {
    UploadRecord upload;
    if (!store.GetUploadByID(upload_id, upload) || (!admin_scope && !CanAccessUpload(session_user, upload))) {
        SendAndClose(socket, 404, ErrorJson("upload not found"));
        return;
    }

    std::string store_error;
    if (!store.AbortUpload(upload.id, upload, store_error)) {
        SendAndClose(socket, 409, ErrorJson(store_error));
        return;
    }
    if (!upload.temp_dir.empty()) {
        std::string remove_error;
        RemoveDirectoryIfExists(std::filesystem::path(config.storage_root) / upload.temp_dir, remove_error);
    }
    SendAndClose(socket, 200, std::string("{\"status\":\"aborted\",\"session\":") + BuildUploadJson(upload) + "}");
}

void HandleCleanupUploads(SocketHandle socket, const ServerConfig& config, PersistentStore& store) {
    std::string store_error;
    std::vector<UploadRecord> removed_uploads;
    const int removed = store.CleanupUploads(NowUnix(), removed_uploads, store_error);
    if (removed < 0) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    for (const auto& upload : removed_uploads) {
        if (upload.temp_dir.empty()) {
            continue;
        }
        std::string remove_error;
        RemoveDirectoryIfExists(std::filesystem::path(config.storage_root) / upload.temp_dir, remove_error);
    }
    SendAndClose(socket, 200, std::string("{\"status\":\"cleanup_completed\",\"removed_sessions\":") + std::to_string(removed) + "}");
}

void HandleInitUploadRequest(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                             const UserRecord& session_user) {
    InitUploadPayload payload;
    std::string parse_error;
    if (!ParseInitUploadPayload(request, payload, parse_error)) {
        SendAndClose(socket, 400, ErrorJson(parse_error));
        return;
    }

    const StorageUsage usage = store.UsageForUser(session_user);
    if (payload.size_bytes > usage.available_storage_bytes) {
        SendAndClose(socket, 409, ErrorJson("storage quota exceeded"));
        return;
    }

    const std::filesystem::path temp_dir =
        std::filesystem::path(config.storage_root) / "uploads" / session_user.id / GenerateStorageToken(20);
    std::string dir_error;
    if (!RemoveDirectoryIfExists(temp_dir, dir_error)) {
        SendAndClose(socket, 500, ErrorJson(dir_error));
        return;
    }
    std::error_code create_error;
    std::filesystem::create_directories(temp_dir, create_error);
    if (create_error) {
        SendAndClose(socket, 500, ErrorJson(create_error.message()));
        return;
    }

    UploadRecord upload;
    std::string store_error;
    if (!store.CreateUploadSession(session_user, payload.original_name, payload.display_name, payload.remark, payload.content_hash,
                                   payload.size_bytes, payload.chunk_size, payload.total_chunks,
                                   temp_dir.lexically_relative(std::filesystem::path(config.storage_root)).generic_string(),
                                   NowUnix() + config.upload_session_ttl_seconds,
                                   upload, store_error)) {
        std::string remove_error;
        RemoveDirectoryIfExists(temp_dir, remove_error);
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }

    SendAndClose(socket, 201, BuildUploadSessionJson(upload));
}

void HandleGetUploadRequest(SocketHandle socket, PersistentStore& store, const UserRecord& session_user, const std::string& upload_id) {
    UploadRecord upload;
    if (!store.GetUploadByID(upload_id, upload) || !CanAccessUpload(session_user, upload)) {
        SendAndClose(socket, 404, ErrorJson("upload not found"));
        return;
    }
    SendAndClose(socket, 200, BuildUploadSessionJson(upload));
}

void HandleUploadPartRequest(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                             const UserRecord& session_user, const std::string& upload_id, int part_number) {
    UploadRecord upload;
    if (!store.GetUploadByID(upload_id, upload) || !CanAccessUpload(session_user, upload)) {
        SendAndClose(socket, 404, ErrorJson("upload not found"));
        return;
    }
    if (upload.status != "pending") {
        SendAndClose(socket, 409, ErrorJson("upload is not pending"));
        return;
    }
    if (UploadSessionExpired(upload, NowUnix())) {
        SendAndClose(socket, 409, ErrorJson("upload session expired"));
        return;
    }
    if (part_number < 0 || part_number >= upload.total_chunks) {
        SendAndClose(socket, 400, ErrorJson("invalid part number"));
        return;
    }

    const std::int64_t expected_size = ExpectedUploadPartSize(upload, part_number);
    if (expected_size < 0 || static_cast<std::int64_t>(request.body.size()) != expected_size) {
        SendAndClose(socket, 400, ErrorJson("invalid part size"));
        return;
    }

    const std::filesystem::path temp_dir = upload.temp_dir.empty()
                                               ? BuildUploadTempDirForUserID(config, upload.user_id, upload.id)
                                               : (std::filesystem::path(config.storage_root) / upload.temp_dir);
    std::error_code create_error;
    std::filesystem::create_directories(temp_dir, create_error);
    if (create_error) {
        SendAndClose(socket, 500, ErrorJson(create_error.message()));
        return;
    }

    std::string write_error;
    if (!WriteBinaryFile(BuildUploadPartPath(temp_dir, part_number), request.body, write_error)) {
        SendAndClose(socket, 500, ErrorJson(write_error));
        return;
    }

    std::string store_error;
    if (!store.AddUploadPart(upload.id, part_number, NowUnix(), upload, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }
    SendAndClose(socket, 200, BuildUploadSessionJson(upload));
}

void HandleCompleteUploadRequest(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const UserRecord& session_user,
                                 const std::string& upload_id, bool admin_scope) {
    UploadRecord upload;
    if (!store.GetUploadByID(upload_id, upload) || !CanAccessUpload(session_user, upload)) {
        SendAndClose(socket, 404, ErrorJson("upload not found"));
        return;
    }
    if (upload.status != "pending") {
        SendAndClose(socket, 409, ErrorJson("upload is not pending"));
        return;
    }
    if (UploadSessionExpired(upload, NowUnix())) {
        SendAndClose(socket, 409, ErrorJson("upload session expired"));
        return;
    }
    if (static_cast<int>(upload.uploaded_parts.size()) != upload.total_chunks) {
        SendAndClose(socket, 409, ErrorJson("upload is incomplete"));
        return;
    }
    for (int expected_part = 0; expected_part < upload.total_chunks; ++expected_part) {
        if (upload.uploaded_parts[expected_part] != expected_part) {
            SendAndClose(socket, 409, ErrorJson("upload is incomplete"));
            return;
        }
    }

    const std::filesystem::path temp_dir = upload.temp_dir.empty()
                                               ? BuildUploadTempDirForUserID(config, upload.user_id, upload.id)
                                               : (std::filesystem::path(config.storage_root) / upload.temp_dir);
    const std::filesystem::path stored_path = BuildUploadStoragePathForUserID(config, upload.user_id, upload.original_name);

    std::error_code create_error;
    std::filesystem::create_directories(stored_path.parent_path(), create_error);
    if (create_error) {
        SendAndClose(socket, 500, ErrorJson(create_error.message()));
        return;
    }

    std::ofstream output(stored_path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        SendAndClose(socket, 500, ErrorJson("cannot open storage file"));
        return;
    }

    std::int64_t total_written = 0;
    for (int part_number = 0; part_number < upload.total_chunks; ++part_number) {
        const std::filesystem::path part_path = BuildUploadPartPath(temp_dir, part_number);
        std::ifstream input(part_path, std::ios::binary);
        if (!input.is_open()) {
            output.close();
            std::error_code cleanup_error;
            std::filesystem::remove(stored_path, cleanup_error);
            SendAndClose(socket, 500, ErrorJson("missing upload part"));
            return;
        }

        char buffer[8192];
        while (input.good()) {
            input.read(buffer, sizeof(buffer));
            const std::streamsize count = input.gcount();
            if (count > 0) {
                output.write(buffer, count);
                total_written += static_cast<std::int64_t>(count);
            }
        }
        if (!input.eof() || !output.good()) {
            output.close();
            std::error_code cleanup_error;
            std::filesystem::remove(stored_path, cleanup_error);
            SendAndClose(socket, 500, ErrorJson("failed to merge upload"));
            return;
        }
    }

    output.close();
    if (total_written != upload.size_bytes) {
        std::error_code cleanup_error;
        std::filesystem::remove(stored_path, cleanup_error);
        SendAndClose(socket, 409, ErrorJson("merged file size mismatch"));
        return;
    }

    std::string hash_error;
    const std::string content_hash = ComputeContentHashForFile(stored_path, hash_error);
    if (!hash_error.empty()) {
        std::error_code cleanup_error;
        std::filesystem::remove(stored_path, cleanup_error);
        SendAndClose(socket, 500, ErrorJson(hash_error));
        return;
    }
    if (!upload.content_hash.empty() &&
        (upload.content_hash.size() != content_hash.size() ||
         !std::equal(upload.content_hash.begin(), upload.content_hash.end(), content_hash.begin(), [](char left, char right) {
             return std::tolower(static_cast<unsigned char>(left)) == std::tolower(static_cast<unsigned char>(right));
         }))) {
        std::error_code cleanup_error;
        std::filesystem::remove(stored_path, cleanup_error);
        SendAndClose(socket, 409, ErrorJson("content hash mismatch"));
        return;
    }

    FileRecord file;
    std::string store_error;
    const std::string relative_path = stored_path.lexically_relative(std::filesystem::path(config.storage_root)).generic_string();
    if (!store.CompleteUpload(upload.id, relative_path, content_hash, file, upload, store_error)) {
        std::error_code cleanup_error;
        std::filesystem::remove(stored_path, cleanup_error);
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }

    std::string remove_error;
    RemoveDirectoryIfExists(temp_dir, remove_error);

    const auto rooms = store.ListRooms(admin_scope, admin_scope ? std::string() : session_user.id);
    std::ostringstream body;
    body << "{"
         << "\"file\":" << BuildFileJson(file, rooms) << ","
         << "\"upload\":" << BuildUploadJson(upload)
         << "}";
    SendAndClose(socket, 200, body.str());
}

void HandleDeleteUploadRequest(SocketHandle socket, const ServerConfig& config, PersistentStore& store, const UserRecord& session_user,
                               const std::string& upload_id) {
    UploadRecord upload;
    if (!store.GetUploadByID(upload_id, upload) || !CanAccessUpload(session_user, upload)) {
        SendAndClose(socket, 404, ErrorJson("upload not found"));
        return;
    }
    const bool pending = upload.status == "pending" && !UploadSessionExpired(upload, NowUnix());
    if (pending) {
        SendAndClose(socket, 409, ErrorJson("upload is pending"));
        return;
    }

    std::string store_error;
    if (!store.DeleteUpload(upload.id, upload, store_error)) {
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }

    std::string remove_error;
    const std::filesystem::path temp_dir = upload.temp_dir.empty()
                                               ? BuildUploadTempDirForUserID(config, upload.user_id, upload.id)
                                               : (std::filesystem::path(config.storage_root) / upload.temp_dir);
    if (!RemoveDirectoryIfExists(temp_dir, remove_error)) {
        SendAndClose(socket, 500, ErrorJson(remove_error));
        return;
    }
    SendAndClose(socket, 200, "{\"status\":\"deleted\"}");
}

void HandleUploadRequest(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                         const UserRecord& session_user, bool admin_scope) {
    const std::string original_name = Trim(request.query.count("name") != 0 ? request.query.at("name") : "");
    const std::string display_name = Trim(request.query.count("display_name") != 0 ? request.query.at("display_name") : "");
    const std::string remark = Trim(request.query.count("remark") != 0 ? request.query.at("remark") : "");
    if (original_name.empty()) {
        SendAndClose(socket, 400, ErrorJson("upload name is required"));
        return;
    }
    if (request.body.empty()) {
        SendAndClose(socket, 400, ErrorJson("upload body is empty"));
        return;
    }

    const StorageUsage usage = store.UsageForUser(session_user);
    const std::int64_t size_bytes = static_cast<std::int64_t>(request.body.size());
    if (size_bytes > usage.available_storage_bytes) {
        SendAndClose(socket, 409, ErrorJson("storage quota exceeded"));
        return;
    }

    const std::filesystem::path stored_path = BuildUploadStoragePath(config, session_user, original_name);
    std::string write_error;
    if (!WriteBinaryFile(stored_path, request.body, write_error)) {
        SendAndClose(socket, 500, ErrorJson(write_error));
        return;
    }

    const std::string relative_path = stored_path.lexically_relative(std::filesystem::path(config.storage_root)).generic_string();
    const std::string effective_display_name = display_name.empty() ? DefaultDisplayName(original_name) : display_name;
    const std::string content_hash = ComputeContentHash(request.body);
    FileRecord file;
    UploadRecord upload;
    std::string store_error;
    if (!store.CreateUploadedFile(session_user, original_name, effective_display_name, remark, relative_path, size_bytes, content_hash, file,
                                  upload, store_error)) {
        std::error_code cleanup_error;
        std::filesystem::remove(stored_path, cleanup_error);
        SendAndClose(socket, 500, ErrorJson(store_error));
        return;
    }

    const auto rooms = store.ListRooms(admin_scope, admin_scope ? std::string() : session_user.id);
    std::ostringstream body;
    body << "{"
         << "\"file\":" << BuildFileJson(file, rooms) << ","
         << "\"upload\":" << BuildUploadJson(upload)
         << "}";
    SendAndClose(socket, 201, body.str());
}

bool TryHandleAdminUploadsRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                                const UserRecord& session_user) {
    if (request.method == "POST" && request.path == "/api/v1/admin/uploads") {
        if (IsJsonRequest(request)) {
            HandleInitUploadRequest(socket, request, config, store, session_user);
        } else {
            HandleUploadRequest(socket, request, config, store, session_user, true);
        }
        return true;
    }
    if (request.method == "GET" && request.path == "/api/v1/admin/uploads") {
        HandleListUploads(socket, request, store, true, session_user);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/admin/uploads/cleanup") {
        HandleCleanupUploads(socket, config, store);
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if (request.method == "POST" && parts.size() == 6 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "admin" &&
        parts[3] == "uploads" && parts[5] == "abort") {
        HandleAbortUploadRequest(socket, config, store, session_user, parts[4], true);
        return true;
    }
    if (parts.size() == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "admin" && parts[3] == "uploads") {
        if (request.method == "GET") {
            HandleGetUploadRequest(socket, store, session_user, parts[4]);
            return true;
        }
        if (request.method == "DELETE") {
            HandleDeleteUploadRequest(socket, config, store, session_user, parts[4]);
            return true;
        }
    }
    if (request.method == "PUT" && parts.size() == 7 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "admin" &&
        parts[3] == "uploads" && parts[5] == "parts") {
        int part_number = 0;
        if (!ParseIntStrict(parts[6], part_number)) {
            SendAndClose(socket, 400, ErrorJson("invalid part number"));
            return true;
        }
        HandleUploadPartRequest(socket, request, config, store, session_user, parts[4], part_number);
        return true;
    }
    if (request.method == "POST" && parts.size() == 6 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "admin" &&
        parts[3] == "uploads" && parts[5] == "complete") {
        HandleCompleteUploadRequest(socket, config, store, session_user, parts[4], true);
        return true;
    }
    return false;
}

bool TryHandleUserUploadsRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config, PersistentStore& store,
                               const UserRecord& session_user) {
    if (request.method == "GET" && request.path == "/api/v1/uploads") {
        HandleListUploads(socket, request, store, false, session_user);
        return true;
    }
    if (request.method == "POST" && request.path == "/api/v1/uploads") {
        if (IsJsonRequest(request)) {
            HandleInitUploadRequest(socket, request, config, store, session_user);
        } else {
            HandleUploadRequest(socket, request, config, store, session_user, false);
        }
        return true;
    }

    const std::vector<std::string> parts = SplitPath(request.path);
    if (request.method == "POST" && parts.size() == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "uploads" &&
        parts[4] == "abort") {
        HandleAbortUploadRequest(socket, config, store, session_user, parts[3], false);
        return true;
    }
    if (request.method == "GET" && parts.size() == 4 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "uploads") {
        HandleGetUploadRequest(socket, store, session_user, parts[3]);
        return true;
    }
    if (request.method == "PUT" && parts.size() == 6 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "uploads" &&
        parts[4] == "parts") {
        int part_number = 0;
        if (!ParseIntStrict(parts[5], part_number)) {
            SendAndClose(socket, 400, ErrorJson("invalid part number"));
            return true;
        }
        HandleUploadPartRequest(socket, request, config, store, session_user, parts[3], part_number);
        return true;
    }
    if (request.method == "POST" && parts.size() == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "uploads" &&
        parts[4] == "complete") {
        HandleCompleteUploadRequest(socket, config, store, session_user, parts[3], false);
        return true;
    }
    if (request.method == "DELETE" && parts.size() == 4 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "uploads") {
        HandleDeleteUploadRequest(socket, config, store, session_user, parts[3]);
        return true;
    }
    return false;
}
