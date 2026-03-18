#pragma once

#include "../../config/config.h"
#include "../../net/net.h"
#include "../../storage/persistence/store.h"
#include "../model/server_types.h"

#include <cstdint>
#include <filesystem>
#include <map>
#include <sstream>
#include <string>
#include <vector>

struct PageSpec {
    int total = 0;
    int offset = 0;
    int limit = 0;
    int end = 0;
};

struct InitUploadPayload {
    std::string original_name;
    std::string display_name;
    std::string remark;
    std::string content_hash;
    std::int64_t size_bytes = 0;
    std::int64_t chunk_size = 0;
    int total_chunks = 0;
};

std::string ToLower(std::string value);
std::string Trim(const std::string& value);
int ParseNonNegativeInt(const std::string& value, int fallback);
PageSpec ResolvePageSpec(const std::map<std::string, std::string>& query, int total);
bool ParseInt64Strict(const std::string& value, std::int64_t& out);
bool ParseIntStrict(const std::string& value, int& out);
std::vector<std::string> SplitPath(const std::string& path);
std::string ErrorJson(const std::string& error);
bool SendResponse(SocketHandle socket, int status, const std::string& body);
void SendAndClose(SocketHandle socket, int status, const std::string& body);
bool SendFileResponse(SocketHandle socket, const std::filesystem::path& path, const std::string& method,
                      const std::map<std::string, std::string>& headers);
void SendFileAndClose(SocketHandle socket, const std::filesystem::path& path, const std::string& method,
                      const std::map<std::string, std::string>& headers);
bool ReadHttpRequest(SocketHandle socket, HttpRequest& request);
bool ParseSimpleJsonObject(const std::string& body, std::map<std::string, std::string>& out, std::string& error);
std::int64_t NowUnix();
std::string GenerateStorageToken(std::size_t length);
std::string ComputeContentHash(const std::string& data);
std::string DefaultDisplayName(const std::string& original_name);
std::filesystem::path BuildUploadStoragePath(const ServerConfig& config, const UserRecord& user, const std::string& original_name);
std::filesystem::path BuildUploadStoragePathForUserID(const ServerConfig& config, const std::string& user_id,
                                                      const std::string& original_name);
std::filesystem::path BuildUploadTempDirForUserID(const ServerConfig& config, const std::string& user_id,
                                                  const std::string& upload_id);
std::filesystem::path BuildUploadPartPath(const std::filesystem::path& temp_dir, int part_number);
bool WriteBinaryFile(const std::filesystem::path& path, const std::string& body, std::string& error);
bool RemoveDirectoryIfExists(const std::filesystem::path& path, std::string& error);
std::string ComputeContentHashForFile(const std::filesystem::path& path, std::string& error);
bool CanAccessUpload(const UserRecord& user, const UploadRecord& upload);
bool IsJsonRequest(const HttpRequest& request);
bool ParseInitUploadPayload(const HttpRequest& request, InitUploadPayload& payload, std::string& error);
bool UploadSessionExpired(const UploadRecord& upload, std::int64_t now);
std::int64_t ExpectedUploadPartSize(const UploadRecord& upload, int part_number);

template <typename T, typename Fn>
std::string BuildPaginatedItemsJson(const std::vector<T>& items, const PageSpec& page, Fn&& fn,
                                    const std::string& extra_fields = std::string()) {
    std::ostringstream out;
    out << "{"
        << "\"total\":" << page.total << ","
        << "\"offset\":" << page.offset << ","
        << "\"limit\":" << page.limit << ","
        << "\"items\":[";
    for (int index = page.offset; index < page.end; ++index) {
        if (index != page.offset) {
            out << ",";
        }
        out << fn(items[static_cast<std::size_t>(index)]);
    }
    out << "]";
    if (!extra_fields.empty()) {
        out << "," << extra_fields;
    }
    out << "}";
    return out.str();
}
