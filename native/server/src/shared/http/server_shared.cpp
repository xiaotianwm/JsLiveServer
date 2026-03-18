#include "server_shared.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <fstream>
#include <iomanip>
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

std::string StatusText(int status) {
    switch (status) {
        case 200:
            return "OK";
        case 201:
            return "Created";
        case 206:
            return "Partial Content";
        case 400:
            return "Bad Request";
        case 401:
            return "Unauthorized";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 409:
            return "Conflict";
        case 416:
            return "Range Not Satisfiable";
        default:
            return "Error";
    }
}

int HexValue(char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'A' && ch <= 'F') {
        return 10 + (ch - 'A');
    }
    if (ch >= 'a' && ch <= 'f') {
        return 10 + (ch - 'a');
    }
    return -1;
}

std::string UrlDecode(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '%' && i + 2 < value.size()) {
            const int hi = HexValue(value[i + 1]);
            const int lo = HexValue(value[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
                continue;
            }
        }
        out.push_back(value[i] == '+' ? ' ' : value[i]);
    }
    return out;
}

std::map<std::string, std::string> ParseQueryString(const std::string& query) {
    std::map<std::string, std::string> out;
    std::size_t start = 0;
    while (start <= query.size()) {
        const std::size_t amp = query.find('&', start);
        const std::string part = query.substr(start, amp == std::string::npos ? std::string::npos : amp - start);
        if (!part.empty()) {
            const std::size_t eq = part.find('=');
            if (eq == std::string::npos) {
                out[UrlDecode(part)] = "";
            } else {
                out[UrlDecode(part.substr(0, eq))] = UrlDecode(part.substr(eq + 1));
            }
        }
        if (amp == std::string::npos) {
            break;
        }
        start = amp + 1;
    }
    return out;
}

std::string GuessPreviewContentType(const std::filesystem::path& path) {
    const std::string extension = ToLower(path.extension().string());
    if (extension == ".mp4" || extension == ".m4v") {
        return "video/mp4";
    }
    if (extension == ".mov") {
        return "video/quicktime";
    }
    if (extension == ".mkv") {
        return "video/x-matroska";
    }
    if (extension == ".webm") {
        return "video/webm";
    }
    if (extension == ".flv") {
        return "video/x-flv";
    }
    if (extension == ".ts") {
        return "video/mp2t";
    }
    if (extension == ".mp3") {
        return "audio/mpeg";
    }
    if (extension == ".wav") {
        return "audio/wav";
    }
    if (extension == ".aac") {
        return "audio/aac";
    }
    if (extension == ".jpg" || extension == ".jpeg") {
        return "image/jpeg";
    }
    if (extension == ".png") {
        return "image/png";
    }
    if (extension == ".gif") {
        return "image/gif";
    }
    if (extension == ".txt") {
        return "text/plain; charset=utf-8";
    }
    return "application/octet-stream";
}

bool ParseRangeHeader(const std::string& value, std::uintmax_t file_size, std::uintmax_t& start, std::uintmax_t& end) {
    const auto parse_int64 = [](const std::string& text, std::int64_t& out) {
        try {
            std::size_t consumed = 0;
            const std::int64_t parsed = std::stoll(text, &consumed, 10);
            if (consumed != text.size()) {
                return false;
            }
            out = parsed;
            return true;
        } catch (...) {
            return false;
        }
    };

    const std::string trimmed = Trim(value);
    if (trimmed.rfind("bytes=", 0) != 0) {
        return false;
    }

    const std::string spec = Trim(trimmed.substr(6));
    const std::size_t dash = spec.find('-');
    if (dash == std::string::npos || spec.find(',') != std::string::npos) {
        return false;
    }

    const std::string left = Trim(spec.substr(0, dash));
    const std::string right = Trim(spec.substr(dash + 1));
    if (left.empty() && right.empty()) {
        return false;
    }

    if (left.empty()) {
        std::int64_t suffix = 0;
        if (!parse_int64(right, suffix) || suffix <= 0 || file_size == 0) {
            return false;
        }
        const std::uintmax_t suffix_size = static_cast<std::uintmax_t>(suffix);
        start = suffix_size >= file_size ? 0 : (file_size - suffix_size);
        end = file_size - 1;
        return true;
    }

    std::int64_t parsed_start = 0;
    if (!parse_int64(left, parsed_start) || parsed_start < 0) {
        return false;
    }
    start = static_cast<std::uintmax_t>(parsed_start);
    if (start >= file_size) {
        return false;
    }

    if (right.empty()) {
        end = file_size - 1;
        return true;
    }

    std::int64_t parsed_end = 0;
    if (!parse_int64(right, parsed_end) || parsed_end < 0) {
        return false;
    }
    end = static_cast<std::uintmax_t>(parsed_end);
    if (end < start) {
        return false;
    }
    if (end >= file_size) {
        end = file_size - 1;
    }
    return true;
}

void SkipWs(const std::string& body, std::size_t& pos) {
    while (pos < body.size() && std::isspace(static_cast<unsigned char>(body[pos])) != 0) {
        ++pos;
    }
}

bool ParseJsonStringToken(const std::string& body, std::size_t& pos, std::string& out) {
    if (pos >= body.size() || body[pos] != '"') {
        return false;
    }
    ++pos;
    out.clear();
    while (pos < body.size()) {
        const char ch = body[pos++];
        if (ch == '\\') {
            if (pos >= body.size()) {
                return false;
            }
            const char escaped = body[pos++];
            switch (escaped) {
                case '"':
                case '\\':
                case '/':
                    out.push_back(escaped);
                    break;
                case 'n':
                    out.push_back('\n');
                    break;
                case 'r':
                    out.push_back('\r');
                    break;
                case 't':
                    out.push_back('\t');
                    break;
                default:
                    out.push_back(escaped);
                    break;
            }
            continue;
        }
        if (ch == '"') {
            return true;
        }
        out.push_back(ch);
    }
    return false;
}

bool ParseJsonPrimitiveToken(const std::string& body, std::size_t& pos, std::string& out) {
    const std::size_t start = pos;
    while (pos < body.size()) {
        const char ch = body[pos];
        if (ch == ',' || ch == '}' || std::isspace(static_cast<unsigned char>(ch)) != 0) {
            break;
        }
        ++pos;
    }
    if (pos == start) {
        return false;
    }
    out = body.substr(start, pos - start);
    return true;
}

std::string SanitizeFileExtension(const std::string& original_name) {
    std::filesystem::path path(original_name);
    std::string extension = path.extension().string();
    if (extension.empty() || extension.size() > 16) {
        return ".bin";
    }
    for (char& ch : extension) {
        if (std::isalnum(static_cast<unsigned char>(ch)) == 0 && ch != '.') {
            return ".bin";
        }
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return extension;
}

}  // namespace

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string Trim(const std::string& value) {
    std::size_t start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
        ++start;
    }
    std::size_t end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return value.substr(start, end - start);
}

int ParseNonNegativeInt(const std::string& value, int fallback) {
    if (value.empty()) {
        return fallback;
    }
    try {
        const int parsed = std::stoi(value);
        return parsed < 0 ? fallback : parsed;
    } catch (...) {
        return fallback;
    }
}

PageSpec ResolvePageSpec(const std::map<std::string, std::string>& query, int total) {
    const auto offset_it = query.find("offset");
    const auto limit_it = query.find("limit");
    const bool has_limit = limit_it != query.end();

    int offset = ParseNonNegativeInt(offset_it == query.end() ? std::string() : offset_it->second, 0);
    int limit = has_limit ? ParseNonNegativeInt(limit_it->second, 50) : total;
    if (has_limit && limit <= 0) {
        limit = 50;
    }
    if (!has_limit) {
        limit = total;
    }
    if (total <= 0) {
        return {};
    }
    if (limit <= 0) {
        limit = total;
    }
    if (offset >= total) {
        offset = ((total - 1) / limit) * limit;
    }

    PageSpec spec;
    spec.total = total;
    spec.offset = offset;
    spec.limit = limit;
    spec.end = std::min(total, offset + limit);
    return spec;
}

bool ParseInt64Strict(const std::string& value, std::int64_t& out) {
    try {
        std::size_t consumed = 0;
        const std::int64_t parsed = std::stoll(value, &consumed, 10);
        if (consumed != value.size()) {
            return false;
        }
        out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseIntStrict(const std::string& value, int& out) {
    try {
        std::size_t consumed = 0;
        const int parsed = std::stoi(value, &consumed, 10);
        if (consumed != value.size()) {
            return false;
        }
        out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start < path.size()) {
        while (start < path.size() && path[start] == '/') {
            ++start;
        }
        if (start >= path.size()) {
            break;
        }
        const std::size_t slash = path.find('/', start);
        parts.push_back(path.substr(start, slash == std::string::npos ? std::string::npos : slash - start));
        if (slash == std::string::npos) {
            break;
        }
        start = slash + 1;
    }
    return parts;
}

std::string ErrorJson(const std::string& error) {
    return std::string("{\"error\":") + JsonString(error) + "}";
}

bool SendResponse(SocketHandle socket, int status, const std::string& body) {
    const std::string response =
        "HTTP/1.1 " + std::to_string(status) + " " + StatusText(status) + "\r\n"
        "Content-Type: application/json; charset=utf-8\r\n"
        "Connection: close\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
    return net::SendAll(socket, reinterpret_cast<const std::uint8_t*>(response.data()), response.size());
}

void SendAndClose(SocketHandle socket, int status, const std::string& body) {
    SendResponse(socket, status, body);
    net::Close(socket);
}

bool SendFileResponse(SocketHandle socket, const std::filesystem::path& path, const std::string& method,
                      const std::map<std::string, std::string>& headers) {
    std::error_code size_error;
    const std::uintmax_t file_size = std::filesystem::file_size(path, size_error);
    if (size_error) {
        return false;
    }

    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        return false;
    }

    int status = 200;
    std::uintmax_t start = 0;
    std::uintmax_t end = file_size == 0 ? 0 : file_size - 1;
    const auto range_it = headers.find("range");
    if (range_it != headers.end() && !range_it->second.empty()) {
        if (!ParseRangeHeader(range_it->second, file_size, start, end)) {
            const std::string invalid_header =
                "HTTP/1.1 416 Range Not Satisfiable\r\n"
                "Content-Range: bytes */" + std::to_string(file_size) + "\r\n"
                "Connection: close\r\n"
                "Content-Length: 0\r\n\r\n";
            return net::SendAll(socket, reinterpret_cast<const std::uint8_t*>(invalid_header.data()), invalid_header.size());
        }
        status = 206;
    }

    const std::uintmax_t content_length = file_size == 0 ? 0 : (end - start + 1);
    const std::string header =
        "HTTP/1.1 " + std::to_string(status) + " " + StatusText(status) + "\r\n"
        "Content-Type: " + GuessPreviewContentType(path) + "\r\n"
        "Accept-Ranges: bytes\r\n"
        "Connection: close\r\n"
        "Content-Length: " + std::to_string(content_length) + "\r\n" +
        (status == 206 ? "Content-Range: bytes " + std::to_string(start) + "-" + std::to_string(end) + "/" +
                             std::to_string(file_size) + "\r\n"
                       : std::string()) +
        "\r\n";
    if (!net::SendAll(socket, reinterpret_cast<const std::uint8_t*>(header.data()), header.size())) {
        return false;
    }

    if (method == "HEAD" || file_size == 0) {
        return true;
    }

    input.seekg(static_cast<std::streamoff>(start), std::ios::beg);
    if (!input.good()) {
        return false;
    }

    char buffer[64 * 1024];
    std::uintmax_t remaining = content_length;
    while (input.good() && remaining > 0) {
        const std::size_t chunk_size = static_cast<std::size_t>(std::min<std::uintmax_t>(remaining, sizeof(buffer)));
        input.read(buffer, static_cast<std::streamsize>(chunk_size));
        const std::streamsize count = input.gcount();
        if (count <= 0) {
            break;
        }
        if (!net::SendAll(socket, reinterpret_cast<const std::uint8_t*>(buffer), static_cast<std::size_t>(count))) {
            return false;
        }
        remaining -= static_cast<std::uintmax_t>(count);
    }
    return remaining == 0;
}

void SendFileAndClose(SocketHandle socket, const std::filesystem::path& path, const std::string& method,
                      const std::map<std::string, std::string>& headers) {
    SendFileResponse(socket, path, method, headers);
    net::Close(socket);
}

bool ReadHttpRequest(SocketHandle socket, HttpRequest& request) {
    std::string raw;
    std::uint8_t buffer[2048];
    while (raw.find("\r\n\r\n") == std::string::npos) {
        const int received = net::RecvSome(socket, buffer, sizeof(buffer));
        if (received <= 0) {
            return false;
        }
        raw.append(reinterpret_cast<const char*>(buffer), static_cast<std::size_t>(received));
        if (raw.size() > 2 * 1024 * 1024) {
            return false;
        }
    }

    const std::size_t header_end = raw.find("\r\n\r\n");
    const std::string header_blob = raw.substr(0, header_end);
    request.body = raw.substr(header_end + 4);

    std::istringstream stream(header_blob);
    std::string request_line;
    if (!std::getline(stream, request_line)) {
        return false;
    }
    if (!request_line.empty() && request_line.back() == '\r') {
        request_line.pop_back();
    }
    std::istringstream request_parser(request_line);
    request_parser >> request.method >> request.target >> request.version;
    if (request.method.empty() || request.target.empty()) {
        return false;
    }

    std::size_t content_length = 0;
    std::string line;
    while (std::getline(stream, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        const std::size_t colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        const std::string key = ToLower(Trim(line.substr(0, colon)));
        const std::string value = Trim(line.substr(colon + 1));
        request.headers[key] = value;
        if (key == "content-length") {
            content_length = static_cast<std::size_t>(std::stoul(value));
        }
    }

    while (request.body.size() < content_length) {
        const int received = net::RecvSome(socket, buffer, sizeof(buffer));
        if (received <= 0) {
            return false;
        }
        request.body.append(reinterpret_cast<const char*>(buffer), static_cast<std::size_t>(received));
    }
    if (request.body.size() > content_length) {
        request.body.resize(content_length);
    }

    request.path = request.target;
    const std::size_t qm = request.target.find('?');
    if (qm != std::string::npos) {
        request.path = request.target.substr(0, qm);
        request.query = ParseQueryString(request.target.substr(qm + 1));
    }
    return true;
}

bool ParseSimpleJsonObject(const std::string& body, std::map<std::string, std::string>& out, std::string& error) {
    out.clear();
    std::size_t pos = 0;
    SkipWs(body, pos);
    if (pos >= body.size() || body[pos] != '{') {
        error = "expected JSON object";
        return false;
    }
    ++pos;
    while (true) {
        SkipWs(body, pos);
        if (pos >= body.size()) {
            error = "unexpected end of body";
            return false;
        }
        if (body[pos] == '}') {
            ++pos;
            return true;
        }
        std::string key;
        if (!ParseJsonStringToken(body, pos, key)) {
            error = "invalid JSON key";
            return false;
        }
        SkipWs(body, pos);
        if (pos >= body.size() || body[pos] != ':') {
            error = "expected ':'";
            return false;
        }
        ++pos;
        SkipWs(body, pos);
        std::string value;
        if (pos < body.size() && body[pos] == '"') {
            if (!ParseJsonStringToken(body, pos, value)) {
                error = "invalid JSON string value";
                return false;
            }
        } else if (!ParseJsonPrimitiveToken(body, pos, value)) {
            error = "unsupported JSON value";
            return false;
        }
        out[key] = value;
        SkipWs(body, pos);
        if (pos >= body.size()) {
            error = "unexpected end of body";
            return false;
        }
        if (body[pos] == ',') {
            ++pos;
            continue;
        }
        if (body[pos] == '}') {
            ++pos;
            return true;
        }
        error = "expected ',' or '}'";
        return false;
    }
}

std::int64_t NowUnix() {
    return static_cast<std::int64_t>(std::time(nullptr));
}

std::string GenerateStorageToken(std::size_t length) {
    static const char alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    thread_local std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<std::size_t> dist(0, sizeof(alphabet) - 2);
    std::string out;
    out.reserve(length);
    for (std::size_t i = 0; i < length; ++i) {
        out.push_back(alphabet[dist(rng)]);
    }
    return out;
}

std::string ComputeContentHash(const std::string& data) {
    std::uint64_t hash = 1469598103934665603ull;
    for (unsigned char ch : data) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= 1099511628211ull;
    }
    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(16) << hash;
    return out.str();
}

std::string DefaultDisplayName(const std::string& original_name) {
    std::filesystem::path path(original_name);
    const std::string stem = path.stem().string();
    return stem.empty() ? original_name : stem;
}

std::filesystem::path BuildUploadStoragePath(const ServerConfig& config, const UserRecord& user, const std::string& original_name) {
    const std::string extension = SanitizeFileExtension(original_name);
    const std::string filename = GenerateStorageToken(20) + extension;
    return std::filesystem::path(config.storage_root) / user.id / filename;
}

std::filesystem::path BuildUploadStoragePathForUserID(const ServerConfig& config, const std::string& user_id,
                                                      const std::string& original_name) {
    const std::string extension = SanitizeFileExtension(original_name);
    const std::string filename = GenerateStorageToken(20) + extension;
    return std::filesystem::path(config.storage_root) / user_id / filename;
}

std::filesystem::path BuildUploadTempDirForUserID(const ServerConfig& config, const std::string& user_id,
                                                  const std::string& upload_id) {
    return std::filesystem::path(config.storage_root) / "uploads" / user_id / upload_id;
}

std::filesystem::path BuildUploadPartPath(const std::filesystem::path& temp_dir, int part_number) {
    std::ostringstream name;
    name << "part-" << std::setw(6) << std::setfill('0') << part_number << ".bin";
    return temp_dir / name.str();
}

bool WriteBinaryFile(const std::filesystem::path& path, const std::string& body, std::string& error) {
    try {
        std::filesystem::create_directories(path.parent_path());
    } catch (const std::exception& ex) {
        error = ex.what();
        return false;
    }

    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        error = "cannot open storage file";
        return false;
    }
    output.write(body.data(), static_cast<std::streamsize>(body.size()));
    if (!output.good()) {
        error = "cannot write storage file";
        return false;
    }
    return true;
}

bool RemoveDirectoryIfExists(const std::filesystem::path& path, std::string& error) {
    if (path.empty()) {
        return true;
    }
    std::error_code remove_error;
    std::filesystem::remove_all(path, remove_error);
    if (remove_error) {
        error = remove_error.message();
        return false;
    }
    return true;
}

std::string ComputeContentHashForFile(const std::filesystem::path& path, std::string& error) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        error = "cannot open storage file";
        return {};
    }

    std::uint64_t hash = 1469598103934665603ull;
    char buffer[8192];
    while (input.good()) {
        input.read(buffer, sizeof(buffer));
        const std::streamsize count = input.gcount();
        for (std::streamsize index = 0; index < count; ++index) {
            hash ^= static_cast<unsigned char>(buffer[index]);
            hash *= 1099511628211ull;
        }
    }
    if (!input.eof()) {
        error = "cannot read storage file";
        return {};
    }

    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(16) << hash;
    return out.str();
}

bool CanAccessUpload(const UserRecord& user, const UploadRecord& upload) {
    return user.role == "admin" || upload.user_id == user.id;
}

bool IsJsonRequest(const HttpRequest& request) {
    const auto it = request.headers.find("content-type");
    if (it == request.headers.end()) {
        return !request.body.empty() && request.body.front() == '{';
    }
    return ToLower(it->second).find("application/json") != std::string::npos;
}

bool ParseInitUploadPayload(const HttpRequest& request, InitUploadPayload& payload, std::string& error) {
    std::map<std::string, std::string> object;
    if (!ParseSimpleJsonObject(request.body, object, error)) {
        return false;
    }

    payload.original_name = Trim(object["original_name"]);
    payload.display_name = Trim(object["display_name"]);
    payload.remark = Trim(object["remark"]);
    payload.content_hash = Trim(object["content_hash"]);
    if (!ParseInt64Strict(Trim(object["size_bytes"]), payload.size_bytes)) {
        error = "size_bytes is required";
        return false;
    }
    if (!ParseInt64Strict(Trim(object["chunk_size"]), payload.chunk_size)) {
        error = "chunk_size is required";
        return false;
    }
    if (!ParseIntStrict(Trim(object["total_chunks"]), payload.total_chunks)) {
        error = "total_chunks is required";
        return false;
    }
    if (payload.original_name.empty()) {
        error = "original_name is required";
        return false;
    }
    if (payload.size_bytes <= 0) {
        error = "size_bytes must be positive";
        return false;
    }
    if (payload.chunk_size <= 0) {
        error = "chunk_size must be positive";
        return false;
    }
    const int expected_total_chunks = static_cast<int>((payload.size_bytes + payload.chunk_size - 1) / payload.chunk_size);
    if (payload.total_chunks != expected_total_chunks) {
        error = "total_chunks does not match size_bytes and chunk_size";
        return false;
    }
    if (payload.display_name.empty()) {
        payload.display_name = DefaultDisplayName(payload.original_name);
    }
    return true;
}

bool UploadSessionExpired(const UploadRecord& upload, std::int64_t now) {
    return upload.status == "pending" && upload.expires_at > 0 && upload.expires_at <= now;
}

std::int64_t ExpectedUploadPartSize(const UploadRecord& upload, int part_number) {
    if (part_number < 0 || part_number >= upload.total_chunks) {
        return -1;
    }
    if (part_number == upload.total_chunks - 1) {
        return upload.size_bytes - (static_cast<std::int64_t>(part_number) * upload.chunk_size);
    }
    return upload.chunk_size;
}
