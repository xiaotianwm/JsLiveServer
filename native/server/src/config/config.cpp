#include "config.h"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <unordered_map>

namespace {

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

bool ParseListen(const std::string& value, std::string& host, std::uint16_t& port) {
    const std::size_t pos = value.rfind(':');
    if (pos == std::string::npos) {
        return false;
    }
    host = Trim(value.substr(0, pos));
    const std::string port_text = Trim(value.substr(pos + 1));
    if (host.empty() || port_text.empty()) {
        return false;
    }
    try {
        const int parsed = std::stoi(port_text);
        if (parsed < 1 || parsed > 65535) {
            return false;
        }
        port = static_cast<std::uint16_t>(parsed);
    } catch (...) {
        return false;
    }
    return true;
}

bool ParseInt64(const std::string& value, std::int64_t& out) {
    try {
        out = std::stoll(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseInt(const std::string& value, int& out) {
    try {
        out = std::stoi(value);
        return true;
    } catch (...) {
        return false;
    }
}

std::string NormalizePathString(const std::filesystem::path& path) {
    std::error_code ec;
    const std::filesystem::path absolute = std::filesystem::absolute(path, ec);
    if (ec) {
        return path.lexically_normal().string();
    }
    return absolute.lexically_normal().string();
}

std::string ResolvePathValue(const std::filesystem::path& base_dir, const std::string& value) {
    const std::filesystem::path candidate(value);
    if (candidate.is_absolute()) {
        return NormalizePathString(candidate);
    }
    return NormalizePathString(base_dir / candidate);
}

std::string ResolveExecutableValue(const std::filesystem::path& base_dir, const std::string& value) {
    const std::filesystem::path candidate(value);
    if (candidate.is_absolute() || candidate.has_parent_path()) {
        return ResolvePathValue(base_dir, value);
    }
    return value;
}

}  // namespace

std::string ServerConfig::Address() const {
    return host + ":" + std::to_string(port);
}

std::string ServerConfig::RtmpAddress() const {
    return rtmp_host + ":" + std::to_string(rtmp_port);
}

bool LoadConfigFile(const std::string& path, ServerConfig& config, std::string& error) {
    std::ifstream input(path);
    if (!input.is_open()) {
        error = "cannot open config: " + path;
        return false;
    }

    std::unordered_map<std::string, std::string> values;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        const std::size_t comment = line.find('#');
        if (comment != std::string::npos) {
            line = line.substr(0, comment);
        }
        line = Trim(line);
        if (line.empty()) {
            continue;
        }
        const std::size_t eq = line.find('=');
        if (eq == std::string::npos) {
            error = "invalid config line " + std::to_string(line_number);
            return false;
        }
        values[Trim(line.substr(0, eq))] = Trim(line.substr(eq + 1));
    }

    config.config_file_path = NormalizePathString(std::filesystem::path(path));

    std::string host;
    std::uint16_t port = 0;
    if (values.count("rtmp.listen") != 0) {
        if (!ParseListen(values["rtmp.listen"], host, port)) {
            error = "invalid rtmp.listen";
            return false;
        }
        config.rtmp_host = host;
        config.rtmp_port = port;
    }
    if (values.count("http.listen") != 0) {
        if (!ParseListen(values["http.listen"], host, port)) {
            error = "invalid http.listen";
            return false;
        }
        config.host = host;
        config.port = port;
    }
    if (values.count("database.path") != 0) {
        config.configured_db_path = values["database.path"];
    }
    if (values.count("storage.root") != 0) {
        config.configured_storage_root = values["storage.root"];
    }
    if (values.count("ffmpeg.exec") != 0) {
        config.configured_ffmpeg_exec = values["ffmpeg.exec"];
    }
    if (values.count("auth.session_ttl_seconds") != 0 &&
        !ParseInt64(values["auth.session_ttl_seconds"], config.auth_session_ttl_seconds)) {
        error = "invalid auth.session_ttl_seconds";
        return false;
    }
    if (values.count("upload.session_ttl_seconds") != 0 &&
        !ParseInt64(values["upload.session_ttl_seconds"], config.upload_session_ttl_seconds)) {
        error = "invalid upload.session_ttl_seconds";
        return false;
    }
    if (values.count("upload.cleanup_interval_seconds") != 0 &&
        !ParseInt64(values["upload.cleanup_interval_seconds"], config.upload_cleanup_interval_seconds)) {
        error = "invalid upload.cleanup_interval_seconds";
        return false;
    }
    if (values.count("room.retry_delay_seconds") != 0 &&
        !ParseInt64(values["room.retry_delay_seconds"], config.room_retry_delay_seconds)) {
        error = "invalid room.retry_delay_seconds";
        return false;
    }
    if (values.count("room.max_log_lines") != 0 &&
        !ParseInt(values["room.max_log_lines"], config.room_max_log_lines)) {
        error = "invalid room.max_log_lines";
        return false;
    }
    if (values.count("stream.gop_cache_size") != 0) {
        int cache_size = 0;
        if (!ParseInt(values["stream.gop_cache_size"], cache_size) || cache_size <= 0) {
            error = "invalid stream.gop_cache_size";
            return false;
        }
        config.stream_gop_cache_size = static_cast<std::size_t>(cache_size);
    }

    config.db_path = config.configured_db_path;
    config.storage_root = config.configured_storage_root;
    config.ffmpeg_exec = config.configured_ffmpeg_exec;

    return true;
}

void ResolveRuntimePaths(ServerConfig& config, const std::filesystem::path& executable_dir) {
    config.db_path = ResolvePathValue(executable_dir, config.configured_db_path);
    config.storage_root = ResolvePathValue(executable_dir, config.configured_storage_root);
    config.ffmpeg_exec = ResolveExecutableValue(executable_dir, config.configured_ffmpeg_exec);
}
