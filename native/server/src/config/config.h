#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

struct ServerConfig {
    std::string host = "0.0.0.0";
    std::uint16_t port = 8080;
    std::string rtmp_host = "0.0.0.0";
    std::uint16_t rtmp_port = 1935;
    std::string db_path = "data/jslive.db";
    std::string storage_root = "data/storage";
    std::string ffmpeg_exec = "ffmpeg";
    std::string configured_db_path = "data/jslive.db";
    std::string configured_storage_root = "data/storage";
    std::string configured_ffmpeg_exec = "ffmpeg";
    std::string config_file_path;
    std::int64_t auth_session_ttl_seconds = 7 * 24 * 60 * 60;
    std::int64_t upload_session_ttl_seconds = 24 * 60 * 60;
    std::int64_t upload_cleanup_interval_seconds = 10 * 60;
    std::int64_t room_retry_delay_seconds = 5;
    int room_max_log_lines = 200;
    std::size_t stream_gop_cache_size = 64;

    std::string Address() const;
    std::string RtmpAddress() const;
};

bool LoadConfigFile(const std::string& path, ServerConfig& config, std::string& error);
void ResolveRuntimePaths(ServerConfig& config, const std::filesystem::path& executable_dir);
