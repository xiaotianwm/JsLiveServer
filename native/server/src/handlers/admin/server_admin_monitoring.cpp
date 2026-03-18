#include "server_admin_monitoring.h"

#include "../../shared/http/server_shared.h"

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <map>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
#include <sstream>
#include <vector>

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

int ProcessId() {
#ifdef _WIN32
    return _getpid();
#else
    return getpid();
#endif
}

std::string PlatformName() {
#ifdef _WIN32
    return "windows";
#elif __linux__
    return "linux";
#elif __APPLE__
    return "macos";
#else
    return "unknown";
#endif
}

std::string ArchitectureName() {
#if defined(_M_X64) || defined(__x86_64__)
    return "amd64";
#elif defined(_M_IX86) || defined(__i386__)
    return "386";
#elif defined(_M_ARM64) || defined(__aarch64__)
    return "arm64";
#elif defined(_M_ARM) || defined(__arm__)
    return "arm";
#else
    return "unknown";
#endif
}

std::string WorkingDirectory() {
    std::error_code ec;
    const std::filesystem::path value = std::filesystem::current_path(ec);
    if (ec) {
        return ".";
    }
    return value.string();
}

template <typename T, typename Fn>
std::string JsonArray(const std::vector<T>& items, Fn build_item) {
    std::ostringstream out;
    out << "[";
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            out << ",";
        }
        out << build_item(items[i]);
    }
    out << "]";
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

std::string BuildStreamJson(const StreamInfoSnapshot& item) {
    std::ostringstream out;
    out << "{"
        << "\"app\":" << JsonString(item.app) << ","
        << "\"stream\":" << JsonString(item.stream) << ","
        << "\"has_publisher\":" << JsonBool(item.has_publisher) << ","
        << "\"publisher_session\":" << JsonString(item.publisher_session) << ","
        << "\"publisher_ip\":" << JsonString(item.publisher_ip) << ","
        << "\"player_count\":" << item.player_count << ","
        << "\"gop_cache_entries\":" << item.gop_cache_entries << ","
        << "\"created_at\":" << FormatTime(static_cast<std::int64_t>(item.created_at))
        << "}";
    return out.str();
}

std::string BuildDashboardJson(PersistentStore& store, std::int64_t started_at) {
    const auto users = store.ListUsers();
    const auto rooms = store.ListRooms(true, "");
    const auto files = store.ListFiles(true, "");
    const auto uploads = store.ListUploads(true, "");

    int admin_users = 0;
    int customer_users = 0;
    int active_users = 0;
    int suspended_users = 0;
    int deleted_users = 0;
    int expired_subscriptions = 0;
    std::int64_t provisioned_storage = 0;
    std::int64_t used_storage = 0;
    std::int64_t reserved_storage = 0;
    int provisioned_slots = 0;
    int occupied_slots = 0;

    for (const auto& user : users) {
        if (user.role == "admin") {
            ++admin_users;
        } else {
            ++customer_users;
        }
        if (user.status == "active") {
            ++active_users;
        } else if (user.status == "suspended") {
            ++suspended_users;
        } else if (user.status == "deleted") {
            ++deleted_users;
        }
        if (user.subscription_ends_at > 0 && user.subscription_ends_at < NowUnix()) {
            ++expired_subscriptions;
        }
        const StorageUsage usage = store.UsageForUser(user);
        provisioned_storage += user.max_storage_bytes;
        used_storage += usage.used_storage_bytes;
        reserved_storage += usage.reserved_storage_bytes;
        provisioned_slots += user.max_active_rooms;
        occupied_slots += usage.active_room_count;
    }

    int active_rooms = 0;
    int running_rooms = 0;
    int retry_wait_rooms = 0;
    int starting_rooms = 0;
    int stopping_rooms = 0;
    int idle_rooms = 0;
    int failed_rooms = 0;
    int inactive_rooms = 0;
    int network_mode_rooms = 0;
    int file_mode_rooms = 0;
    int rooms_with_error = 0;
    std::map<std::string, bool> active_owners;
    for (const auto& room : rooms) {
        if (room.managed_status == "active") {
            ++active_rooms;
            active_owners[room.owner_id] = true;
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

    std::int64_t total_file_size = 0;
    std::int64_t in_use_size = 0;
    int in_use_files = 0;
    std::map<std::string, bool> file_owners;
    for (const auto& file : files) {
        total_file_size += file.size_bytes;
        file_owners[file.user_id] = true;
        if (ActiveRoomCountForFile(file, rooms) > 0) {
            ++in_use_files;
            in_use_size += file.size_bytes;
        }
    }

    int pending_uploads = 0;
    int completed_uploads = 0;
    int aborted_uploads = 0;
    int expired_pending_uploads = 0;
    std::int64_t pending_bytes = 0;
    std::int64_t completed_bytes = 0;
    std::map<std::string, bool> upload_owners;
    for (const auto& upload : uploads) {
        upload_owners[upload.user_id] = true;
        if (upload.status == "pending") {
            ++pending_uploads;
            pending_bytes += upload.size_bytes;
            if (upload.expires_at > 0 && upload.expires_at < NowUnix()) {
                ++expired_pending_uploads;
            }
        } else if (upload.status == "completed") {
            ++completed_uploads;
            completed_bytes += upload.size_bytes;
        } else if (upload.status == "aborted") {
            ++aborted_uploads;
        }
    }

    const std::int64_t now = NowUnix();
    const std::int64_t uptime = std::max<std::int64_t>(0, now - started_at);

    std::ostringstream out;
    out << "{"
        << "\"server\":{"
        << "\"hostname\":\"native-jslive\","
        << "\"pid\":" << ProcessId() << ","
        << "\"working_dir\":" << JsonString(WorkingDirectory()) << ","
        << "\"started_at\":" << FormatTime(started_at) << ","
        << "\"current_time\":" << FormatTime(now) << ","
        << "\"uptime_seconds\":" << uptime << ","
        << "\"go_version\":\"native-cpp\","
        << "\"go_os\":" << JsonString(PlatformName()) << ","
        << "\"go_arch\":" << JsonString(ArchitectureName()) << ","
        << "\"num_cpu\":1,"
        << "\"gomaxprocs\":1,"
        << "\"goroutines\":0,"
        << "\"memory\":{\"alloc_bytes\":0,\"heap_alloc_bytes\":0,\"sys_bytes\":0,\"heap_objects\":0,\"gc_cycles\":0,\"last_gc_at\":null}"
        << "},"
        << "\"business\":{"
        << "\"users\":{"
        << "\"total_users\":" << users.size() << ","
        << "\"admin_users\":" << admin_users << ","
        << "\"customer_users\":" << customer_users << ","
        << "\"active_users\":" << active_users << ","
        << "\"suspended_users\":" << suspended_users << ","
        << "\"deleted_users\":" << deleted_users << ","
        << "\"expired_subscriptions\":" << expired_subscriptions
        << "},"
        << "\"rooms\":{"
        << "\"total_rooms\":" << rooms.size() << ","
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
        << "\"active_owner_count\":" << active_owners.size()
        << "},"
        << "\"files\":{"
        << "\"total_files\":" << files.size() << ","
        << "\"total_size_bytes\":" << total_file_size << ","
        << "\"in_use_files\":" << in_use_files << ","
        << "\"in_use_size_bytes\":" << in_use_size << ","
        << "\"unique_owner_count\":" << file_owners.size()
        << "},"
        << "\"uploads\":{"
        << "\"total_sessions\":" << uploads.size() << ","
        << "\"pending_sessions\":" << pending_uploads << ","
        << "\"completed_sessions\":" << completed_uploads << ","
        << "\"aborted_sessions\":" << aborted_uploads << ","
        << "\"expired_pending_sessions\":" << expired_pending_uploads << ","
        << "\"pending_bytes\":" << pending_bytes << ","
        << "\"completed_bytes\":" << completed_bytes << ","
        << "\"unique_owner_count\":" << upload_owners.size()
        << "},"
        << "\"quota\":{"
        << "\"quota_user_count\":" << users.size() << ","
        << "\"provisioned_storage_bytes\":" << provisioned_storage << ","
        << "\"used_storage_bytes\":" << used_storage << ","
        << "\"reserved_upload_bytes\":" << reserved_storage << ","
        << "\"remaining_storage_bytes\":" << std::max<std::int64_t>(0, provisioned_storage - used_storage - reserved_storage) << ","
        << "\"provisioned_active_room_slots\":" << provisioned_slots << ","
        << "\"occupied_active_room_slots\":" << occupied_slots << ","
        << "\"remaining_active_room_slots\":" << std::max(0, provisioned_slots - occupied_slots)
        << "}"
        << "}"
        << "}";
    return out.str();
}

std::string BuildSystemConfigJson(const ServerConfig& config, const std::vector<RoomRecord>& rooms, std::int64_t started_at) {
    int running_rooms = 0;
    int retry_rooms = 0;
    for (const auto& room : rooms) {
        if (room.runtime_status == "running") {
            ++running_rooms;
        } else if (room.runtime_status == "retry_wait") {
            ++retry_rooms;
        }
    }
    const std::int64_t now = NowUnix();
    const std::int64_t uptime = std::max<std::int64_t>(0, now - started_at);
    std::ostringstream out;
    out << "{"
        << "\"server\":{\"host\":" << JsonString(config.host) << ",\"port\":" << JsonString(std::to_string(config.port))
        << ",\"address\":" << JsonString(config.Address()) << ",\"rtmp_host\":" << JsonString(config.rtmp_host)
        << ",\"rtmp_port\":" << JsonString(std::to_string(config.rtmp_port)) << ",\"rtmp_address\":"
        << JsonString(config.RtmpAddress()) << "},"
        << "\"paths\":{\"config_file\":" << JsonString(config.config_file_path)
        << ",\"ffmpeg_exec\":" << JsonString(config.configured_ffmpeg_exec)
        << ",\"resolved_ffmpeg_exec\":" << JsonString(config.ffmpeg_exec)
        << ",\"db_path\":" << JsonString(config.configured_db_path)
        << ",\"resolved_db_path\":" << JsonString(config.db_path)
        << ",\"storage_root\":" << JsonString(config.configured_storage_root)
        << ",\"resolved_storage_root\":" << JsonString(config.storage_root) << "},"
        << "\"rooms\":{\"retry_delay_seconds\":" << config.room_retry_delay_seconds
        << ",\"max_log_lines\":" << config.room_max_log_lines << "},"
        << "\"uploads\":{\"session_ttl_seconds\":" << config.upload_session_ttl_seconds
        << ",\"cleanup_interval_seconds\":" << config.upload_cleanup_interval_seconds << "},"
        << "\"policies\":{\"retry_until_manual_stop\":true,\"started_room_occupies_slot\":true,"
        << "\"pending_upload_reserves_quota\":true,\"supports_single_request_upload\":true,"
        << "\"supports_chunk_upload\":true,\"supports_resumable_upload\":true},"
        << "\"health\":{\"status\":\"ok\",\"checked_at\":" << FormatTime(now)
        << ",\"checks\":[{\"name\":\"store\",\"ok\":true,\"detail\":\"flat file store ready\",\"latency_ms\":0}]},"
        << "\"runtime\":{\"started_at\":" << FormatTime(started_at) << ",\"current_time\":" << FormatTime(now)
        << ",\"uptime_seconds\":" << uptime << ",\"running_rooms\":" << running_rooms
        << ",\"retrying_rooms\":" << retry_rooms << "}"
        << "}";
    return out.str();
}

std::string EmptyTrendJson() {
    return "{\"window_seconds\":3600,\"sample_interval_seconds\":30,\"retention_seconds\":86400,\"points\":[],\"summary\":{\"point_count\":0,\"first_at\":null,\"last_at\":null,\"max_active_rooms\":0,\"max_running_rooms\":0,\"max_retry_wait_rooms\":0,\"max_failed_rooms\":0,\"max_occupied_slots\":0,\"max_pending_uploads\":0,\"max_used_storage_bytes\":0,\"max_reserved_upload_bytes\":0,\"max_goroutines\":0,\"max_alloc_bytes\":0,\"max_heap_alloc_bytes\":0}}";
}

}  // namespace

bool TryHandleAdminMonitoringRoute(SocketHandle socket, const HttpRequest& request, const ServerConfig& config,
                                   PersistentStore& store, StreamManager& stream_manager, std::int64_t started_at) {
    if (request.method == "GET" && request.path == "/api/v1/admin/dashboard") {
        SendAndClose(socket, 200, BuildDashboardJson(store, started_at));
        return true;
    }
    if (request.method == "GET" && request.path == "/api/v1/admin/dashboard/trends") {
        SendAndClose(socket, 200, EmptyTrendJson());
        return true;
    }
    if (request.method == "GET" && request.path == "/api/v1/admin/system/config") {
        SendAndClose(socket, 200, BuildSystemConfigJson(config, store.ListRooms(true, ""), started_at));
        return true;
    }
    if (request.method == "GET" && request.path == "/api/v1/admin/streams") {
        const auto streams = stream_manager.SnapshotAll();
        SendAndClose(socket, 200, std::string("{\"items\":") + JsonArray<StreamInfoSnapshot>(streams, [](const StreamInfoSnapshot& item) {
            return BuildStreamJson(item);
        }) + "}");
        return true;
    }
    return false;
}
