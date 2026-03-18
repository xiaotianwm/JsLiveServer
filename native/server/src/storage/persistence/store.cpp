#include "store.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>

namespace {

constexpr std::size_t kRoomLogRetention = 200;

std::string EscapeField(const std::string& value) {
    std::ostringstream out;
    for (unsigned char ch : value) {
        if (std::isalnum(ch) != 0 || ch == '-' || ch == '_' || ch == '.' || ch == ':' || ch == '/') {
            out << static_cast<char>(ch);
        } else {
            static const char* kHex = "0123456789ABCDEF";
            out << '%';
            out << kHex[(ch >> 4) & 0x0f];
            out << kHex[ch & 0x0f];
        }
    }
    return out.str();
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

std::string UnescapeField(const std::string& value) {
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
        out.push_back(value[i]);
    }
    return out;
}

std::vector<std::string> SplitLine(const std::string& line) {
    std::vector<std::string> parts;
    std::stringstream stream(line);
    std::string item;
    while (std::getline(stream, item, '|')) {
        parts.push_back(UnescapeField(item));
    }
    return parts;
}

std::string JoinLogs(const std::vector<std::string>& logs) {
    std::ostringstream out;
    for (std::size_t i = 0; i < logs.size(); ++i) {
        if (i != 0) {
            out << '\n';
        }
        out << logs[i];
    }
    return out.str();
}

std::vector<std::string> SplitLogs(const std::string& encoded) {
    std::vector<std::string> logs;
    std::stringstream stream(encoded);
    std::string line;
    while (std::getline(stream, line)) {
        logs.push_back(line);
    }
    return logs;
}

std::string JoinInts(const std::vector<int>& values) {
    std::ostringstream out;
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index != 0) {
            out << ",";
        }
        out << values[index];
    }
    return out.str();
}

std::vector<int> SplitInts(const std::string& encoded) {
    std::vector<int> values;
    if (encoded.empty()) {
        return values;
    }
    std::stringstream stream(encoded);
    std::string part;
    while (std::getline(stream, part, ',')) {
        if (part.empty()) {
            continue;
        }
        try {
            values.push_back(std::stoi(part));
        } catch (...) {
        }
    }
    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
    return values;
}

}  // namespace

bool PersistentStore::Load(const std::string& path, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    path_ = path;
    users_.clear();
    rooms_.clear();
    rtmp_services_.clear();
    files_.clear();
    uploads_.clear();
    sessions_.clear();

    try {
        const std::filesystem::path db_path(path_);
        if (db_path.has_parent_path()) {
            std::filesystem::create_directories(db_path.parent_path());
        }
        if (!std::filesystem::exists(db_path)) {
            return PersistLocked(error);
        }
    } catch (const std::exception& ex) {
        error = ex.what();
        return false;
    }

    std::ifstream input(path_);
    if (!input.is_open()) {
        error = "cannot open database: " + path_;
        return false;
    }

    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const std::vector<std::string> parts = SplitLine(line);
        if (parts.empty() || parts[0] == "VERSION") {
            continue;
        }
        if (parts[0] == "USER" && parts.size() == 10) {
            UserRecord user;
            user.id = parts[1];
            user.username = parts[2];
            user.password = parts[3];
            user.role = parts[4];
            user.status = parts[5];
            user.max_storage_bytes = std::stoll(parts[6]);
            user.max_active_rooms = std::stoi(parts[7]);
            user.subscription_ends_at = std::stoll(parts[8]);
            user.created_at = std::stoll(parts[9]);
            users_[user.id] = user;
            continue;
        }
        if (parts[0] == "ROOMV5" && parts.size() == 28) {
            RoomRecord room;
            room.id = parts[1];
            room.name = parts[2];
            room.owner_id = parts[3];
            room.owner_name = parts[4];
            room.stream_name = parts[5];
            room.publish_key = parts[6];
            room.play_key = parts[7];
            room.mode = parts[8];
            room.input_url = parts[9];
            room.file_id = parts[10];
            room.rtmp_url = parts[12];
            room.managed_status = parts[13];
            room.runtime_status = parts[14];
            room.last_error = parts[15];
            room.retry_count = std::stoi(parts[16]);
            room.next_retry_at = std::stoll(parts[17]);
            room.last_start_attempt_at = std::stoll(parts[18]);
            room.last_running_at = std::stoll(parts[19]);
            room.last_exit_at = std::stoll(parts[20]);
            room.created_at = std::stoll(parts[21]);
            room.updated_at = std::stoll(parts[22]);
            room.activated_at = std::stoll(parts[23]);
            room.stopped_at = std::stoll(parts[24]);
            room.log_line_count = std::stoi(parts[25]);
            room.latest_log = parts[26];
            room.recent_logs = SplitLogs(parts[27]);
            if (room.recent_logs.empty() && !room.latest_log.empty()) {
                room.recent_logs.push_back(room.latest_log);
            }
            rooms_[room.id] = room;
            continue;
        }
        if (parts[0] == "ROOMV4" && parts.size() == 27) {
            RoomRecord room;
            room.id = parts[1];
            room.name = parts[2];
            room.owner_id = parts[3];
            room.owner_name = parts[4];
            room.stream_name = parts[5];
            room.publish_key = parts[6];
            room.play_key = parts[7];
            room.mode = parts[8];
            room.input_url = parts[9];
            room.file_id = parts[10];
            room.rtmp_url = parts[11];
            room.managed_status = parts[12];
            room.runtime_status = parts[13];
            room.last_error = parts[14];
            room.retry_count = std::stoi(parts[15]);
            room.next_retry_at = std::stoll(parts[16]);
            room.last_start_attempt_at = std::stoll(parts[17]);
            room.last_running_at = std::stoll(parts[18]);
            room.last_exit_at = std::stoll(parts[19]);
            room.created_at = std::stoll(parts[20]);
            room.updated_at = std::stoll(parts[21]);
            room.activated_at = std::stoll(parts[22]);
            room.stopped_at = std::stoll(parts[23]);
            room.log_line_count = std::stoi(parts[24]);
            room.latest_log = parts[25];
            room.recent_logs = SplitLogs(parts[26]);
            if (room.recent_logs.empty() && !room.latest_log.empty()) {
                room.recent_logs.push_back(room.latest_log);
            }
            rooms_[room.id] = room;
            continue;
        }
        if (parts[0] == "ROOMV3" && parts.size() == 26) {
            RoomRecord room;
            room.id = parts[1];
            room.name = parts[2];
            room.owner_id = parts[3];
            room.owner_name = parts[4];
            room.stream_name = parts[5];
            room.publish_key = parts[6];
            room.play_key = parts[7];
            room.mode = parts[8];
            room.input_url = parts[9];
            room.file_id = parts[10];
            room.rtmp_url = parts[11];
            room.managed_status = parts[12];
            room.runtime_status = parts[13];
            room.last_error = parts[14];
            room.retry_count = std::stoi(parts[15]);
            room.next_retry_at = std::stoll(parts[16]);
            room.last_start_attempt_at = std::stoll(parts[17]);
            room.last_running_at = std::stoll(parts[18]);
            room.last_exit_at = std::stoll(parts[19]);
            room.created_at = std::stoll(parts[20]);
            room.updated_at = std::stoll(parts[21]);
            room.activated_at = std::stoll(parts[22]);
            room.stopped_at = std::stoll(parts[23]);
            room.log_line_count = std::stoi(parts[24]);
            room.latest_log = parts[25];
            if (!room.latest_log.empty()) {
                room.recent_logs.push_back(room.latest_log);
            }
            rooms_[room.id] = room;
            continue;
        }
        if (parts[0] == "ROOMV2" && parts.size() == 23) {
            RoomRecord room;
            room.id = parts[1];
            room.name = parts[2];
            room.owner_id = parts[3];
            room.owner_name = parts[4];
            room.mode = parts[5];
            room.input_url = parts[6];
            room.file_id = parts[7];
            room.rtmp_url = parts[8];
            room.managed_status = parts[9];
            room.runtime_status = parts[10];
            room.last_error = parts[11];
            room.retry_count = std::stoi(parts[12]);
            room.next_retry_at = std::stoll(parts[13]);
            room.last_start_attempt_at = std::stoll(parts[14]);
            room.last_running_at = std::stoll(parts[15]);
            room.last_exit_at = std::stoll(parts[16]);
            room.created_at = std::stoll(parts[17]);
            room.updated_at = std::stoll(parts[18]);
            room.activated_at = std::stoll(parts[19]);
            room.stopped_at = std::stoll(parts[20]);
            room.log_line_count = std::stoi(parts[21]);
            room.latest_log = parts[22];
            room.stream_name = room.id;
            room.publish_key = GenerateID("pub_", 16);
            room.play_key = GenerateID("play_", 16);
            if (!room.latest_log.empty()) {
                room.recent_logs.push_back(room.latest_log);
            }
            rooms_[room.id] = room;
            continue;
        }
        if (parts[0] == "RTMPSERVICEV1" && parts.size() == 18) {
            RtmpServiceRecord service;
            service.id = parts[1];
            service.name = parts[2];
            service.owner_id = parts[3];
            service.owner_name = parts[4];
            service.stream_name = parts[5];
            service.publish_key = parts[6];
            service.play_key = parts[7];
            service.source_url = parts[8];
            service.managed_status = parts[9];
            service.runtime_status = parts[10];
            service.last_error = parts[11];
            service.created_at = std::stoll(parts[12]);
            service.updated_at = std::stoll(parts[13]);
            service.activated_at = std::stoll(parts[14]);
            service.stopped_at = std::stoll(parts[15]);
            service.last_publisher_connected_at = std::stoll(parts[16]);
            service.last_publisher_disconnected_at = std::stoll(parts[17]);
            service.log_line_count = 0;
            if (service.last_publisher_connected_at > 0) {
                service.latest_log = "Publisher activity recorded";
            }
            rtmp_services_[service.id] = service;
            continue;
        }
        if (parts[0] == "RTMPSERVICEV2" && parts.size() == 21) {
            RtmpServiceRecord service;
            service.id = parts[1];
            service.name = parts[2];
            service.owner_id = parts[3];
            service.owner_name = parts[4];
            service.stream_name = parts[5];
            service.publish_key = parts[6];
            service.play_key = parts[7];
            service.source_url = parts[8];
            service.managed_status = parts[9];
            service.runtime_status = parts[10];
            service.last_error = parts[11];
            service.created_at = std::stoll(parts[12]);
            service.updated_at = std::stoll(parts[13]);
            service.activated_at = std::stoll(parts[14]);
            service.stopped_at = std::stoll(parts[15]);
            service.last_publisher_connected_at = std::stoll(parts[16]);
            service.last_publisher_disconnected_at = std::stoll(parts[17]);
            service.log_line_count = std::stoi(parts[18]);
            service.latest_log = parts[19];
            service.recent_logs = SplitLogs(parts[20]);
            if (service.recent_logs.empty() && !service.latest_log.empty()) {
                service.recent_logs.push_back(service.latest_log);
            }
            rtmp_services_[service.id] = service;
            continue;
        }
        if (parts[0] == "FILEV3" && parts.size() == 13) {
            FileRecord file;
            file.id = parts[1];
            file.user_id = parts[2];
            file.owner_name = parts[3];
            file.original_name = parts[4];
            file.display_name = parts[5];
            file.remark = parts[6];
            file.stored_path = parts[7];
            file.size_bytes = std::stoll(parts[8]);
            file.content_hash = parts[9];
            file.status = parts[10];
            file.created_at = std::stoll(parts[11]);
            file.updated_at = std::stoll(parts[12]);
            files_[file.id] = file;
            continue;
        }
        if (parts[0] == "FILEV2" && parts.size() == 12) {
            FileRecord file;
            file.id = parts[1];
            file.user_id = parts[2];
            file.owner_name = parts[3];
            file.original_name = parts[4];
            file.display_name = parts[5];
            file.remark = parts[6];
            file.stored_path.clear();
            file.size_bytes = std::stoll(parts[7]);
            file.content_hash = parts[8];
            file.status = parts[9];
            file.created_at = std::stoll(parts[10]);
            file.updated_at = std::stoll(parts[11]);
            files_[file.id] = file;
            continue;
        }
        if (parts[0] == "UPLOADV3" && parts.size() == 19) {
            UploadRecord upload;
            upload.id = parts[1];
            upload.user_id = parts[2];
            upload.owner_name = parts[3];
            upload.original_name = parts[4];
            upload.display_name = parts[5];
            upload.remark = parts[6];
            upload.content_hash = parts[7];
            upload.size_bytes = std::stoll(parts[8]);
            upload.chunk_size = std::stoll(parts[9]);
            upload.total_chunks = std::stoi(parts[10]);
            upload.uploaded_chunk_count = std::stoi(parts[11]);
            upload.uploaded_parts = SplitInts(parts[12]);
            upload.status = parts[13];
            upload.temp_dir = parts[14];
            upload.completed_file_id = parts[15];
            upload.created_at = std::stoll(parts[16]);
            upload.updated_at = std::stoll(parts[17]);
            upload.expires_at = std::stoll(parts[18]);
            if (upload.uploaded_chunk_count <= 0) {
                upload.uploaded_chunk_count = static_cast<int>(upload.uploaded_parts.size());
            }
            uploads_[upload.id] = upload;
            continue;
        }
        if (parts[0] == "UPLOADV2" && parts.size() == 15) {
            UploadRecord upload;
            upload.id = parts[1];
            upload.user_id = parts[2];
            upload.owner_name = parts[3];
            upload.original_name = parts[4];
            upload.display_name = parts[4];
            upload.remark.clear();
            upload.content_hash = parts[5];
            upload.size_bytes = std::stoll(parts[6]);
            upload.chunk_size = std::stoll(parts[7]);
            upload.total_chunks = std::stoi(parts[8]);
            upload.uploaded_chunk_count = std::stoi(parts[9]);
            if (upload.uploaded_chunk_count > 0) {
                for (int part_number = 0; part_number < upload.uploaded_chunk_count; ++part_number) {
                    upload.uploaded_parts.push_back(part_number);
                }
            }
            upload.status = parts[10];
            upload.temp_dir.clear();
            upload.completed_file_id = parts[11];
            upload.created_at = std::stoll(parts[12]);
            upload.updated_at = std::stoll(parts[13]);
            upload.expires_at = std::stoll(parts[14]);
            uploads_[upload.id] = upload;
            continue;
        }
        if (parts[0] == "SESSION" && parts.size() == 5) {
            AuthSessionRecord session;
            session.token = parts[1];
            session.user_id = parts[2];
            session.created_at = std::stoll(parts[3]);
            session.expires_at = std::stoll(parts[4]);
            sessions_[session.token] = session;
        }
    }

    return true;
}

bool PersistentStore::SeedDefaultsIfEmpty(std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!users_.empty()) {
        return true;
    }

    const std::int64_t now = CurrentUnixTime();
    UserRecord admin;
    admin.id = GenerateID("usr_", 12);
    admin.username = "admin";
    admin.password = "plain:admin123";
    admin.role = "admin";
    admin.status = "active";
    admin.max_storage_bytes = 1LL << 40;
    admin.max_active_rooms = 999;
    admin.subscription_ends_at = now + 3600LL * 24 * 365 * 10;
    admin.created_at = now;
    users_[admin.id] = admin;

    UserRecord user;
    user.id = GenerateID("usr_", 12);
    user.username = "user";
    user.password = "plain:user123";
    user.role = "user";
    user.status = "active";
    user.max_storage_bytes = 10LL << 30;
    user.max_active_rooms = 2;
    user.subscription_ends_at = now + 3600LL * 24 * 30;
    user.created_at = now + 1;
    users_[user.id] = user;

    RoomRecord room;
    room.id = GenerateID("room_", 10);
    room.name = "Sample Studio";
    room.owner_id = user.id;
    room.owner_name = user.username;
    room.stream_name = "sample-room";
    room.publish_key = GenerateID("pub_", 16);
    room.play_key = GenerateID("play_", 16);
    room.mode = "network";
    room.input_url = "rtmp://origin.example/live/input";
    room.rtmp_url = "rtmp://127.0.0.1/live/sample";
    room.managed_status = "active";
    room.runtime_status = "idle";
    room.created_at = now;
    room.updated_at = now;
    room.activated_at = now;
    room.log_line_count = 2;
    room.latest_log = "Room seeded for native desktop integration";
    room.recent_logs = {"Room created", room.latest_log};
    rooms_[room.id] = room;

    RtmpServiceRecord service;
    service.id = GenerateID("rtmp_", 10);
    service.name = "Sample RTMP Service";
    service.owner_id = user.id;
    service.owner_name = user.username;
    service.stream_name = "sample-service";
    service.publish_key = GenerateID("pub_", 16);
    service.play_key = GenerateID("play_", 16);
    service.source_url = "OBS Studio";
    service.managed_status = "active";
    service.runtime_status = "idle";
    service.created_at = now;
    service.updated_at = now;
    service.activated_at = now;
    service.log_line_count = 1;
    service.latest_log = "RTMP service created";
    service.recent_logs = {service.latest_log};
    rtmp_services_[service.id] = service;

    FileRecord file;
    file.id = GenerateID("file_", 10);
    file.user_id = user.id;
    file.owner_name = user.username;
    file.original_name = "launch.mp4";
    file.display_name = "Launch Reel";
    file.remark = "Seed media file";
    file.size_bytes = 512LL * 1024 * 1024;
    file.content_hash = "seed-launch-hash";
    file.status = "ready";
    file.created_at = now;
    file.updated_at = now;
    files_[file.id] = file;

    UploadRecord upload;
    upload.id = GenerateID("upl_", 10);
    upload.user_id = user.id;
    upload.owner_name = user.username;
    upload.original_name = "promo-pack.zip";
    upload.content_hash = "seed-upload-hash";
    upload.size_bytes = 256LL * 1024 * 1024;
    upload.chunk_size = 8LL * 1024 * 1024;
    upload.total_chunks = 32;
    upload.uploaded_chunk_count = 10;
    upload.status = "pending";
    upload.created_at = now;
    upload.updated_at = now;
    upload.expires_at = now + 3600LL * 24;
    uploads_[upload.id] = upload;

    return PersistLocked(error);
}

bool PersistentStore::FindUserByCredentials(const std::string& username, const std::string& password, UserRecord& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& entry : users_) {
        if (entry.second.username == username && CheckPassword(entry.second.password, password)) {
            out = entry.second;
            return true;
        }
    }
    return false;
}

bool PersistentStore::GetUserByID(const std::string& user_id, UserRecord& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = users_.find(user_id);
    if (it == users_.end()) {
        return false;
    }
    out = it->second;
    return true;
}

bool PersistentStore::GetUserByUsername(const std::string& username, UserRecord& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& entry : users_) {
        if (entry.second.username == username) {
            out = entry.second;
            return true;
        }
    }
    return false;
}

bool PersistentStore::VerifyUserPassword(const std::string& user_id, const std::string& password) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = users_.find(user_id);
    if (it == users_.end()) {
        return false;
    }
    return CheckPassword(it->second.password, password);
}

std::vector<UserRecord> PersistentStore::ListUsers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<UserRecord> items;
    items.reserve(users_.size());
    for (const auto& entry : users_) {
        items.push_back(entry.second);
    }
    std::sort(items.begin(), items.end(), [](const UserRecord& left, const UserRecord& right) {
        return left.created_at < right.created_at;
    });
    return items;
}

bool PersistentStore::CreateUser(UserRecord& user, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& entry : users_) {
        if (entry.second.username == user.username) {
            error = "username already exists";
            return false;
        }
    }

    if (user.id.empty()) {
        user.id = GenerateID("usr_", 12);
    }
    if (users_.find(user.id) != users_.end()) {
        error = "user id already exists";
        return false;
    }
    if (user.created_at <= 0) {
        user.created_at = CurrentUnixTime();
    }

    users_[user.id] = user;
    if (!PersistLocked(error)) {
        users_.erase(user.id);
        return false;
    }
    return true;
}

bool PersistentStore::UpdateUser(const UserRecord& user, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto existing = users_.find(user.id);
    if (existing == users_.end()) {
        error = "user not found";
        return false;
    }
    for (const auto& entry : users_) {
        if (entry.first != user.id && entry.second.username == user.username) {
            error = "username already exists";
            return false;
        }
    }

    const UserRecord previous = existing->second;
    users_[user.id] = user;
    if (!PersistLocked(error)) {
        users_[user.id] = previous;
        return false;
    }
    return true;
}

bool PersistentStore::GetRoomByID(const std::string& room_id, RoomRecord& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rooms_.find(room_id);
    if (it == rooms_.end()) {
        return false;
    }
    out = it->second;
    return true;
}

bool PersistentStore::GetRtmpServiceByID(const std::string& service_id, RtmpServiceRecord& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rtmp_services_.find(service_id);
    if (it == rtmp_services_.end()) {
        return false;
    }
    out = it->second;
    return true;
}

bool PersistentStore::GetFileByID(const std::string& file_id, FileRecord& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = files_.find(file_id);
    if (it == files_.end()) {
        return false;
    }
    out = it->second;
    return true;
}

bool PersistentStore::AuthorizeRoomStream(const std::string& stream_name, const std::string& action, const std::string& key,
                                          RoomRecord& out, std::string& error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& entry : rooms_) {
        const RoomRecord& room = entry.second;
        if (room.stream_name != stream_name) {
            continue;
        }
        if (room.managed_status != "active") {
            error = "room is not active";
            return false;
        }
        if (key.empty()) {
            error = "stream key is required";
            return false;
        }
        if (action == "publish") {
            if (room.publish_key != key) {
                error = "publish key rejected";
                return false;
            }
        } else {
            if (room.play_key != key) {
                error = "play key rejected";
                return false;
            }
        }
        out = room;
        return true;
    }
    error = "room not found";
    return false;
}

bool PersistentStore::AuthorizeRtmpServiceStream(const std::string& stream_name, const std::string& action, const std::string& key,
                                                 RtmpServiceRecord& out, std::string& error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    (void)action;
    (void)key;
    for (const auto& entry : rtmp_services_) {
        const RtmpServiceRecord& service = entry.second;
        if (service.stream_name != stream_name) {
            continue;
        }
        if (service.managed_status != "active") {
            error = "rtmp service is not active";
            return false;
        }
        out = service;
        return true;
    }
    error = "rtmp service not found";
    return false;
}

bool PersistentStore::CreateRoom(const UserRecord& owner, const std::string& name, const std::string& mode,
                                 const std::string& input_url, const std::string& file_id, const std::string& rtmp_url,
                                 RoomRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::int64_t now = CurrentUnixTime();
    RoomRecord room;
    room.id = GenerateID("room_", 10);
    room.name = name;
    room.owner_id = owner.id;
    room.owner_name = owner.username;
    room.stream_name = room.id;
    room.publish_key = GenerateID("pub_", 16);
    room.play_key = GenerateID("play_", 16);
    room.mode = mode;
    room.input_url = input_url;
    room.file_id = file_id;
    room.rtmp_url = rtmp_url;
    room.managed_status = "inactive";
    room.runtime_status = "idle";
    room.created_at = now;
    room.updated_at = now;
    room.activated_at = 0;
    room.stopped_at = now;
    room.log_line_count = 1;
    room.latest_log = "Room created";
    room.recent_logs = {room.latest_log};
    rooms_[room.id] = room;
    if (!PersistLocked(error)) {
        rooms_.erase(room.id);
        return false;
    }
    out = room;
    return true;
}

bool PersistentStore::CreateRtmpService(const UserRecord& owner, const std::string& name, const std::string& source_url,
                                        RtmpServiceRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::int64_t now = CurrentUnixTime();
    RtmpServiceRecord service;
    service.id = GenerateID("rtmp_", 10);
    service.name = name;
    service.owner_id = owner.id;
    service.owner_name = owner.username;
    service.stream_name = service.id;
    service.publish_key = GenerateID("pub_", 16);
    service.play_key = GenerateID("play_", 16);
    service.source_url = source_url;
    service.managed_status = "inactive";
    service.runtime_status = "idle";
    service.created_at = now;
    service.updated_at = now;
    service.stopped_at = now;
    service.log_line_count = 1;
    service.latest_log = "RTMP service created";
    service.recent_logs = {service.latest_log};
    rtmp_services_[service.id] = service;
    if (!PersistLocked(error)) {
        rtmp_services_.erase(service.id);
        return false;
    }
    out = service;
    return true;
}

bool PersistentStore::UpdateRoom(const RoomRecord& room, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rooms_.find(room.id);
    if (it == rooms_.end()) {
        error = "room not found";
        return false;
    }
    rooms_[room.id] = room;
    return PersistLocked(error);
}

bool PersistentStore::UpdateRtmpService(const RtmpServiceRecord& service, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rtmp_services_.find(service.id);
    if (it == rtmp_services_.end()) {
        error = "rtmp service not found";
        return false;
    }
    rtmp_services_[service.id] = service;
    return PersistLocked(error);
}

bool PersistentStore::DeleteRoom(const std::string& room_id, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rooms_.find(room_id);
    if (it == rooms_.end()) {
        error = "room not found";
        return false;
    }
    rooms_.erase(it);
    return PersistLocked(error);
}

bool PersistentStore::DeleteRtmpService(const std::string& service_id, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rtmp_services_.find(service_id);
    if (it == rtmp_services_.end()) {
        error = "rtmp service not found";
        return false;
    }
    rtmp_services_.erase(it);
    return PersistLocked(error);
}

bool PersistentStore::RemoveFileRecord(const std::string& file_id, FileRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = files_.find(file_id);
    if (it == files_.end()) {
        error = "file not found";
        return false;
    }
    for (const auto& room_entry : rooms_) {
        if (room_entry.second.file_id == file_id) {
            error = "file is referenced by a room";
            return false;
        }
    }

    out = it->second;
    files_.erase(it);
    for (auto& upload_entry : uploads_) {
        if (upload_entry.second.completed_file_id == file_id) {
            upload_entry.second.completed_file_id.clear();
            upload_entry.second.updated_at = CurrentUnixTime();
        }
    }
    return PersistLocked(error);
}

bool PersistentStore::SetRoomState(const std::string& room_id, const std::string& managed_status, const std::string& runtime_status,
                                   const RoomStatePatch& patch, RoomRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rooms_.find(room_id);
    if (it == rooms_.end()) {
        error = "room not found";
        return false;
    }

    RoomRecord& room = it->second;
    room.managed_status = managed_status;
    room.runtime_status = runtime_status;
    room.updated_at = CurrentUnixTime();
    if (patch.set_last_error) {
        room.last_error = patch.last_error;
    }
    if (patch.set_retry_count) {
        room.retry_count = patch.retry_count;
    }
    if (patch.set_next_retry_at) {
        room.next_retry_at = patch.next_retry_at;
    }
    if (patch.set_last_start_attempt_at) {
        room.last_start_attempt_at = patch.last_start_attempt_at;
    }
    if (patch.set_last_running_at) {
        room.last_running_at = patch.last_running_at;
    }
    if (patch.set_last_exit_at) {
        room.last_exit_at = patch.last_exit_at;
    }
    if (patch.set_activated_at) {
        room.activated_at = patch.activated_at;
    }
    if (patch.set_stopped_at) {
        room.stopped_at = patch.stopped_at;
    }
    if (patch.append_log && !patch.log_line.empty()) {
        AppendRoomLogLocked(room, patch.log_line);
    }
    if (!PersistLocked(error)) {
        return false;
    }
    out = room;
    return true;
}

bool PersistentStore::SetRtmpServiceState(const std::string& service_id, const std::string& managed_status,
                                          const std::string& runtime_status, const RoomStatePatch& patch, RtmpServiceRecord& out,
                                          std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rtmp_services_.find(service_id);
    if (it == rtmp_services_.end()) {
        error = "rtmp service not found";
        return false;
    }

    RtmpServiceRecord& service = it->second;
    service.managed_status = managed_status;
    service.runtime_status = runtime_status;
    service.updated_at = CurrentUnixTime();
    if (patch.set_last_error) {
        service.last_error = patch.last_error;
    }
    if (patch.set_last_running_at) {
        service.last_publisher_connected_at = patch.last_running_at;
    }
    if (patch.set_last_exit_at) {
        service.last_publisher_disconnected_at = patch.last_exit_at;
    }
    if (patch.set_activated_at) {
        service.activated_at = patch.activated_at;
    }
    if (patch.set_stopped_at) {
        service.stopped_at = patch.stopped_at;
    }
    if (patch.append_log && !patch.log_line.empty()) {
        AppendRtmpServiceLogLocked(service, patch.log_line);
    }
    if (!PersistLocked(error)) {
        return false;
    }
    out = service;
    return true;
}

bool PersistentStore::AppendRoomLog(const std::string& room_id, const std::string& line, RoomRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rooms_.find(room_id);
    if (it == rooms_.end()) {
        error = "room not found";
        return false;
    }

    RoomRecord& room = it->second;
    room.updated_at = CurrentUnixTime();
    AppendRoomLogLocked(room, line);
    if (!PersistLocked(error)) {
        return false;
    }
    out = room;
    return true;
}

bool PersistentStore::SetRoomLatestLog(const std::string& room_id, const std::string& line, RoomRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rooms_.find(room_id);
    if (it == rooms_.end()) {
        error = "room not found";
        return false;
    }

    RoomRecord& room = it->second;
    room.updated_at = CurrentUnixTime();
    room.latest_log = line;
    if (!PersistLocked(error)) {
        return false;
    }
    out = room;
    return true;
}

bool PersistentStore::AppendRtmpServiceLog(const std::string& service_id, const std::string& line, RtmpServiceRecord& out,
                                           std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rtmp_services_.find(service_id);
    if (it == rtmp_services_.end()) {
        error = "rtmp service not found";
        return false;
    }

    RtmpServiceRecord& service = it->second;
    service.updated_at = CurrentUnixTime();
    AppendRtmpServiceLogLocked(service, line);
    if (!PersistLocked(error)) {
        return false;
    }
    out = service;
    return true;
}

bool PersistentStore::MarkRoomPublishStarted(const std::string& stream_name, const std::string& client_ip, RoomRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : rooms_) {
        RoomRecord& room = entry.second;
        if (room.stream_name != stream_name) {
            continue;
        }
        const std::int64_t now = CurrentUnixTime();
        room.runtime_status = "running";
        room.last_error.clear();
        room.last_start_attempt_at = now;
        room.last_running_at = now;
        room.updated_at = now;
        AppendRoomLogLocked(room, "RTMP publisher connected from " + client_ip);
        if (!PersistLocked(error)) {
            return false;
        }
        out = room;
        return true;
    }
    error = "room not found";
    return false;
}

bool PersistentStore::MarkRtmpServicePublishStarted(const std::string& stream_name, const std::string& client_ip, RtmpServiceRecord& out,
                                                    std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : rtmp_services_) {
        RtmpServiceRecord& service = entry.second;
        if (service.stream_name != stream_name) {
            continue;
        }
        const std::int64_t now = CurrentUnixTime();
        service.runtime_status = "running";
        service.last_error.clear();
        service.last_publisher_connected_at = now;
        service.updated_at = now;
        AppendRtmpServiceLogLocked(service, "RTMP publisher connected from " + client_ip);
        if (!PersistLocked(error)) {
            return false;
        }
        out = service;
        return true;
    }
    error = "rtmp service not found";
    return false;
}

bool PersistentStore::MarkRoomPublishStopped(const std::string& stream_name, const std::string& reason, RoomRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : rooms_) {
        RoomRecord& room = entry.second;
        if (room.stream_name != stream_name) {
            continue;
        }
        const std::int64_t now = CurrentUnixTime();
        room.runtime_status = room.managed_status == "active" ? "idle" : "idle";
        room.last_exit_at = now;
        room.updated_at = now;
        AppendRoomLogLocked(room, "RTMP publisher disconnected: " + reason);
        if (!PersistLocked(error)) {
            return false;
        }
        out = room;
        return true;
    }
    error = "room not found";
    return false;
}

bool PersistentStore::MarkRtmpServicePublishStopped(const std::string& stream_name, const std::string& reason, RtmpServiceRecord& out,
                                                    std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : rtmp_services_) {
        RtmpServiceRecord& service = entry.second;
        if (service.stream_name != stream_name) {
            continue;
        }
        const std::int64_t now = CurrentUnixTime();
        service.runtime_status = "idle";
        service.last_publisher_disconnected_at = now;
        service.updated_at = now;
        AppendRtmpServiceLogLocked(service, "RTMP publisher disconnected: " + reason);
        if (!PersistLocked(error)) {
            return false;
        }
        out = service;
        return true;
    }
    error = "rtmp service not found";
    return false;
}

bool PersistentStore::AppendRoomLogByStreamName(const std::string& stream_name, const std::string& line, RoomRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : rooms_) {
        RoomRecord& room = entry.second;
        if (room.stream_name != stream_name) {
            continue;
        }
        room.updated_at = CurrentUnixTime();
        AppendRoomLogLocked(room, line);
        if (!PersistLocked(error)) {
            return false;
        }
        out = room;
        return true;
    }
    error = "room not found";
    return false;
}

bool PersistentStore::AppendRtmpServiceLogByStreamName(const std::string& stream_name, const std::string& line, RtmpServiceRecord& out,
                                                       std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : rtmp_services_) {
        RtmpServiceRecord& service = entry.second;
        if (service.stream_name != stream_name) {
            continue;
        }
        service.updated_at = CurrentUnixTime();
        AppendRtmpServiceLogLocked(service, line);
        if (!PersistLocked(error)) {
            return false;
        }
        out = service;
        return true;
    }
    error = "rtmp service not found";
    return false;
}

StorageUsage PersistentStore::UsageForUser(const UserRecord& user) const {
    std::lock_guard<std::mutex> lock(mutex_);
    StorageUsage usage;
    usage.max_storage_bytes = user.max_storage_bytes;
    usage.max_active_rooms = user.max_active_rooms;
    for (const auto& entry : files_) {
        if (entry.second.user_id == user.id && entry.second.status == "ready") {
            usage.used_storage_bytes += entry.second.size_bytes;
        }
    }
    for (const auto& entry : uploads_) {
        if (entry.second.user_id == user.id && entry.second.status == "pending") {
            usage.reserved_storage_bytes += entry.second.size_bytes;
        }
    }
    for (const auto& entry : rooms_) {
        if (entry.second.owner_id == user.id && entry.second.managed_status == "active") {
            ++usage.active_room_count;
        }
    }
    usage.available_storage_bytes = std::max<std::int64_t>(0, usage.max_storage_bytes - usage.used_storage_bytes - usage.reserved_storage_bytes);
    usage.available_room_slots = std::max(0, usage.max_active_rooms - usage.active_room_count);
    return usage;
}

std::vector<RoomRecord> PersistentStore::ListRooms(bool admin, const std::string& user_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<RoomRecord> items;
    for (const auto& entry : rooms_) {
        if (!admin && entry.second.owner_id != user_id) {
            continue;
        }
        items.push_back(entry.second);
    }
    std::sort(items.begin(), items.end(), [](const RoomRecord& left, const RoomRecord& right) {
        return left.created_at > right.created_at;
    });
    return items;
}

std::vector<RtmpServiceRecord> PersistentStore::ListRtmpServices(bool admin, const std::string& user_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<RtmpServiceRecord> items;
    for (const auto& entry : rtmp_services_) {
        if (!admin && entry.second.owner_id != user_id) {
            continue;
        }
        items.push_back(entry.second);
    }
    std::sort(items.begin(), items.end(), [](const RtmpServiceRecord& left, const RtmpServiceRecord& right) {
        return left.created_at > right.created_at;
    });
    return items;
}

std::vector<FileRecord> PersistentStore::ListFiles(bool admin, const std::string& user_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<FileRecord> items;
    for (const auto& entry : files_) {
        if (!admin && entry.second.user_id != user_id) {
            continue;
        }
        if (entry.second.status != "ready") {
            continue;
        }
        items.push_back(entry.second);
    }
    std::sort(items.begin(), items.end(), [](const FileRecord& left, const FileRecord& right) {
        return left.created_at > right.created_at;
    });
    return items;
}

std::vector<UploadRecord> PersistentStore::ListUploads(bool admin, const std::string& user_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<UploadRecord> items;
    for (const auto& entry : uploads_) {
        if (!admin && entry.second.user_id != user_id) {
            continue;
        }
        items.push_back(entry.second);
    }
    std::sort(items.begin(), items.end(), [](const UploadRecord& left, const UploadRecord& right) {
        return left.created_at > right.created_at;
    });
    return items;
}

bool PersistentStore::GetUploadByID(const std::string& upload_id, UploadRecord& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = uploads_.find(upload_id);
    if (it == uploads_.end()) {
        return false;
    }
    out = it->second;
    return true;
}

bool PersistentStore::CreateUploadSession(const UserRecord& owner, const std::string& original_name, const std::string& display_name,
                                          const std::string& remark, const std::string& content_hash, std::int64_t size_bytes,
                                          std::int64_t chunk_size, int total_chunks, const std::string& temp_dir,
                                          std::int64_t expires_at, UploadRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::int64_t now = CurrentUnixTime();

    UploadRecord upload;
    upload.id = GenerateID("upl_", 10);
    upload.user_id = owner.id;
    upload.owner_name = owner.username;
    upload.original_name = original_name;
    upload.display_name = display_name.empty() ? original_name : display_name;
    upload.remark = remark;
    upload.content_hash = content_hash;
    upload.size_bytes = size_bytes;
    upload.chunk_size = chunk_size;
    upload.total_chunks = total_chunks;
    upload.uploaded_chunk_count = 0;
    upload.uploaded_parts.clear();
    upload.status = "pending";
    upload.temp_dir = temp_dir;
    upload.completed_file_id.clear();
    upload.created_at = now;
    upload.updated_at = now;
    upload.expires_at = expires_at;

    uploads_[upload.id] = upload;
    if (!PersistLocked(error)) {
        uploads_.erase(upload.id);
        return false;
    }
    out = upload;
    return true;
}

bool PersistentStore::AddUploadPart(const std::string& upload_id, int part_number, std::int64_t updated_at, UploadRecord& out,
                                    std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = uploads_.find(upload_id);
    if (it == uploads_.end()) {
        error = "upload not found";
        return false;
    }

    UploadRecord& upload = it->second;
    if (std::find(upload.uploaded_parts.begin(), upload.uploaded_parts.end(), part_number) == upload.uploaded_parts.end()) {
        upload.uploaded_parts.push_back(part_number);
        std::sort(upload.uploaded_parts.begin(), upload.uploaded_parts.end());
    }
    upload.uploaded_chunk_count = static_cast<int>(upload.uploaded_parts.size());
    upload.updated_at = updated_at;
    if (!PersistLocked(error)) {
        return false;
    }
    out = upload;
    return true;
}

bool PersistentStore::CompleteUpload(const std::string& upload_id, const std::string& stored_path, const std::string& content_hash,
                                     FileRecord& file_out, UploadRecord& upload_out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = uploads_.find(upload_id);
    if (it == uploads_.end()) {
        error = "upload not found";
        return false;
    }

    UploadRecord& upload = it->second;
    if (upload.status != "pending") {
        error = "upload is not pending";
        return false;
    }

    FileRecord file;
    file.id = GenerateID("file_", 10);
    file.user_id = upload.user_id;
    file.owner_name = upload.owner_name;
    file.original_name = upload.original_name;
    file.display_name = upload.display_name.empty() ? upload.original_name : upload.display_name;
    file.remark = upload.remark;
    file.stored_path = stored_path;
    file.size_bytes = upload.size_bytes;
    file.content_hash = content_hash;
    file.status = "ready";
    file.created_at = CurrentUnixTime();
    file.updated_at = file.created_at;

    upload.status = "completed";
    upload.completed_file_id = file.id;
    upload.updated_at = file.updated_at;
    upload.expires_at = 0;
    upload.temp_dir.clear();

    files_[file.id] = file;
    if (!PersistLocked(error)) {
        files_.erase(file.id);
        return false;
    }

    file_out = file;
    upload_out = upload;
    return true;
}

bool PersistentStore::CreateUploadedFile(const UserRecord& owner, const std::string& original_name, const std::string& display_name,
                                         const std::string& remark, const std::string& stored_path, std::int64_t size_bytes,
                                         const std::string& content_hash, FileRecord& file_out, UploadRecord& upload_out,
                                         std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::int64_t now = CurrentUnixTime();

    FileRecord file;
    file.id = GenerateID("file_", 10);
    file.user_id = owner.id;
    file.owner_name = owner.username;
    file.original_name = original_name;
    file.display_name = display_name.empty() ? original_name : display_name;
    file.remark = remark;
    file.stored_path = stored_path;
    file.size_bytes = size_bytes;
    file.content_hash = content_hash;
    file.status = "ready";
    file.created_at = now;
    file.updated_at = now;

    UploadRecord upload;
    upload.id = GenerateID("upl_", 10);
    upload.user_id = owner.id;
    upload.owner_name = owner.username;
    upload.original_name = original_name;
    upload.display_name = display_name.empty() ? original_name : display_name;
    upload.remark = remark;
    upload.content_hash = content_hash;
    upload.size_bytes = size_bytes;
    upload.chunk_size = size_bytes;
    upload.total_chunks = 1;
    upload.uploaded_chunk_count = 1;
    upload.uploaded_parts = {0};
    upload.status = "completed";
    upload.temp_dir.clear();
    upload.completed_file_id = file.id;
    upload.created_at = now;
    upload.updated_at = now;
    upload.expires_at = 0;

    files_[file.id] = file;
    uploads_[upload.id] = upload;
    if (!PersistLocked(error)) {
        files_.erase(file.id);
        uploads_.erase(upload.id);
        return false;
    }

    file_out = file;
    upload_out = upload;
    return true;
}

bool PersistentStore::AbortUpload(const std::string& upload_id, UploadRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = uploads_.find(upload_id);
    if (it == uploads_.end()) {
        error = "upload not found";
        return false;
    }

    UploadRecord& upload = it->second;
    if (upload.status == "completed") {
        error = "completed upload cannot be aborted";
        return false;
    }
    if (upload.status == "aborted") {
        out = upload;
        return true;
    }

    upload.status = "aborted";
    upload.updated_at = CurrentUnixTime();
    upload.expires_at = 0;
    if (!PersistLocked(error)) {
        return false;
    }
    out = upload;
    return true;
}

bool PersistentStore::DeleteUpload(const std::string& upload_id, UploadRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = uploads_.find(upload_id);
    if (it == uploads_.end()) {
        error = "upload not found";
        return false;
    }
    out = it->second;
    uploads_.erase(it);
    return PersistLocked(error);
}

int PersistentStore::CleanupUploads(std::int64_t now, std::vector<UploadRecord>& removed_uploads, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    int removed = 0;
    removed_uploads.clear();
    for (auto it = uploads_.begin(); it != uploads_.end();) {
        const UploadRecord& upload = it->second;
        const bool expired_pending = upload.status == "pending" && upload.expires_at > 0 && upload.expires_at <= now;
        const bool aborted = upload.status == "aborted";
        if (!expired_pending && !aborted) {
            ++it;
            continue;
        }
        removed_uploads.push_back(upload);
        it = uploads_.erase(it);
        ++removed;
    }
    if (!PersistLocked(error)) {
        return -1;
    }
    return removed;
}

bool PersistentStore::CreateAuthSession(const AuthSessionRecord& session, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    sessions_[session.token] = session;
    return PersistLocked(error);
}

bool PersistentStore::GetAuthSession(const std::string& token, AuthSessionRecord& out, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = sessions_.find(token);
    if (it == sessions_.end()) {
        return false;
    }
    const std::int64_t now = CurrentUnixTime();
    if (it->second.expires_at <= now) {
        sessions_.erase(it);
        PersistLocked(error);
        return false;
    }
    out = it->second;
    return true;
}

bool PersistentStore::DeleteAuthSession(const std::string& token, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    sessions_.erase(token);
    return PersistLocked(error);
}

bool PersistentStore::DeleteAuthSessionsForUser(const std::string& user_id, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        if (it->second.user_id == user_id) {
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
    return PersistLocked(error);
}

bool PersistentStore::DeleteExpiredAuthSessions(std::int64_t now, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        if (it->second.expires_at <= now) {
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
    return PersistLocked(error);
}

bool PersistentStore::PersistLocked(std::string& error) const {
    try {
        const std::filesystem::path db_path(path_);
        if (db_path.has_parent_path()) {
            std::filesystem::create_directories(db_path.parent_path());
        }
    } catch (const std::exception& ex) {
        error = ex.what();
        return false;
    }

    std::ofstream output(path_, std::ios::trunc);
    if (!output.is_open()) {
        error = "cannot write database: " + path_;
        return false;
    }

    output << "VERSION|2\n";
    for (const auto& entry : users_) {
        const auto& user = entry.second;
        output << "USER|" << EscapeField(user.id) << "|" << EscapeField(user.username) << "|" << EscapeField(user.password)
               << "|" << EscapeField(user.role) << "|" << EscapeField(user.status) << "|" << user.max_storage_bytes
               << "|" << user.max_active_rooms << "|" << user.subscription_ends_at << "|" << user.created_at << "\n";
    }
    for (const auto& entry : rooms_) {
        const auto& room = entry.second;
        output << "ROOMV5|" << EscapeField(room.id) << "|" << EscapeField(room.name) << "|" << EscapeField(room.owner_id)
               << "|" << EscapeField(room.owner_name) << "|" << EscapeField(room.stream_name) << "|"
               << EscapeField(room.publish_key) << "|" << EscapeField(room.play_key) << "|" << EscapeField(room.mode)
               << "|" << EscapeField(room.input_url) << "|" << EscapeField(room.file_id) << "|"
               << "|" << EscapeField(room.rtmp_url) << "|"
               << EscapeField(room.managed_status) << "|"
               << EscapeField(room.runtime_status) << "|" << EscapeField(room.last_error) << "|" << room.retry_count
               << "|" << room.next_retry_at << "|" << room.last_start_attempt_at << "|" << room.last_running_at
               << "|" << room.last_exit_at << "|" << room.created_at << "|" << room.updated_at << "|"
               << room.activated_at << "|" << room.stopped_at << "|" << room.log_line_count << "|"
               << EscapeField(room.latest_log) << "|" << EscapeField(JoinLogs(room.recent_logs)) << "\n";
    }
    for (const auto& entry : rtmp_services_) {
        const auto& service = entry.second;
        output << "RTMPSERVICEV2|" << EscapeField(service.id) << "|" << EscapeField(service.name) << "|"
               << EscapeField(service.owner_id) << "|" << EscapeField(service.owner_name) << "|"
               << EscapeField(service.stream_name) << "|" << EscapeField(service.publish_key) << "|"
               << EscapeField(service.play_key) << "|" << EscapeField(service.source_url) << "|"
               << EscapeField(service.managed_status) << "|" << EscapeField(service.runtime_status) << "|"
               << EscapeField(service.last_error) << "|" << service.created_at << "|" << service.updated_at << "|"
               << service.activated_at << "|" << service.stopped_at << "|" << service.last_publisher_connected_at << "|"
               << service.last_publisher_disconnected_at << "|" << service.log_line_count << "|"
               << EscapeField(service.latest_log) << "|" << EscapeField(JoinLogs(service.recent_logs)) << "\n";
    }
    for (const auto& entry : files_) {
        const auto& file = entry.second;
        output << "FILEV3|" << EscapeField(file.id) << "|" << EscapeField(file.user_id) << "|" << EscapeField(file.owner_name)
               << "|" << EscapeField(file.original_name) << "|" << EscapeField(file.display_name) << "|" << EscapeField(file.remark)
               << "|" << EscapeField(file.stored_path) << "|" << file.size_bytes << "|" << EscapeField(file.content_hash)
               << "|" << EscapeField(file.status) << "|" << file.created_at << "|" << file.updated_at << "\n";
    }
    for (const auto& entry : uploads_) {
        const auto& upload = entry.second;
        output << "UPLOADV3|" << EscapeField(upload.id) << "|" << EscapeField(upload.user_id) << "|" << EscapeField(upload.owner_name)
               << "|" << EscapeField(upload.original_name) << "|" << EscapeField(upload.display_name) << "|"
               << EscapeField(upload.remark) << "|" << EscapeField(upload.content_hash) << "|" << upload.size_bytes
               << "|" << upload.chunk_size << "|" << upload.total_chunks << "|" << upload.uploaded_chunk_count << "|"
               << EscapeField(JoinInts(upload.uploaded_parts)) << "|" << EscapeField(upload.status) << "|"
               << EscapeField(upload.temp_dir) << "|" << EscapeField(upload.completed_file_id) << "|"
               << upload.created_at << "|" << upload.updated_at << "|" << upload.expires_at << "\n";
    }
    for (const auto& entry : sessions_) {
        const auto& session = entry.second;
        if (session.expires_at <= CurrentUnixTime()) {
            continue;
        }
        output << "SESSION|" << EscapeField(session.token) << "|" << EscapeField(session.user_id) << "|"
               << session.created_at << "|" << session.expires_at << "\n";
    }

    return true;
}

std::int64_t PersistentStore::CurrentUnixTime() {
    return static_cast<std::int64_t>(std::time(nullptr));
}

std::string PersistentStore::GenerateID(const std::string& prefix, std::size_t length) {
    static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    thread_local std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<std::size_t> dist(0, sizeof(alphabet) - 2);

    std::string out = prefix;
    out.reserve(prefix.size() + length);
    for (std::size_t i = 0; i < length; ++i) {
        out.push_back(alphabet[dist(rng)]);
    }
    return out;
}

bool PersistentStore::CheckPassword(const std::string& stored_password, const std::string& password) {
    const std::string prefix = "plain:";
    if (stored_password.rfind(prefix, 0) == 0) {
        return stored_password.substr(prefix.size()) == password;
    }
    return stored_password == password;
}

void PersistentStore::AppendRoomLogLocked(RoomRecord& room, const std::string& line) const {
    ++room.log_line_count;
    room.latest_log = line;
    room.recent_logs.push_back(line);
    if (room.recent_logs.size() > kRoomLogRetention) {
        room.recent_logs.erase(room.recent_logs.begin(), room.recent_logs.begin() + static_cast<std::ptrdiff_t>(room.recent_logs.size() - kRoomLogRetention));
    }
}

void PersistentStore::AppendRtmpServiceLogLocked(RtmpServiceRecord& service, const std::string& line) const {
    ++service.log_line_count;
    service.latest_log = line;
    service.recent_logs.push_back(line);
    if (service.recent_logs.size() > kRoomLogRetention) {
        service.recent_logs.erase(service.recent_logs.begin(),
                                  service.recent_logs.begin() + static_cast<std::ptrdiff_t>(service.recent_logs.size() - kRoomLogRetention));
    }
}
