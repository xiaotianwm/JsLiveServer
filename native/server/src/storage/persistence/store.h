#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

struct StorageUsage {
    std::int64_t used_storage_bytes = 0;
    std::int64_t reserved_storage_bytes = 0;
    std::int64_t max_storage_bytes = 0;
    std::int64_t available_storage_bytes = 0;
    int active_room_count = 0;
    int max_active_rooms = 0;
    int available_room_slots = 0;
};

struct UserRecord {
    std::string id;
    std::string username;
    std::string password;
    std::string role;
    std::string status;
    std::int64_t max_storage_bytes = 0;
    int max_active_rooms = 0;
    std::int64_t subscription_ends_at = 0;
    std::int64_t created_at = 0;
};

struct RoomRecord {
    std::string id;
    std::string name;
    std::string owner_id;
    std::string owner_name;
    std::string stream_name;
    std::string publish_key;
    std::string play_key;
    std::string mode;
    std::string input_url;
    std::string file_id;
    std::string rtmp_url;
    std::string managed_status;
    std::string runtime_status;
    std::string last_error;
    int retry_count = 0;
    std::int64_t next_retry_at = 0;
    std::int64_t last_start_attempt_at = 0;
    std::int64_t last_running_at = 0;
    std::int64_t last_exit_at = 0;
    std::int64_t created_at = 0;
    std::int64_t updated_at = 0;
    std::int64_t activated_at = 0;
    std::int64_t stopped_at = 0;
    int log_line_count = 0;
    std::string latest_log;
    std::vector<std::string> recent_logs;
};

struct RoomStatePatch {
    bool set_last_error = false;
    std::string last_error;
    bool set_retry_count = false;
    int retry_count = 0;
    bool set_next_retry_at = false;
    std::int64_t next_retry_at = 0;
    bool set_last_start_attempt_at = false;
    std::int64_t last_start_attempt_at = 0;
    bool set_last_running_at = false;
    std::int64_t last_running_at = 0;
    bool set_last_exit_at = false;
    std::int64_t last_exit_at = 0;
    bool set_activated_at = false;
    std::int64_t activated_at = 0;
    bool set_stopped_at = false;
    std::int64_t stopped_at = 0;
    bool append_log = false;
    std::string log_line;
};

struct RtmpServiceRecord {
    std::string id;
    std::string name;
    std::string owner_id;
    std::string owner_name;
    std::string stream_name;
    std::string publish_key;
    std::string play_key;
    std::string source_url;
    std::string managed_status;
    std::string runtime_status;
    std::string last_error;
    std::int64_t created_at = 0;
    std::int64_t updated_at = 0;
    std::int64_t activated_at = 0;
    std::int64_t stopped_at = 0;
    std::int64_t last_publisher_connected_at = 0;
    std::int64_t last_publisher_disconnected_at = 0;
    int log_line_count = 0;
    std::string latest_log;
    std::vector<std::string> recent_logs;
};

struct FileRecord {
    std::string id;
    std::string user_id;
    std::string owner_name;
    std::string original_name;
    std::string display_name;
    std::string remark;
    std::string stored_path;
    std::int64_t size_bytes = 0;
    std::string content_hash;
    std::string status;
    std::int64_t created_at = 0;
    std::int64_t updated_at = 0;
};

struct UploadRecord {
    std::string id;
    std::string user_id;
    std::string owner_name;
    std::string original_name;
    std::string display_name;
    std::string remark;
    std::string content_hash;
    std::int64_t size_bytes = 0;
    std::int64_t chunk_size = 0;
    int total_chunks = 0;
    int uploaded_chunk_count = 0;
    std::vector<int> uploaded_parts;
    std::string status;
    std::string temp_dir;
    std::string completed_file_id;
    std::int64_t created_at = 0;
    std::int64_t updated_at = 0;
    std::int64_t expires_at = 0;
};

struct AuthSessionRecord {
    std::string token;
    std::string user_id;
    std::int64_t created_at = 0;
    std::int64_t expires_at = 0;
};

class PersistentStore {
public:
    bool Load(const std::string& path, std::string& error);
    bool SeedDefaultsIfEmpty(std::string& error);

    bool FindUserByCredentials(const std::string& username, const std::string& password, UserRecord& out) const;
    bool GetUserByID(const std::string& user_id, UserRecord& out) const;
    bool GetUserByUsername(const std::string& username, UserRecord& out) const;
    bool VerifyUserPassword(const std::string& user_id, const std::string& password) const;
    std::vector<UserRecord> ListUsers() const;
    bool CreateUser(UserRecord& user, std::string& error);
    bool UpdateUser(const UserRecord& user, std::string& error);
    bool GetRoomByID(const std::string& room_id, RoomRecord& out) const;
    bool GetRtmpServiceByID(const std::string& service_id, RtmpServiceRecord& out) const;
    bool GetFileByID(const std::string& file_id, FileRecord& out) const;
    bool AuthorizeRoomStream(const std::string& stream_name, const std::string& action, const std::string& key,
                             RoomRecord& out, std::string& error) const;
    bool AuthorizeRtmpServiceStream(const std::string& stream_name, const std::string& action, const std::string& key,
                                    RtmpServiceRecord& out, std::string& error) const;
    bool CreateRoom(const UserRecord& owner, const std::string& name, const std::string& mode, const std::string& input_url,
                    const std::string& file_id, const std::string& rtmp_url, RoomRecord& out,
                    std::string& error);
    bool UpdateRoom(const RoomRecord& room, std::string& error);
    bool DeleteRoom(const std::string& room_id, std::string& error);
    bool CreateRtmpService(const UserRecord& owner, const std::string& name, const std::string& source_url, RtmpServiceRecord& out,
                           std::string& error);
    bool UpdateRtmpService(const RtmpServiceRecord& service, std::string& error);
    bool DeleteRtmpService(const std::string& service_id, std::string& error);
    bool RemoveFileRecord(const std::string& file_id, FileRecord& out, std::string& error);
    bool SetRoomState(const std::string& room_id, const std::string& managed_status, const std::string& runtime_status,
                      const RoomStatePatch& patch, RoomRecord& out, std::string& error);
    bool AppendRoomLog(const std::string& room_id, const std::string& line, RoomRecord& out, std::string& error);
    bool SetRoomLatestLog(const std::string& room_id, const std::string& line, RoomRecord& out, std::string& error);
    bool MarkRoomPublishStarted(const std::string& stream_name, const std::string& client_ip, RoomRecord& out, std::string& error);
    bool MarkRoomPublishStopped(const std::string& stream_name, const std::string& reason, RoomRecord& out, std::string& error);
    bool AppendRoomLogByStreamName(const std::string& stream_name, const std::string& line, RoomRecord& out, std::string& error);
    bool SetRtmpServiceState(const std::string& service_id, const std::string& managed_status, const std::string& runtime_status,
                             const RoomStatePatch& patch, RtmpServiceRecord& out, std::string& error);
    bool AppendRtmpServiceLog(const std::string& service_id, const std::string& line, RtmpServiceRecord& out, std::string& error);
    bool MarkRtmpServicePublishStarted(const std::string& stream_name, const std::string& client_ip, RtmpServiceRecord& out,
                                       std::string& error);
    bool MarkRtmpServicePublishStopped(const std::string& stream_name, const std::string& reason, RtmpServiceRecord& out,
                                       std::string& error);
    bool AppendRtmpServiceLogByStreamName(const std::string& stream_name, const std::string& line, RtmpServiceRecord& out,
                                          std::string& error);

    StorageUsage UsageForUser(const UserRecord& user) const;
    std::vector<RoomRecord> ListRooms(bool admin, const std::string& user_id) const;
    std::vector<RtmpServiceRecord> ListRtmpServices(bool admin, const std::string& user_id) const;
    std::vector<FileRecord> ListFiles(bool admin, const std::string& user_id) const;
    std::vector<UploadRecord> ListUploads(bool admin, const std::string& user_id) const;
    bool GetUploadByID(const std::string& upload_id, UploadRecord& out) const;
    bool CreateUploadSession(const UserRecord& owner, const std::string& original_name, const std::string& display_name,
                             const std::string& remark, const std::string& content_hash, std::int64_t size_bytes,
                             std::int64_t chunk_size, int total_chunks, const std::string& temp_dir, std::int64_t expires_at,
                             UploadRecord& out, std::string& error);
    bool AddUploadPart(const std::string& upload_id, int part_number, std::int64_t updated_at, UploadRecord& out, std::string& error);
    bool CompleteUpload(const std::string& upload_id, const std::string& stored_path, const std::string& content_hash,
                        FileRecord& file_out, UploadRecord& upload_out, std::string& error);
    bool CreateUploadedFile(const UserRecord& owner, const std::string& original_name, const std::string& display_name,
                            const std::string& remark, const std::string& stored_path, std::int64_t size_bytes,
                            const std::string& content_hash, FileRecord& file_out, UploadRecord& upload_out, std::string& error);
    bool AbortUpload(const std::string& upload_id, UploadRecord& out, std::string& error);
    bool DeleteUpload(const std::string& upload_id, UploadRecord& out, std::string& error);
    int CleanupUploads(std::int64_t now, std::vector<UploadRecord>& removed_uploads, std::string& error);

    bool CreateAuthSession(const AuthSessionRecord& session, std::string& error);
    bool GetAuthSession(const std::string& token, AuthSessionRecord& out, std::string& error);
    bool DeleteAuthSession(const std::string& token, std::string& error);
    bool DeleteAuthSessionsForUser(const std::string& user_id, std::string& error);
    bool DeleteExpiredAuthSessions(std::int64_t now, std::string& error);

private:
    bool PersistLocked(std::string& error) const;
    static std::int64_t CurrentUnixTime();
    static std::string GenerateID(const std::string& prefix, std::size_t length);
    static bool CheckPassword(const std::string& stored_password, const std::string& password);
    void AppendRoomLogLocked(RoomRecord& room, const std::string& line) const;
    void AppendRtmpServiceLogLocked(RtmpServiceRecord& service, const std::string& line) const;

    mutable std::mutex mutex_;
    std::string path_;
    std::map<std::string, UserRecord> users_;
    std::map<std::string, RoomRecord> rooms_;
    std::map<std::string, RtmpServiceRecord> rtmp_services_;
    std::map<std::string, FileRecord> files_;
    std::map<std::string, UploadRecord> uploads_;
    std::map<std::string, AuthSessionRecord> sessions_;
};
