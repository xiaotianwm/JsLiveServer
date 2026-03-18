#pragma once

#include "../../config/config.h"
#include "../../storage/persistence/store.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

class RoomTaskManager {
public:
    RoomTaskManager(const ServerConfig& config, PersistentStore& store);
    ~RoomTaskManager();

    bool Recover(std::string& error);
    bool StartRoom(const std::string& room_id, RoomRecord& out, std::string& error);
    bool StopRoom(const std::string& room_id, RoomRecord& out, std::string& error);
    bool DeleteRoom(const std::string& room_id, std::string& error);
    void Shutdown();

private:
    struct OutputTracker;
    struct RunningRoom;

    bool ValidateRoomForManagedStart(const RoomRecord& room, std::string& error) const;
    bool LaunchRoom(const std::string& room_id, RoomRecord& out, std::string& error);
    bool MoveRoomToRetry(const std::string& room_id, const std::string& last_error, std::int64_t last_exit_at,
                         RoomRecord* out, std::string& error);
    bool MoveRoomToFailed(const std::string& room_id, const std::string& last_error, RoomRecord* out, std::string& error);
    bool WaitForIdle(const std::string& room_id, RoomRecord& out, std::string& error);
    void StartOutputReader(const std::string& room_id, const std::shared_ptr<RunningRoom>& running);
    void WaitForProcessExit(const std::string& room_id, const std::shared_ptr<RunningRoom>& running);
    void HandleOutputLine(const std::string& room_id, const std::shared_ptr<RunningRoom>& running, const std::string& line);
    void ArmRetryTimer(const std::string& room_id, std::uint64_t token, std::int64_t delay_seconds);
    std::uint64_t InvalidateRetryToken(const std::string& room_id);

    ServerConfig config_;
    PersistentStore& store_;
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<RunningRoom>> running_;
    std::unordered_map<std::string, std::uint64_t> retry_tokens_;
    bool shutting_down_ = false;
};
