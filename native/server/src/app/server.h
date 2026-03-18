#pragma once

#include "../config/config.h"
#include "../runtime/rooms/room_task_manager.h"
#include "../storage/persistence/store.h"
#include "../runtime/rtmp/stream_manager.h"

#include <atomic>
#include <cstdint>
#include <string>

class NativeServer {
public:
    NativeServer(const ServerConfig& config, PersistentStore& store, StreamManager& stream_manager, RoomTaskManager& room_task_manager);

    bool Run(std::string& error);

private:
    ServerConfig config_;
    PersistentStore& store_;
    StreamManager& stream_manager_;
    RoomTaskManager& room_task_manager_;
    std::int64_t started_at_ = 0;
    std::atomic<int> active_requests_{0};
};
