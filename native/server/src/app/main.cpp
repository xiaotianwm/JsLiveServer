#include "../config/config.h"
#include "../net/net.h"
#include "../runtime/rooms/room_task_manager.h"
#include "../runtime/rtmp/rtmp_auth.h"
#include "../runtime/rtmp/rtmp_server.h"
#include "server.h"
#include "../storage/persistence/store.h"
#include "../runtime/rtmp/stream_manager.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

int main(int argc, char* argv[]) {
    std::filesystem::path executable_dir = std::filesystem::current_path();
    std::filesystem::path config_path;
    if (argc > 0 && argv[0] != nullptr) {
        const std::filesystem::path executable_path = std::filesystem::absolute(argv[0]);
        executable_dir = executable_path.parent_path();
        const std::filesystem::path local_config = executable_path.parent_path() / "server.conf";
        if (std::filesystem::exists(local_config)) {
            config_path = local_config;
        }
    }
    if (config_path.empty() && argc > 1 && argv[1] != nullptr && std::string(argv[1]).empty() == false) {
        config_path = argv[1];
    }
    if (config_path.empty()) {
        config_path = "config/server.conf";
    }

    ServerConfig config;
    std::string error;
    if (!LoadConfigFile(config_path.string(), config, error)) {
        std::cerr << "config error: " << error << std::endl;
        return 1;
    }
    ResolveRuntimePaths(config, executable_dir);

    PersistentStore store;
    if (!store.Load(config.db_path, error)) {
        std::cerr << "store error: " << error << std::endl;
        return 1;
    }
    if (!store.SeedDefaultsIfEmpty(error)) {
        std::cerr << "seed error: " << error << std::endl;
        return 1;
    }

    if (!net::Initialize(error)) {
        std::cerr << "network init error: " << error << std::endl;
        return 1;
    }

    std::cout << "config file: " << config_path.string() << std::endl;
    std::cout << "JsLive native server listening on " << config.Address() << std::endl;
    std::cout << "JsLive RTMP runtime listening on " << config.RtmpAddress() << std::endl;
    std::cout << "seed accounts: admin/admin123, user/user123" << std::endl;

    StreamManager stream_manager;
    RoomTaskManager room_task_manager(config, store);
    RtmpAuthenticator authenticator(store);
    RtmpServer rtmp_server(config, store, stream_manager, authenticator);
    std::thread rtmp_thread([&]() {
        std::string rtmp_error;
        if (!rtmp_server.Run(rtmp_error)) {
            std::cerr << "rtmp server stopped: " << rtmp_error << std::endl;
        }
    });

    if (!room_task_manager.Recover(error)) {
        std::cerr << "recover rooms failed: " << error << std::endl;
        room_task_manager.Shutdown();
        if (rtmp_thread.joinable()) {
            rtmp_thread.join();
        }
        net::Cleanup();
        return 1;
    }

    NativeServer server(config, store, stream_manager, room_task_manager);
    const bool ok = server.Run(error);
    if (!ok) {
        std::cerr << "server stopped: " << error << std::endl;
    }
    room_task_manager.Shutdown();
    if (rtmp_thread.joinable()) {
        rtmp_thread.join();
    }
    net::Cleanup();
    return ok ? 0 : 1;
}
