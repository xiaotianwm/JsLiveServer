#include "room_urls.h"

#include <string>

std::string ResolveRtmpHost(const ServerConfig& config) {
    if (!config.rtmp_host.empty() && config.rtmp_host != "0.0.0.0") {
        return config.rtmp_host;
    }
    return "127.0.0.1";
}

std::string BuildPublishUrl(const ServerConfig& config, const RoomRecord& room) {
    return "rtmp://" + ResolveRtmpHost(config) + ":" + std::to_string(config.rtmp_port) + "/live/" + room.stream_name + "?key=" + room.publish_key;
}

std::string BuildPlayUrl(const ServerConfig& config, const RoomRecord& room) {
    return "rtmp://" + ResolveRtmpHost(config) + ":" + std::to_string(config.rtmp_port) + "/live/" + room.stream_name + "?key=" + room.play_key;
}

std::string BuildServiceUrl(const ServerConfig& config, const RtmpServiceRecord& service) {
    return "rtmp://" + ResolveRtmpHost(config) + ":" + std::to_string(config.rtmp_port) + "/live/" + service.stream_name;
}
