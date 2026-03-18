#pragma once

#include "../../config/config.h"
#include "../../storage/persistence/store.h"

#include <string>

std::string ResolveRtmpHost(const ServerConfig& config);
std::string BuildPublishUrl(const ServerConfig& config, const RoomRecord& room);
std::string BuildPlayUrl(const ServerConfig& config, const RoomRecord& room);
std::string BuildServiceUrl(const ServerConfig& config, const RtmpServiceRecord& service);
