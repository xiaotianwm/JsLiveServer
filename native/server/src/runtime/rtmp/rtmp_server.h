#pragma once

#include "../../config/config.h"
#include "rtmp_auth.h"
#include "../../storage/persistence/store.h"
#include "stream_manager.h"

#include <string>

class RtmpServer {
public:
    RtmpServer(const ServerConfig& config, PersistentStore& store, StreamManager& stream_manager, const RtmpAuthenticator& authenticator);

    bool Run(std::string& error);

private:
    ServerConfig config_;
    PersistentStore& store_;
    StreamManager& stream_manager_;
    const RtmpAuthenticator& authenticator_;
};
