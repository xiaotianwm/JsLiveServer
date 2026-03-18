#pragma once

#include "../../storage/persistence/store.h"

#include <map>
#include <string>

struct RtmpAuthRequest {
    std::string action;
    std::string app;
    std::string stream;
    std::string tc_url;
    std::string client_ip;
    std::string token;
    std::map<std::string, std::string> query;
};

struct RtmpAuthResult {
    bool allow = false;
    std::string code;
    std::string message;
    RoomRecord room;
};

class RtmpAuthenticator {
public:
    explicit RtmpAuthenticator(const PersistentStore& store);

    RtmpAuthResult Authorize(const RtmpAuthRequest& request) const;

private:
    const PersistentStore& store_;
};
