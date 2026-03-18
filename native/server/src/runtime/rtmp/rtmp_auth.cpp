#include "rtmp_auth.h"

namespace {

std::string ResolveManagedKey(const RtmpAuthRequest& request) {
    const char* candidates[] = {"roomKey", "publishKey", "playKey", "key", "token", "auth"};
    for (const char* candidate : candidates) {
        const auto it = request.query.find(candidate);
        if (it != request.query.end() && !it->second.empty()) {
            return it->second;
        }
    }
    return request.token;
}

}  // namespace

RtmpAuthenticator::RtmpAuthenticator(const PersistentStore& store) : store_(store) {}

RtmpAuthResult RtmpAuthenticator::Authorize(const RtmpAuthRequest& request) const {
    RoomRecord room;
    std::string error;
    if (store_.AuthorizeRoomStream(request.stream, request.action, ResolveManagedKey(request), room, error)) {
        RtmpAuthResult result;
        result.allow = true;
        result.code = "OK";
        result.message = "stream key accepted";
        result.room = room;
        return result;
    }

    RtmpServiceRecord service;
    if (store_.AuthorizeRtmpServiceStream(request.stream, request.action, ResolveManagedKey(request), service, error)) {
        RtmpAuthResult result;
        result.allow = true;
        result.code = "OK";
        result.message = "stream key accepted";
        return result;
    }

    RtmpAuthResult denied;
    denied.allow = false;
    denied.code = "STREAM_DENIED";
    denied.message = error;
    return denied;
}
