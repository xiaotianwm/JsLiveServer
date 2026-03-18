#include "stream_manager.h"

#include <algorithm>

namespace {

bool SessionMatches(const std::weak_ptr<StreamSink>& weak, const std::string& session_key) {
    const std::shared_ptr<StreamSink> locked = weak.lock();
    return locked && locked->SessionKey() == session_key;
}

}  // namespace

std::string StreamManager::MakeKey(const std::string& app, const std::string& stream) {
    return app + "/" + stream;
}

bool StreamManager::IsVideoKeyFrame(const MediaPacket& packet) {
    if (packet.type != 9 || packet.payload.empty()) {
        return false;
    }
    return (packet.payload[0] >> 4) == 1;
}

bool StreamManager::IsAvcSequenceHeader(const MediaPacket& packet) {
    return packet.type == 9 && packet.payload.size() >= 2 && (packet.payload[0] & 0x0f) == 7 && packet.payload[1] == 0;
}

bool StreamManager::IsAacSequenceHeader(const MediaPacket& packet) {
    return packet.type == 8 && packet.payload.size() >= 2 && ((packet.payload[0] >> 4) & 0x0f) == 10 && packet.payload[1] == 0;
}

void StreamManager::CleanupLocked(StreamEntry& entry) {
    entry.players.erase(std::remove_if(entry.players.begin(), entry.players.end(),
                                       [](const std::weak_ptr<StreamSink>& item) { return item.expired(); }),
                        entry.players.end());

    const std::shared_ptr<StreamSink> publisher = entry.publisher.lock();
    if (!publisher) {
        entry.publisher_session.clear();
        entry.publisher_ip.clear();
    }
}

bool StreamManager::RegisterPublisher(const std::string& app, const std::string& stream,
                                      const std::shared_ptr<StreamSink>& publisher, const std::string& publisher_ip,
                                      std::size_t cache_limit, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    StreamEntry& entry = streams_[MakeKey(app, stream)];
    CleanupLocked(entry);
    if (!entry.publisher.expired()) {
        error = "stream already has an active publisher";
        return false;
    }

    entry.app = app;
    entry.stream = stream;
    entry.publisher = publisher;
    entry.publisher_session = publisher->SessionKey();
    entry.publisher_ip = publisher_ip;
    entry.cache_limit = cache_limit;
    if (entry.created_at == 0) {
        entry.created_at = std::time(nullptr);
    }
    entry.gop_cache.clear();
    entry.has_metadata = false;
    entry.has_audio_sequence_header = false;
    entry.has_video_sequence_header = false;
    return true;
}

void StreamManager::UnregisterPublisher(const std::string& app, const std::string& stream, const std::string& session_key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = streams_.find(MakeKey(app, stream));
    if (it == streams_.end()) {
        return;
    }

    StreamEntry& entry = it->second;
    if (entry.publisher_session == session_key) {
        entry.publisher.reset();
        entry.publisher_session.clear();
        entry.publisher_ip.clear();
        entry.gop_cache.clear();
        entry.has_metadata = false;
        entry.has_audio_sequence_header = false;
        entry.has_video_sequence_header = false;
    }

    CleanupLocked(entry);
    if (entry.players.empty() && entry.publisher.expired()) {
        streams_.erase(it);
    }
}

void StreamManager::AddPlayer(const std::string& app, const std::string& stream, const std::shared_ptr<StreamSink>& player,
                              std::vector<MediaPacket>& bootstrap_packets) {
    std::lock_guard<std::mutex> lock(mutex_);
    StreamEntry& entry = streams_[MakeKey(app, stream)];
    entry.app = app;
    entry.stream = stream;
    if (entry.created_at == 0) {
        entry.created_at = std::time(nullptr);
    }

    CleanupLocked(entry);
    entry.players.push_back(player);

    if (entry.has_metadata) {
        bootstrap_packets.push_back(entry.metadata);
    }
    if (entry.has_audio_sequence_header) {
        bootstrap_packets.push_back(entry.audio_sequence_header);
    }
    if (entry.has_video_sequence_header) {
        bootstrap_packets.push_back(entry.video_sequence_header);
    }
    bootstrap_packets.insert(bootstrap_packets.end(), entry.gop_cache.begin(), entry.gop_cache.end());
}

void StreamManager::RemovePlayer(const std::string& app, const std::string& stream, const std::string& session_key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = streams_.find(MakeKey(app, stream));
    if (it == streams_.end()) {
        return;
    }

    StreamEntry& entry = it->second;
    entry.players.erase(std::remove_if(entry.players.begin(), entry.players.end(),
                                       [&](const std::weak_ptr<StreamSink>& item) {
                                           return item.expired() || SessionMatches(item, session_key);
                                       }),
                        entry.players.end());
    CleanupLocked(entry);
    if (entry.players.empty() && entry.publisher.expired()) {
        streams_.erase(it);
    }
}

std::vector<std::shared_ptr<StreamSink>> StreamManager::OnPublisherPacket(const std::string& app, const std::string& stream,
                                                                          const MediaPacket& packet) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = streams_.find(MakeKey(app, stream));
    if (it == streams_.end()) {
        return {};
    }

    StreamEntry& entry = it->second;
    CleanupLocked(entry);

    if (packet.type == 18) {
        entry.metadata = packet;
        entry.has_metadata = true;
    } else if (IsAacSequenceHeader(packet)) {
        entry.audio_sequence_header = packet;
        entry.has_audio_sequence_header = true;
    } else if (IsAvcSequenceHeader(packet)) {
        entry.video_sequence_header = packet;
        entry.has_video_sequence_header = true;
    }

    if (packet.type == 8 || packet.type == 9 || packet.type == 18) {
        if (packet.type == 9 && IsVideoKeyFrame(packet)) {
            entry.gop_cache.clear();
        }
        entry.gop_cache.push_back(packet);
        if (entry.gop_cache.size() > entry.cache_limit) {
            entry.gop_cache.erase(entry.gop_cache.begin(), entry.gop_cache.begin() + 1);
        }
    }

    std::vector<std::shared_ptr<StreamSink>> players;
    players.reserve(entry.players.size());
    for (const auto& weak : entry.players) {
        const std::shared_ptr<StreamSink> player = weak.lock();
        if (player) {
            players.push_back(player);
        }
    }
    return players;
}

std::size_t StreamManager::KickRoom(const std::string& app, const std::string& stream, const std::string& reason) {
    std::vector<std::shared_ptr<StreamSink>> sessions;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = streams_.find(MakeKey(app, stream));
        if (it == streams_.end()) {
            return 0;
        }

        StreamEntry& entry = it->second;
        CleanupLocked(entry);
        if (const std::shared_ptr<StreamSink> publisher = entry.publisher.lock()) {
            sessions.push_back(publisher);
        }
        for (const auto& weak : entry.players) {
            if (const std::shared_ptr<StreamSink> player = weak.lock()) {
                sessions.push_back(player);
            }
        }
        streams_.erase(it);
    }

    for (const auto& session : sessions) {
        session->ForceClose(reason);
    }
    return sessions.size();
}

std::vector<StreamInfoSnapshot> StreamManager::SnapshotAll() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<StreamInfoSnapshot> snapshots;
    snapshots.reserve(streams_.size());
    for (const auto& item : streams_) {
        const StreamEntry& entry = item.second;
        StreamInfoSnapshot snapshot;
        snapshot.app = entry.app;
        snapshot.stream = entry.stream;
        snapshot.has_publisher = !entry.publisher.expired();
        snapshot.publisher_session = entry.publisher_session;
        snapshot.publisher_ip = entry.publisher_ip;
        for (const auto& weak : entry.players) {
            if (!weak.expired()) {
                ++snapshot.player_count;
            }
        }
        snapshot.gop_cache_entries = entry.gop_cache.size();
        snapshot.created_at = entry.created_at;
        snapshots.push_back(snapshot);
    }
    return snapshots;
}

bool StreamManager::SnapshotOne(const std::string& app, const std::string& stream, StreamInfoSnapshot& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = streams_.find(MakeKey(app, stream));
    if (it == streams_.end()) {
        return false;
    }

    const StreamEntry& entry = it->second;
    out.app = entry.app;
    out.stream = entry.stream;
    out.has_publisher = !entry.publisher.expired();
    out.publisher_session = entry.publisher_session;
    out.publisher_ip = entry.publisher_ip;
    out.player_count = 0;
    for (const auto& weak : entry.players) {
        if (!weak.expired()) {
            ++out.player_count;
        }
    }
    out.gop_cache_entries = entry.gop_cache.size();
    out.created_at = entry.created_at;
    return true;
}
