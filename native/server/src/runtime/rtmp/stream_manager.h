#pragma once

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

struct MediaPacket {
    std::uint8_t type = 0;
    std::uint32_t timestamp = 0;
    std::vector<std::uint8_t> payload;
};

class StreamSink {
public:
    virtual ~StreamSink() = default;
    virtual bool SendMediaPacket(std::uint8_t type, std::uint32_t timestamp, const std::vector<std::uint8_t>& payload) = 0;
    virtual std::string SessionKey() const = 0;
    virtual void ForceClose(const std::string& reason) = 0;
};

struct StreamInfoSnapshot {
    std::string app;
    std::string stream;
    bool has_publisher = false;
    std::string publisher_session;
    std::string publisher_ip;
    std::size_t player_count = 0;
    std::size_t gop_cache_entries = 0;
    std::time_t created_at = 0;
};

class StreamManager {
public:
    bool RegisterPublisher(const std::string& app, const std::string& stream, const std::shared_ptr<StreamSink>& publisher,
                           const std::string& publisher_ip, std::size_t cache_limit, std::string& error);
    void UnregisterPublisher(const std::string& app, const std::string& stream, const std::string& session_key);

    void AddPlayer(const std::string& app, const std::string& stream, const std::shared_ptr<StreamSink>& player,
                   std::vector<MediaPacket>& bootstrap_packets);
    void RemovePlayer(const std::string& app, const std::string& stream, const std::string& session_key);

    std::vector<std::shared_ptr<StreamSink>> OnPublisherPacket(const std::string& app, const std::string& stream,
                                                               const MediaPacket& packet);
    std::size_t KickRoom(const std::string& app, const std::string& stream, const std::string& reason);

    std::vector<StreamInfoSnapshot> SnapshotAll() const;
    bool SnapshotOne(const std::string& app, const std::string& stream, StreamInfoSnapshot& out) const;

private:
    struct StreamEntry {
        std::string app;
        std::string stream;
        std::weak_ptr<StreamSink> publisher;
        std::string publisher_session;
        std::string publisher_ip;
        std::vector<std::weak_ptr<StreamSink>> players;
        std::vector<MediaPacket> gop_cache;
        MediaPacket metadata;
        bool has_metadata = false;
        MediaPacket audio_sequence_header;
        bool has_audio_sequence_header = false;
        MediaPacket video_sequence_header;
        bool has_video_sequence_header = false;
        std::size_t cache_limit = 64;
        std::time_t created_at = 0;
    };

    static std::string MakeKey(const std::string& app, const std::string& stream);
    static bool IsVideoKeyFrame(const MediaPacket& packet);
    static bool IsAvcSequenceHeader(const MediaPacket& packet);
    static bool IsAacSequenceHeader(const MediaPacket& packet);
    void CleanupLocked(StreamEntry& entry);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, StreamEntry> streams_;
};
