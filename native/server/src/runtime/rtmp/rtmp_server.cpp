#include "rtmp_server.h"

#include "amf0.h"
#include "../../net/net.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

struct RtmpMessage {
    std::uint32_t timestamp = 0;
    std::uint32_t message_stream_id = 0;
    std::uint32_t chunk_stream_id = 0;
    std::uint8_t type = 0;
    std::vector<std::uint8_t> payload;
};

struct ChunkState {
    bool header_valid = false;
    bool message_active = false;
    std::uint8_t last_fmt = 0;
    bool extended_timestamp = false;
    std::uint32_t timestamp = 0;
    std::uint32_t timestamp_delta = 0;
    std::uint32_t message_length = 0;
    std::uint8_t message_type = 0;
    std::uint32_t message_stream_id = 0;
    std::vector<std::uint8_t> payload;
    std::size_t received = 0;
};

std::atomic<std::uint64_t> g_session_counter{1};

void WriteU24(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>(value & 0xff));
}

void WriteU32BE(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>(value & 0xff));
}

void WriteU32LE(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xff));
}

std::string GetString(const std::vector<Amf0Value>& values, std::size_t index) {
    if (index >= values.size() || values[index].type != Amf0Value::Type::String) {
        return std::string();
    }
    return values[index].string_value;
}

double GetNumber(const std::vector<Amf0Value>& values, std::size_t index) {
    if (index >= values.size() || values[index].type != Amf0Value::Type::Number) {
        return 0.0;
    }
    return values[index].number_value;
}

std::string GetObjectString(const Amf0Value& value, const std::string& key) {
    const Amf0Value* found = FindObjectValue(value, key);
    if (found == nullptr || found->type != Amf0Value::Type::String) {
        return std::string();
    }
    return found->string_value;
}

int HexValue(char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'A' && ch <= 'F') {
        return 10 + (ch - 'A');
    }
    if (ch >= 'a' && ch <= 'f') {
        return 10 + (ch - 'a');
    }
    return -1;
}

std::string UrlDecode(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '%' && i + 2 < value.size()) {
            const int hi = HexValue(value[i + 1]);
            const int lo = HexValue(value[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
                continue;
            }
        }
        if (value[i] == '+') {
            out.push_back(' ');
        } else {
            out.push_back(value[i]);
        }
    }
    return out;
}

std::map<std::string, std::string> ParseQueryString(const std::string& query) {
    std::map<std::string, std::string> out;
    std::size_t start = 0;
    while (start <= query.size()) {
        const std::size_t amp = query.find('&', start);
        const std::string part = query.substr(start, amp == std::string::npos ? std::string::npos : amp - start);
        if (!part.empty()) {
            const std::size_t eq = part.find('=');
            if (eq == std::string::npos) {
                out[UrlDecode(part)] = "";
            } else {
                out[UrlDecode(part.substr(0, eq))] = UrlDecode(part.substr(eq + 1));
            }
        }
        if (amp == std::string::npos) {
            break;
        }
        start = amp + 1;
    }
    return out;
}

std::string ResolveToken(const std::map<std::string, std::string>& query) {
    const char* candidates[] = {"roomKey", "publishKey", "playKey", "key", "token", "auth"};
    for (const char* candidate : candidates) {
        const auto it = query.find(candidate);
        if (it != query.end()) {
            return it->second;
        }
    }
    return std::string();
}

class RtmpSession : public StreamSink, public std::enable_shared_from_this<RtmpSession> {
public:
    RtmpSession(SocketHandle socket, std::string client_ip, const ServerConfig& config, PersistentStore& store,
                StreamManager& stream_manager, const RtmpAuthenticator& authenticator)
        : socket_(socket),
          client_ip_(std::move(client_ip)),
          config_(config),
          store_(store),
          stream_manager_(stream_manager),
          authenticator_(authenticator),
          session_key_("session-" + std::to_string(g_session_counter.fetch_add(1))) {}

    ~RtmpSession() override {
        Cleanup();
    }

    void Run() {
        const bool ok = PerformHandshake() && ProcessLoop();
        if (!ok) {
            Cleanup();
        }
    }

    bool SendMediaPacket(std::uint8_t type, std::uint32_t timestamp, const std::vector<std::uint8_t>& payload) override {
        std::uint32_t chunk_stream_id = 5;
        if (type == 8) {
            chunk_stream_id = 4;
        } else if (type == 9) {
            chunk_stream_id = 6;
        }
        return SendMessage(type, message_stream_id_, timestamp, chunk_stream_id, payload);
    }

    std::string SessionKey() const override {
        return session_key_;
    }

    void ForceClose(const std::string& reason) override {
        (void)reason;
        bool expected = false;
        if (!closed_.compare_exchange_strong(expected, true)) {
            return;
        }
        net::Close(socket_);
    }

private:
    bool PerformHandshake() {
        std::uint8_t c0 = 0;
        if (!net::RecvAll(socket_, &c0, 1) || c0 != 3) {
            return false;
        }

        std::vector<std::uint8_t> c1(1536);
        if (!net::RecvAll(socket_, c1.data(), c1.size())) {
            return false;
        }

        std::vector<std::uint8_t> response;
        response.reserve(1 + 1536 + 1536);
        response.push_back(3);

        std::vector<std::uint8_t> s1(1536, 0);
        const std::uint32_t now = static_cast<std::uint32_t>(std::time(nullptr));
        s1[0] = static_cast<std::uint8_t>((now >> 24) & 0xff);
        s1[1] = static_cast<std::uint8_t>((now >> 16) & 0xff);
        s1[2] = static_cast<std::uint8_t>((now >> 8) & 0xff);
        s1[3] = static_cast<std::uint8_t>(now & 0xff);
        for (std::size_t i = 8; i < s1.size(); ++i) {
            s1[i] = static_cast<std::uint8_t>(i & 0xff);
        }

        response.insert(response.end(), s1.begin(), s1.end());
        response.insert(response.end(), c1.begin(), c1.end());
        if (!net::SendAll(socket_, response.data(), response.size())) {
            return false;
        }

        std::vector<std::uint8_t> c2(1536);
        return net::RecvAll(socket_, c2.data(), c2.size());
    }

    bool ProcessLoop() {
        while (!closed_.load()) {
            RtmpMessage message;
            if (!ReadMessage(message)) {
                return false;
            }

            if (message.type == 1 && message.payload.size() >= 4) {
                input_chunk_size_ = (static_cast<std::uint32_t>(message.payload[0]) << 24) |
                                    (static_cast<std::uint32_t>(message.payload[1]) << 16) |
                                    (static_cast<std::uint32_t>(message.payload[2]) << 8) |
                                    static_cast<std::uint32_t>(message.payload[3]);
                if (input_chunk_size_ == 0) {
                    input_chunk_size_ = 128;
                }
                continue;
            }

            if (message.type == 20 || message.type == 17) {
                std::vector<std::uint8_t> payload = message.payload;
                if (message.type == 17 && !payload.empty()) {
                    payload.erase(payload.begin());
                }

                std::vector<Amf0Value> values;
                std::string error;
                if (!DecodeAmf0Values(payload, values, error)) {
                    std::cerr << "amf decode failed: " << error << std::endl;
                    return false;
                }
                if (!HandleCommand(message.message_stream_id, values)) {
                    return false;
                }
                continue;
            }

            if (is_publisher_ && (message.type == 8 || message.type == 9 || message.type == 18)) {
                MediaPacket packet;
                packet.type = message.type;
                packet.timestamp = message.timestamp;
                packet.payload = std::move(message.payload);
                const auto players = stream_manager_.OnPublisherPacket(app_, stream_name_, packet);
                for (const auto& player : players) {
                    player->SendMediaPacket(packet.type, packet.timestamp, packet.payload);
                }
            }
        }
        return true;
    }

    bool ReadMessage(RtmpMessage& out) {
        while (true) {
            std::uint8_t first = 0;
            if (!net::RecvAll(socket_, &first, 1)) {
                return false;
            }

            const std::uint8_t fmt = static_cast<std::uint8_t>((first >> 6) & 0x03);
            std::uint32_t csid = first & 0x3f;
            if (csid == 0) {
                std::uint8_t ext = 0;
                if (!net::RecvAll(socket_, &ext, 1)) {
                    return false;
                }
                csid = 64 + ext;
            } else if (csid == 1) {
                std::uint8_t ext[2] = {0, 0};
                if (!net::RecvAll(socket_, ext, 2)) {
                    return false;
                }
                csid = 64 + ext[0] + (static_cast<std::uint32_t>(ext[1]) * 256);
            }

            ChunkState& state = chunk_states_[csid];
            if (fmt == 0) {
                std::uint8_t header[11] = {0};
                if (!net::RecvAll(socket_, header, sizeof(header))) {
                    return false;
                }

                std::uint32_t timestamp = (static_cast<std::uint32_t>(header[0]) << 16) |
                                          (static_cast<std::uint32_t>(header[1]) << 8) |
                                          static_cast<std::uint32_t>(header[2]);
                state.message_length = (static_cast<std::uint32_t>(header[3]) << 16) |
                                       (static_cast<std::uint32_t>(header[4]) << 8) |
                                       static_cast<std::uint32_t>(header[5]);
                state.message_type = header[6];
                state.message_stream_id = static_cast<std::uint32_t>(header[7]) |
                                          (static_cast<std::uint32_t>(header[8]) << 8) |
                                          (static_cast<std::uint32_t>(header[9]) << 16) |
                                          (static_cast<std::uint32_t>(header[10]) << 24);
                state.extended_timestamp = timestamp == 0x00ffffff;
                if (state.extended_timestamp) {
                    std::uint8_t ext_ts[4] = {0};
                    if (!net::RecvAll(socket_, ext_ts, sizeof(ext_ts))) {
                        return false;
                    }
                    state.timestamp = (static_cast<std::uint32_t>(ext_ts[0]) << 24) |
                                      (static_cast<std::uint32_t>(ext_ts[1]) << 16) |
                                      (static_cast<std::uint32_t>(ext_ts[2]) << 8) |
                                      static_cast<std::uint32_t>(ext_ts[3]);
                } else {
                    state.timestamp = timestamp;
                }
                state.timestamp_delta = 0;
                state.last_fmt = 0;
                state.header_valid = true;
                state.payload.clear();
                state.received = 0;
                state.message_active = true;
            } else if (fmt == 1) {
                if (!state.header_valid) {
                    return false;
                }
                std::uint8_t header[7] = {0};
                if (!net::RecvAll(socket_, header, sizeof(header))) {
                    return false;
                }
                std::uint32_t delta = (static_cast<std::uint32_t>(header[0]) << 16) |
                                      (static_cast<std::uint32_t>(header[1]) << 8) |
                                      static_cast<std::uint32_t>(header[2]);
                state.message_length = (static_cast<std::uint32_t>(header[3]) << 16) |
                                       (static_cast<std::uint32_t>(header[4]) << 8) |
                                       static_cast<std::uint32_t>(header[5]);
                state.message_type = header[6];
                state.extended_timestamp = delta == 0x00ffffff;
                if (state.extended_timestamp) {
                    std::uint8_t ext_ts[4] = {0};
                    if (!net::RecvAll(socket_, ext_ts, sizeof(ext_ts))) {
                        return false;
                    }
                    delta = (static_cast<std::uint32_t>(ext_ts[0]) << 24) |
                            (static_cast<std::uint32_t>(ext_ts[1]) << 16) |
                            (static_cast<std::uint32_t>(ext_ts[2]) << 8) |
                            static_cast<std::uint32_t>(ext_ts[3]);
                }
                state.timestamp_delta = delta;
                state.timestamp += delta;
                state.last_fmt = 1;
                state.header_valid = true;
                state.payload.clear();
                state.received = 0;
                state.message_active = true;
            } else if (fmt == 2) {
                if (!state.header_valid) {
                    return false;
                }
                std::uint8_t header[3] = {0};
                if (!net::RecvAll(socket_, header, sizeof(header))) {
                    return false;
                }
                std::uint32_t delta = (static_cast<std::uint32_t>(header[0]) << 16) |
                                      (static_cast<std::uint32_t>(header[1]) << 8) |
                                      static_cast<std::uint32_t>(header[2]);
                state.extended_timestamp = delta == 0x00ffffff;
                if (state.extended_timestamp) {
                    std::uint8_t ext_ts[4] = {0};
                    if (!net::RecvAll(socket_, ext_ts, sizeof(ext_ts))) {
                        return false;
                    }
                    delta = (static_cast<std::uint32_t>(ext_ts[0]) << 24) |
                            (static_cast<std::uint32_t>(ext_ts[1]) << 16) |
                            (static_cast<std::uint32_t>(ext_ts[2]) << 8) |
                            static_cast<std::uint32_t>(ext_ts[3]);
                }
                state.timestamp_delta = delta;
                state.timestamp += delta;
                state.last_fmt = 2;
                state.header_valid = true;
                state.payload.clear();
                state.received = 0;
                state.message_active = true;
            } else {
                if (!state.header_valid) {
                    return false;
                }
                if (!state.message_active) {
                    if (state.last_fmt == 1 || state.last_fmt == 2 || state.last_fmt == 3) {
                        state.timestamp += state.timestamp_delta;
                    }
                    state.payload.clear();
                    state.received = 0;
                    state.message_active = true;
                    state.last_fmt = 3;
                }
                if (state.extended_timestamp) {
                    std::uint8_t ext_ts[4] = {0};
                    if (!net::RecvAll(socket_, ext_ts, sizeof(ext_ts))) {
                        return false;
                    }
                }
            }

            if (state.message_length == 0) {
                state.message_active = false;
                out.chunk_stream_id = csid;
                out.message_stream_id = state.message_stream_id;
                out.timestamp = state.timestamp;
                out.type = state.message_type;
                out.payload.clear();
                return true;
            }

            const std::uint32_t remaining = state.message_length - static_cast<std::uint32_t>(state.received);
            const std::size_t to_read = std::min<std::size_t>(input_chunk_size_, remaining);
            const std::size_t old_size = state.payload.size();
            state.payload.resize(old_size + to_read);
            if (!net::RecvAll(socket_, state.payload.data() + old_size, to_read)) {
                return false;
            }
            state.received += to_read;

            if (state.received == state.message_length) {
                out.chunk_stream_id = csid;
                out.message_stream_id = state.message_stream_id;
                out.timestamp = state.timestamp;
                out.type = state.message_type;
                out.payload = state.payload;
                state.payload.clear();
                state.received = 0;
                state.message_active = false;
                return true;
            }
        }
    }

    bool HandleCommand(std::uint32_t message_stream_id, const std::vector<Amf0Value>& values) {
        if (values.empty() || values[0].type != Amf0Value::Type::String) {
            return true;
        }

        const std::string command = values[0].string_value;
        const double transaction_id = GetNumber(values, 1);

        if (command == "connect") {
            if (values.size() >= 3 && (values[2].type == Amf0Value::Type::Object || values[2].type == Amf0Value::Type::EcmaArray)) {
                app_ = GetObjectString(values[2], "app");
                tc_url_ = GetObjectString(values[2], "tcUrl");
                const Amf0Value* object_encoding = FindObjectValue(values[2], "objectEncoding");
                if (object_encoding != nullptr && object_encoding->type == Amf0Value::Type::Number) {
                    object_encoding_ = object_encoding->number_value;
                }
            }
            return SendConnectSuccess(transaction_id);
        }

        if (command == "releaseStream" || command == "FCPublish") {
            return SendResult(transaction_id, {Amf0Value::Null()});
        }

        if (command == "createStream") {
            return SendCreateStreamResult(transaction_id);
        }

        if (command == "getStreamLength") {
            return SendResult(transaction_id, {Amf0Value::Null(), Amf0Value::Number(0.0)});
        }

        if (command == "publish") {
            return HandlePublish(message_stream_id, values);
        }

        if (command == "play") {
            return HandlePlay(message_stream_id, values);
        }

        if (command == "deleteStream" || command == "closeStream") {
            return false;
        }

        return true;
    }

    bool HandlePublish(std::uint32_t message_stream_id, const std::vector<Amf0Value>& values) {
        message_stream_id_ = message_stream_id == 0 ? 1 : message_stream_id;
        std::map<std::string, std::string> query;
        if (!ResolveAppStream(GetString(values, 3), query)) {
            return SendStatus("error", "NetStream.Publish.BadName", "invalid stream name", message_stream_id_);
        }

        RtmpAuthRequest request;
        request.action = "publish";
        request.app = app_;
        request.stream = stream_name_;
        request.tc_url = tc_url_;
        request.client_ip = client_ip_;
        request.query = query;
        request.token = ResolveToken(query);
        const RtmpAuthResult auth = authenticator_.Authorize(request);
        if (!auth.allow) {
            SendStatus("error", "NetStream.Publish.Unauthorized", auth.code + ": " + auth.message, message_stream_id_);
            return false;
        }

        std::string error;
        if (!stream_manager_.RegisterPublisher(app_, stream_name_, shared_from_this(), client_ip_, config_.stream_gop_cache_size, error)) {
            SendStatus("error", "NetStream.Publish.BadName", error, message_stream_id_);
            return false;
        }

        is_publisher_ = true;
        authorized_room_id_ = auth.room.id;
        {
            RoomRecord updated_room;
            std::string store_error;
            if (!store_.MarkRoomPublishStarted(stream_name_, client_ip_, updated_room, store_error)) {
                RtmpServiceRecord updated_service;
                if (!store_.MarkRtmpServicePublishStarted(stream_name_, client_ip_, updated_service, store_error)) {
                    std::cerr << "mark publish started failed: " << store_error << std::endl;
                }
            }
        }
        return SendStatus("status", "NetStream.Publish.Start", "publishing started", message_stream_id_);
    }

    bool HandlePlay(std::uint32_t message_stream_id, const std::vector<Amf0Value>& values) {
        message_stream_id_ = message_stream_id == 0 ? 1 : message_stream_id;
        std::map<std::string, std::string> query;
        if (!ResolveAppStream(GetString(values, 3), query)) {
            return SendStatus("error", "NetStream.Play.BadName", "invalid stream name", message_stream_id_);
        }

        RtmpAuthRequest request;
        request.action = "play";
        request.app = app_;
        request.stream = stream_name_;
        request.tc_url = tc_url_;
        request.client_ip = client_ip_;
        request.query = query;
        request.token = ResolveToken(query);
        const RtmpAuthResult auth = authenticator_.Authorize(request);
        if (!auth.allow) {
            SendStatus("error", "NetStream.Play.Unauthorized", auth.code + ": " + auth.message, message_stream_id_);
            return false;
        }

        std::vector<MediaPacket> bootstrap;
        stream_manager_.AddPlayer(app_, stream_name_, shared_from_this(), bootstrap);
        is_player_ = true;
        authorized_room_id_ = auth.room.id;
        {
            RoomRecord updated_room;
            std::string store_error;
            if (!store_.AppendRoomLogByStreamName(stream_name_, "RTMP player connected from " + client_ip_, updated_room, store_error)) {
                RtmpServiceRecord updated_service;
                if (!store_.AppendRtmpServiceLogByStreamName(stream_name_, "RTMP player connected from " + client_ip_, updated_service,
                                                             store_error)) {
                    std::cerr << "append player log failed: " << store_error << std::endl;
                }
            }
        }

        if (!SendStreamBegin(message_stream_id_)) {
            return false;
        }
        if (!SendStatus("status", "NetStream.Play.Reset", "play reset", message_stream_id_)) {
            return false;
        }
        if (!SendStatus("status", "NetStream.Play.Start", "play started", message_stream_id_)) {
            return false;
        }
        if (!SendStatus("status", "NetStream.Data.Start", "data start", message_stream_id_)) {
            return false;
        }

        for (const auto& packet : bootstrap) {
            if (!SendMediaPacket(packet.type, packet.timestamp, packet.payload)) {
                return false;
            }
        }
        return true;
    }

    bool ResolveAppStream(const std::string& raw, std::map<std::string, std::string>& query) {
        if (raw.empty()) {
            return false;
        }

        std::string path = raw;
        const std::size_t qm = raw.find('?');
        if (qm != std::string::npos) {
            path = raw.substr(0, qm);
            query = ParseQueryString(raw.substr(qm + 1));
        }

        if (!app_.empty() && path.rfind(app_ + "/", 0) == 0) {
            path = path.substr(app_.size() + 1);
        } else if (app_.empty()) {
            const std::size_t slash = path.find('/');
            if (slash != std::string::npos) {
                app_ = path.substr(0, slash);
                path = path.substr(slash + 1);
            }
        }

        if (app_.empty()) {
            app_ = "live";
        }
        stream_name_ = path;
        return !stream_name_.empty();
    }

    bool SendConnectSuccess(double transaction_id) {
        std::vector<std::uint8_t> window;
        WriteU32BE(window, 5000000);
        if (!SendMessage(5, 0, 0, 2, window)) {
            return false;
        }

        std::vector<std::uint8_t> peer;
        WriteU32BE(peer, 5000000);
        peer.push_back(2);
        if (!SendMessage(6, 0, 0, 2, peer)) {
            return false;
        }

        std::vector<std::uint8_t> chunk_size;
        WriteU32BE(chunk_size, output_chunk_size_);
        if (!SendMessage(1, 0, 0, 2, chunk_size)) {
            return false;
        }

        std::map<std::string, Amf0Value> props;
        props["fmsVer"] = Amf0Value::String("jslive-rtmp/0.1");
        props["capabilities"] = Amf0Value::Number(31.0);

        std::map<std::string, Amf0Value> info;
        info["level"] = Amf0Value::String("status");
        info["code"] = Amf0Value::String("NetConnection.Connect.Success");
        info["description"] = Amf0Value::String("Connection succeeded.");
        info["objectEncoding"] = Amf0Value::Number(object_encoding_);

        std::vector<Amf0Value> values;
        values.push_back(Amf0Value::String("_result"));
        values.push_back(Amf0Value::Number(transaction_id));
        values.push_back(Amf0Value::Object(props));
        values.push_back(Amf0Value::Object(info));
        return SendCommand(0, values);
    }

    bool SendCreateStreamResult(double transaction_id) {
        std::vector<Amf0Value> values;
        values.push_back(Amf0Value::String("_result"));
        values.push_back(Amf0Value::Number(transaction_id));
        values.push_back(Amf0Value::Null());
        values.push_back(Amf0Value::Number(1.0));
        return SendCommand(0, values);
    }

    bool SendResult(double transaction_id, const std::vector<Amf0Value>& rest) {
        std::vector<Amf0Value> values;
        values.push_back(Amf0Value::String("_result"));
        values.push_back(Amf0Value::Number(transaction_id));
        values.insert(values.end(), rest.begin(), rest.end());
        return SendCommand(0, values);
    }

    bool SendStatus(const std::string& level, const std::string& code, const std::string& description,
                    std::uint32_t message_stream_id) {
        std::map<std::string, Amf0Value> info;
        info["level"] = Amf0Value::String(level);
        info["code"] = Amf0Value::String(code);
        info["description"] = Amf0Value::String(description);

        std::vector<Amf0Value> values;
        values.push_back(Amf0Value::String("onStatus"));
        values.push_back(Amf0Value::Number(0.0));
        values.push_back(Amf0Value::Null());
        values.push_back(Amf0Value::Object(info));
        return SendCommand(message_stream_id, values);
    }

    bool SendStreamBegin(std::uint32_t message_stream_id) {
        std::vector<std::uint8_t> payload;
        payload.push_back(0x00);
        payload.push_back(0x00);
        WriteU32BE(payload, message_stream_id);
        return SendMessage(4, 0, 0, 2, payload);
    }

    bool SendCommand(std::uint32_t message_stream_id, const std::vector<Amf0Value>& values) {
        return SendMessage(20, message_stream_id, 0, 3, EncodeAmf0Values(values));
    }

    bool SendMessage(std::uint8_t type, std::uint32_t message_stream_id, std::uint32_t timestamp, std::uint32_t chunk_stream_id,
                     const std::vector<std::uint8_t>& payload) {
        std::lock_guard<std::mutex> lock(send_mutex_);

        std::vector<std::uint8_t> buffer;
        std::size_t offset = 0;
        const bool extended_ts = timestamp >= 0x00ffffff;
        while (offset < payload.size() || (payload.empty() && offset == 0)) {
            const bool first_chunk = offset == 0;
            const std::uint8_t fmt = first_chunk ? 0 : 3;
            buffer.push_back(static_cast<std::uint8_t>((fmt << 6) | (chunk_stream_id & 0x3f)));
            if (first_chunk) {
                WriteU24(buffer, extended_ts ? 0x00ffffff : timestamp);
                WriteU24(buffer, static_cast<std::uint32_t>(payload.size()));
                buffer.push_back(type);
                WriteU32LE(buffer, message_stream_id);
            }
            if (extended_ts) {
                WriteU32BE(buffer, timestamp);
            }

            const std::size_t chunk = std::min<std::size_t>(output_chunk_size_, payload.size() - offset);
            if (chunk > 0) {
                buffer.insert(buffer.end(), payload.begin() + static_cast<std::ptrdiff_t>(offset),
                              payload.begin() + static_cast<std::ptrdiff_t>(offset + chunk));
            }
            offset += chunk;
            if (payload.empty()) {
                break;
            }
        }
        return net::SendAll(socket_, buffer.data(), buffer.size());
    }

    void Cleanup() {
        if (closed_.load()) {
            return;
        }
        closed_.store(true);

        if (is_publisher_) {
            stream_manager_.UnregisterPublisher(app_, stream_name_, session_key_);
            RoomRecord updated_room;
            std::string store_error;
            if (!store_.MarkRoomPublishStopped(stream_name_, "session closed", updated_room, store_error)) {
                RtmpServiceRecord updated_service;
                if (!store_.MarkRtmpServicePublishStopped(stream_name_, "session closed", updated_service, store_error)) {
                    std::cerr << "mark publish stopped failed: " << store_error << std::endl;
                }
            }
        }
        if (is_player_) {
            stream_manager_.RemovePlayer(app_, stream_name_, session_key_);
            RoomRecord updated_room;
            std::string store_error;
            if (!store_.AppendRoomLogByStreamName(stream_name_, "RTMP player disconnected", updated_room, store_error)) {
                RtmpServiceRecord updated_service;
                if (!store_.AppendRtmpServiceLogByStreamName(stream_name_, "RTMP player disconnected", updated_service, store_error)) {
                    std::cerr << "append player disconnect log failed: " << store_error << std::endl;
                }
            }
        }
        net::Close(socket_);
    }

    SocketHandle socket_ = kInvalidSocket;
    std::string client_ip_;
    ServerConfig config_;
    PersistentStore& store_;
    StreamManager& stream_manager_;
    const RtmpAuthenticator& authenticator_;
    std::string session_key_;
    std::mutex send_mutex_;
    std::unordered_map<std::uint32_t, ChunkState> chunk_states_;
    std::uint32_t input_chunk_size_ = 128;
    std::uint32_t output_chunk_size_ = 4096;
    std::uint32_t message_stream_id_ = 1;
    double object_encoding_ = 0.0;
    bool is_publisher_ = false;
    bool is_player_ = false;
    std::atomic<bool> closed_{false};
    std::string app_;
    std::string stream_name_;
    std::string tc_url_;
    std::string authorized_room_id_;
};

}  // namespace

RtmpServer::RtmpServer(const ServerConfig& config, PersistentStore& store, StreamManager& stream_manager, const RtmpAuthenticator& authenticator)
    : config_(config), store_(store), stream_manager_(stream_manager), authenticator_(authenticator) {}

bool RtmpServer::Run(std::string& error) {
    SocketHandle listener = kInvalidSocket;
    if (!net::CreateListener(config_.rtmp_host, config_.rtmp_port, listener, error)) {
        return false;
    }

    std::cout << "RTMP listen on " << config_.rtmp_host << ":" << config_.rtmp_port << std::endl;
    while (true) {
        SocketHandle client = kInvalidSocket;
        std::string client_ip;
        if (!net::Accept(listener, client, client_ip, error)) {
            net::Close(listener);
            return false;
        }

        auto session = std::make_shared<RtmpSession>(client, client_ip, config_, store_, stream_manager_, authenticator_);
        std::thread([session]() { session->Run(); }).detach();
    }
}
