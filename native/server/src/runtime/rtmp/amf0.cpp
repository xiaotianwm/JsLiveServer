#include "amf0.h"

#include <cstring>

namespace {

void WriteU16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>(value & 0xff));
}

void WriteU32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>(value & 0xff));
}

bool ReadU16(const std::vector<std::uint8_t>& data, std::size_t& offset, std::uint16_t& out) {
    if (offset + 2 > data.size()) {
        return false;
    }
    out = static_cast<std::uint16_t>((data[offset] << 8) | data[offset + 1]);
    offset += 2;
    return true;
}

bool ReadU32(const std::vector<std::uint8_t>& data, std::size_t& offset, std::uint32_t& out) {
    if (offset + 4 > data.size()) {
        return false;
    }
    out = (static_cast<std::uint32_t>(data[offset]) << 24) |
          (static_cast<std::uint32_t>(data[offset + 1]) << 16) |
          (static_cast<std::uint32_t>(data[offset + 2]) << 8) |
          static_cast<std::uint32_t>(data[offset + 3]);
    offset += 4;
    return true;
}

bool DecodeValue(const std::vector<std::uint8_t>& data, std::size_t& offset, Amf0Value& out, std::string& error) {
    if (offset >= data.size()) {
        error = "unexpected end of AMF payload";
        return false;
    }

    const std::uint8_t marker = data[offset++];
    switch (marker) {
        case 0x00: {
            if (offset + 8 > data.size()) {
                error = "truncated AMF number";
                return false;
            }
            std::uint64_t bits = 0;
            for (int i = 0; i < 8; ++i) {
                bits = (bits << 8) | data[offset + i];
            }
            double number = 0.0;
            std::memcpy(&number, &bits, sizeof(number));
            offset += 8;
            out = Amf0Value::Number(number);
            return true;
        }
        case 0x01: {
            if (offset >= data.size()) {
                error = "truncated AMF boolean";
                return false;
            }
            out = Amf0Value::Boolean(data[offset++] != 0);
            return true;
        }
        case 0x02: {
            std::uint16_t length = 0;
            if (!ReadU16(data, offset, length) || offset + length > data.size()) {
                error = "truncated AMF string";
                return false;
            }
            out = Amf0Value::String(std::string(reinterpret_cast<const char*>(&data[offset]), length));
            offset += length;
            return true;
        }
        case 0x03: {
            out.type = Amf0Value::Type::Object;
            while (true) {
                if (offset + 3 <= data.size() && data[offset] == 0x00 && data[offset + 1] == 0x00 && data[offset + 2] == 0x09) {
                    offset += 3;
                    return true;
                }
                std::uint16_t key_length = 0;
                if (!ReadU16(data, offset, key_length) || offset + key_length > data.size()) {
                    error = "truncated AMF object key";
                    return false;
                }
                const std::string key(reinterpret_cast<const char*>(&data[offset]), key_length);
                offset += key_length;
                Amf0Value value;
                if (!DecodeValue(data, offset, value, error)) {
                    return false;
                }
                out.object_value[key] = value;
            }
        }
        case 0x05:
            out = Amf0Value::Null();
            return true;
        case 0x08: {
            std::uint32_t ignored_count = 0;
            if (!ReadU32(data, offset, ignored_count)) {
                error = "truncated AMF ecma array";
                return false;
            }
            (void)ignored_count;
            out.type = Amf0Value::Type::EcmaArray;
            while (true) {
                if (offset + 3 <= data.size() && data[offset] == 0x00 && data[offset + 1] == 0x00 && data[offset + 2] == 0x09) {
                    offset += 3;
                    return true;
                }
                std::uint16_t key_length = 0;
                if (!ReadU16(data, offset, key_length) || offset + key_length > data.size()) {
                    error = "truncated AMF ecma array key";
                    return false;
                }
                const std::string key(reinterpret_cast<const char*>(&data[offset]), key_length);
                offset += key_length;
                Amf0Value value;
                if (!DecodeValue(data, offset, value, error)) {
                    return false;
                }
                out.object_value[key] = value;
            }
        }
        case 0x0A: {
            std::uint32_t count = 0;
            if (!ReadU32(data, offset, count)) {
                error = "truncated AMF array";
                return false;
            }
            out.type = Amf0Value::Type::StrictArray;
            out.array_value.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                Amf0Value value;
                if (!DecodeValue(data, offset, value, error)) {
                    return false;
                }
                out.array_value.push_back(value);
            }
            return true;
        }
        case 0x0C: {
            std::uint32_t length = 0;
            if (!ReadU32(data, offset, length) || offset + length > data.size()) {
                error = "truncated AMF long string";
                return false;
            }
            out = Amf0Value::String(std::string(reinterpret_cast<const char*>(&data[offset]), length));
            offset += length;
            return true;
        }
        default:
            error = "unsupported AMF marker: " + std::to_string(marker);
            return false;
    }
}

void WriteDouble(std::vector<std::uint8_t>& out, double value) {
    std::uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<std::uint8_t>((bits >> (i * 8)) & 0xff));
    }
}

void EncodeObjectFields(const std::map<std::string, Amf0Value>& object, std::vector<std::uint8_t>& out) {
    for (const auto& entry : object) {
        WriteU16(out, static_cast<std::uint16_t>(entry.first.size()));
        out.insert(out.end(), entry.first.begin(), entry.first.end());
        EncodeAmf0Value(entry.second, out);
    }
    out.push_back(0x00);
    out.push_back(0x00);
    out.push_back(0x09);
}

}  // namespace

Amf0Value Amf0Value::Number(double value) {
    Amf0Value out;
    out.type = Type::Number;
    out.number_value = value;
    return out;
}

Amf0Value Amf0Value::Boolean(bool value) {
    Amf0Value out;
    out.type = Type::Boolean;
    out.bool_value = value;
    return out;
}

Amf0Value Amf0Value::String(const std::string& value) {
    Amf0Value out;
    out.type = Type::String;
    out.string_value = value;
    return out;
}

Amf0Value Amf0Value::Object(const std::map<std::string, Amf0Value>& value) {
    Amf0Value out;
    out.type = Type::Object;
    out.object_value = value;
    return out;
}

Amf0Value Amf0Value::Null() {
    return Amf0Value();
}

bool DecodeAmf0Values(const std::vector<std::uint8_t>& data, std::vector<Amf0Value>& values, std::string& error) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        Amf0Value value;
        if (!DecodeValue(data, offset, value, error)) {
            return false;
        }
        values.push_back(value);
    }
    return true;
}

void EncodeAmf0Value(const Amf0Value& value, std::vector<std::uint8_t>& out) {
    switch (value.type) {
        case Amf0Value::Type::Number:
            out.push_back(0x00);
            WriteDouble(out, value.number_value);
            break;
        case Amf0Value::Type::Boolean:
            out.push_back(0x01);
            out.push_back(value.bool_value ? 1 : 0);
            break;
        case Amf0Value::Type::String:
            if (value.string_value.size() <= 65535) {
                out.push_back(0x02);
                WriteU16(out, static_cast<std::uint16_t>(value.string_value.size()));
            } else {
                out.push_back(0x0C);
                WriteU32(out, static_cast<std::uint32_t>(value.string_value.size()));
            }
            out.insert(out.end(), value.string_value.begin(), value.string_value.end());
            break;
        case Amf0Value::Type::Object:
            out.push_back(0x03);
            EncodeObjectFields(value.object_value, out);
            break;
        case Amf0Value::Type::Null:
            out.push_back(0x05);
            break;
        case Amf0Value::Type::EcmaArray:
            out.push_back(0x08);
            WriteU32(out, static_cast<std::uint32_t>(value.object_value.size()));
            EncodeObjectFields(value.object_value, out);
            break;
        case Amf0Value::Type::StrictArray:
            out.push_back(0x0A);
            WriteU32(out, static_cast<std::uint32_t>(value.array_value.size()));
            for (const auto& item : value.array_value) {
                EncodeAmf0Value(item, out);
            }
            break;
    }
}

std::vector<std::uint8_t> EncodeAmf0Values(const std::vector<Amf0Value>& values) {
    std::vector<std::uint8_t> out;
    for (const auto& value : values) {
        EncodeAmf0Value(value, out);
    }
    return out;
}

const Amf0Value* FindObjectValue(const Amf0Value& value, const std::string& key) {
    if (value.type != Amf0Value::Type::Object && value.type != Amf0Value::Type::EcmaArray) {
        return nullptr;
    }
    const auto it = value.object_value.find(key);
    if (it == value.object_value.end()) {
        return nullptr;
    }
    return &it->second;
}
