#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

struct Amf0Value {
    enum class Type {
        Number,
        Boolean,
        String,
        Object,
        Null,
        EcmaArray,
        StrictArray
    };

    Type type = Type::Null;
    double number_value = 0.0;
    bool bool_value = false;
    std::string string_value;
    std::map<std::string, Amf0Value> object_value;
    std::vector<Amf0Value> array_value;

    static Amf0Value Number(double value);
    static Amf0Value Boolean(bool value);
    static Amf0Value String(const std::string& value);
    static Amf0Value Object(const std::map<std::string, Amf0Value>& value);
    static Amf0Value Null();
};

bool DecodeAmf0Values(const std::vector<std::uint8_t>& data, std::vector<Amf0Value>& values, std::string& error);
void EncodeAmf0Value(const Amf0Value& value, std::vector<std::uint8_t>& out);
std::vector<std::uint8_t> EncodeAmf0Values(const std::vector<Amf0Value>& values);

const Amf0Value* FindObjectValue(const Amf0Value& value, const std::string& key);
