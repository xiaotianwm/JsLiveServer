#pragma once

#include <map>
#include <string>

struct HttpRequest {
    std::string method;
    std::string target;
    std::string path;
    std::string version;
    std::map<std::string, std::string> headers;
    std::map<std::string, std::string> query;
    std::string body;
};
