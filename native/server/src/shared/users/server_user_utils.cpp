#include "server_user_utils.h"

#include "../http/server_shared.h"

#include <ctime>
#include <iomanip>
#include <sstream>

namespace {

std::string JsonEscape(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char ch : value) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    return out;
}

std::string JsonString(const std::string& value) {
    return "\"" + JsonEscape(value) + "\"";
}

std::string JsonBool(bool value) {
    return value ? "true" : "false";
}

std::string FormatTime(std::int64_t unix_time) {
    if (unix_time <= 0) {
        return "null";
    }
    const std::time_t value = static_cast<std::time_t>(unix_time);
    std::tm tm_value{};
#ifdef _WIN32
    gmtime_s(&tm_value, &value);
#else
    gmtime_r(&value, &tm_value);
#endif
    std::ostringstream out;
    out << "\"" << std::put_time(&tm_value, "%Y-%m-%dT%H:%M:%SZ") << "\"";
    return out.str();
}

std::string BuildStorageUsageJson(const StorageUsage& usage) {
    std::ostringstream out;
    out << "{"
        << "\"used_storage_bytes\":" << usage.used_storage_bytes << ","
        << "\"reserved_storage_bytes\":" << usage.reserved_storage_bytes << ","
        << "\"max_storage_bytes\":" << usage.max_storage_bytes << ","
        << "\"available_storage_bytes\":" << usage.available_storage_bytes << ","
        << "\"active_room_count\":" << usage.active_room_count << ","
        << "\"max_active_rooms\":" << usage.max_active_rooms << ","
        << "\"available_room_slots\":" << usage.available_room_slots
        << "}";
    return out.str();
}

std::time_t TimegmUtc(std::tm* value) {
#ifdef _WIN32
    return _mkgmtime(value);
#else
    return timegm(value);
#endif
}

}  // namespace

std::string BuildUserJson(const UserRecord& user, const StorageUsage& usage) {
    const bool subscription_expired = user.subscription_ends_at > 0 && user.subscription_ends_at < NowUnix();
    const bool account_enabled = user.status == "active" && !subscription_expired;
    std::ostringstream out;
    out << "{"
        << "\"id\":" << JsonString(user.id) << ","
        << "\"username\":" << JsonString(user.username) << ","
        << "\"role\":" << JsonString(user.role) << ","
        << "\"status\":" << JsonString(user.status) << ","
        << "\"account_enabled\":" << JsonBool(account_enabled) << ","
        << "\"max_storage_bytes\":" << user.max_storage_bytes << ","
        << "\"max_active_rooms\":" << user.max_active_rooms << ","
        << "\"subscription_ends_at\":" << FormatTime(user.subscription_ends_at) << ","
        << "\"subscription_expired\":" << JsonBool(subscription_expired) << ","
        << "\"created_at\":" << FormatTime(user.created_at) << ","
        << "\"usage\":" << BuildStorageUsageJson(usage)
        << "}";
    return out.str();
}

std::string NormalizeStoredPassword(const std::string& password) {
    const std::string trimmed = Trim(password);
    if (trimmed.rfind("plain:", 0) == 0) {
        return trimmed;
    }
    return "plain:" + trimmed;
}

bool IsValidUserRole(const std::string& role) {
    return role == "admin" || role == "user";
}

bool IsValidUserStatus(const std::string& status) {
    return status == "active" || status == "inactive" || status == "deleted";
}

bool ParseUserDate(const std::string& value, std::int64_t& out) {
    const std::string trimmed = Trim(value);
    if (trimmed.empty()) {
        return false;
    }

    std::tm tm_value{};
    {
        std::istringstream stream(trimmed);
        stream >> std::get_time(&tm_value, "%Y-%m-%d");
        if (!stream.fail() && stream.peek() == EOF) {
            tm_value.tm_hour = 0;
            tm_value.tm_min = 0;
            tm_value.tm_sec = 0;
            const std::time_t parsed = std::mktime(&tm_value);
            if (parsed < 0) {
                return false;
            }
            out = static_cast<std::int64_t>(parsed);
            return true;
        }
    }

    tm_value = {};
    {
        std::istringstream stream(trimmed);
        stream >> std::get_time(&tm_value, "%Y-%m-%dT%H:%M:%SZ");
        if (!stream.fail() && stream.peek() == EOF) {
            const std::time_t parsed = TimegmUtc(&tm_value);
            if (parsed < 0) {
                return false;
            }
            out = static_cast<std::int64_t>(parsed);
            return true;
        }
    }

    return false;
}
