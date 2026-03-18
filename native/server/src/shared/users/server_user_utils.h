#pragma once

#include "../../storage/persistence/store.h"

#include <cstdint>
#include <string>

std::string BuildUserJson(const UserRecord& user, const StorageUsage& usage);
std::string NormalizeStoredPassword(const std::string& password);
bool IsValidUserRole(const std::string& role);
bool IsValidUserStatus(const std::string& status);
bool ParseUserDate(const std::string& value, std::int64_t& out);
