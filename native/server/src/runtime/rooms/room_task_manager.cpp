#include "room_task_manager.h"

#include "room_urls.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstring>
#include <cstdio>
#include <ctime>
#include <deque>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>
#include <filesystem>

namespace {

std::int64_t NowUnix() {
    return static_cast<std::int64_t>(std::time(nullptr));
}

std::string Trim(const std::string& value) {
    std::size_t start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
        ++start;
    }
    std::size_t end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return value.substr(start, end - start);
}

bool NeedsQuotes(const std::string& value) {
    return value.empty() || value.find_first_of(" \t\n\v\"") != std::string::npos;
}

#ifdef _WIN32
std::string QuoteWindowsArg(const std::string& value) {
    if (!NeedsQuotes(value)) {
        return value;
    }

    std::string out = "\"";
    int backslashes = 0;
    for (char ch : value) {
        if (ch == '\\') {
            ++backslashes;
            continue;
        }
        if (ch == '"') {
            out.append(static_cast<std::size_t>(backslashes * 2 + 1), '\\');
            out.push_back('"');
            backslashes = 0;
            continue;
        }
        if (backslashes > 0) {
            out.append(static_cast<std::size_t>(backslashes), '\\');
            backslashes = 0;
        }
        out.push_back(ch);
    }
    if (backslashes > 0) {
        out.append(static_cast<std::size_t>(backslashes * 2), '\\');
    }
    out.push_back('"');
    return out;
}

std::string JoinCommandLine(const std::vector<std::string>& args) {
    std::ostringstream out;
    for (std::size_t i = 0; i < args.size(); ++i) {
        if (i != 0) {
            out << ' ';
        }
        out << QuoteWindowsArg(args[i]);
    }
    return out.str();
}
#endif

std::string LastProcessError(const std::string& prefix) {
#ifdef _WIN32
    const DWORD code = GetLastError();
    std::ostringstream out;
    out << prefix << " (win32=" << code << ")";
    return out.str();
#else
    std::ostringstream out;
    out << prefix << " (" << std::strerror(errno) << ")";
    return out.str();
#endif
}

std::filesystem::path ExecutableDirectory() {
#ifdef _WIN32
    std::vector<char> buffer(MAX_PATH, '\0');
    DWORD length = GetModuleFileNameA(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
    while (length == buffer.size()) {
        buffer.resize(buffer.size() * 2, '\0');
        length = GetModuleFileNameA(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
    }
    if (length == 0) {
        return std::filesystem::current_path();
    }
    return std::filesystem::path(std::string(buffer.data(), length)).parent_path();
#else
    std::vector<char> buffer(1024, '\0');
    for (;;) {
        const ssize_t length = ::readlink("/proc/self/exe", buffer.data(), buffer.size());
        if (length < 0) {
            return std::filesystem::current_path();
        }
        if (static_cast<std::size_t>(length) < buffer.size()) {
            return std::filesystem::path(std::string(buffer.data(), static_cast<std::size_t>(length))).parent_path();
        }
        buffer.resize(buffer.size() * 2, '\0');
    }
#endif
}

bool TryResolveExecutable(const std::filesystem::path& candidate, std::string& resolved) {
    if (candidate.empty()) {
        return false;
    }

    std::error_code ec;
    if (candidate.has_parent_path() || candidate.is_absolute()) {
        const std::filesystem::path absolute = std::filesystem::absolute(candidate, ec);
        if (!ec && std::filesystem::exists(absolute, ec) && std::filesystem::is_regular_file(absolute, ec)
#ifdef _WIN32
            ) {
#else
            && ::access(absolute.string().c_str(), X_OK) == 0) {
#endif
            resolved = absolute.string();
            return true;
        }
    }

#ifdef _WIN32
    DWORD length = SearchPathA(nullptr, candidate.string().c_str(), nullptr, 0, nullptr, nullptr);
    if (length == 0 && candidate.extension().empty()) {
        length = SearchPathA(nullptr, candidate.string().c_str(), ".exe", 0, nullptr, nullptr);
    }
    if (length == 0) {
        return false;
    }

    std::vector<char> buffer(length + 1, '\0');
    DWORD result = SearchPathA(nullptr, candidate.string().c_str(), nullptr, static_cast<DWORD>(buffer.size()), buffer.data(), nullptr);
    if (result == 0 && candidate.extension().empty()) {
        result = SearchPathA(nullptr, candidate.string().c_str(), ".exe", static_cast<DWORD>(buffer.size()), buffer.data(), nullptr);
    }
    if (result == 0) {
        return false;
    }
    resolved.assign(buffer.data(), result);
    return true;
#else
    if (candidate.has_parent_path() || candidate.is_absolute()) {
        return false;
    }

    const char* path_env = std::getenv("PATH");
    if (path_env == nullptr || *path_env == '\0') {
        return false;
    }

    std::istringstream stream(path_env);
    std::string entry;
    while (std::getline(stream, entry, ':')) {
        const std::filesystem::path base = entry.empty() ? std::filesystem::current_path() : std::filesystem::path(entry);
        const std::filesystem::path absolute = base / candidate;
        if (::access(absolute.string().c_str(), X_OK) == 0) {
            resolved = absolute.string();
            return true;
        }
    }
    return false;
#endif
}

std::string ResolveFfmpegExecutable(const ServerConfig& config) {
    std::string resolved;
    if (TryResolveExecutable(config.ffmpeg_exec, resolved)) {
        return resolved;
    }

    const std::filesystem::path cwd = std::filesystem::current_path();
    const std::filesystem::path exe_dir = ExecutableDirectory();
#ifdef _WIN32
    constexpr const char* kBundledFfmpegLocalName = "ffmpeg.exe";
    const char* bundled_rel_paths[] = {
        "gosrc/bin/ffmpeg/windows-amd64/ffmpeg.exe",
        "gosrc/server/runtime/ffmpeg/windows-amd64/ffmpeg.exe",
        "gosrc/third_party/ffmpeg/windows-amd64/ffmpeg.exe",
    };
#else
    constexpr const char* kBundledFfmpegLocalName = "ffmpeg";
    const char* bundled_rel_paths[] = {
        "gosrc/bin/ffmpeg/linux-amd64/ffmpeg",
        "gosrc/server/runtime/ffmpeg/linux-amd64/ffmpeg",
        "gosrc/third_party/ffmpeg/linux-amd64/ffmpeg",
    };
#endif
    if (TryResolveExecutable(cwd / kBundledFfmpegLocalName, resolved) || TryResolveExecutable(exe_dir / kBundledFfmpegLocalName, resolved)) {
        return resolved;
    }
    std::vector<std::filesystem::path> roots = {
        cwd,
        exe_dir,
        exe_dir.parent_path(),
        exe_dir.parent_path().parent_path(),
        exe_dir.parent_path().parent_path().parent_path(),
    };
    for (const auto& root : roots) {
        for (const char* rel : bundled_rel_paths) {
            if (TryResolveExecutable(root / rel, resolved)) {
                return resolved;
            }
        }
    }

    return std::string();
}

bool IsProgressLine(const std::string& line) {
    const std::string lower = [&line]() {
        std::string value = line;
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return value;
    }();
    return lower.find("frame=") != std::string::npos ||
           (lower.find("time=") != std::string::npos && lower.find("bitrate=") != std::string::npos);
}

std::string FormatRetryLog(std::int64_t unix_time, int retry_count) {
    std::time_t value = static_cast<std::time_t>(unix_time);
    std::tm tm_value{};
#ifdef _WIN32
    gmtime_s(&tm_value, &value);
#else
    gmtime_r(&value, &tm_value);
#endif
    char buffer[32] = {};
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &tm_value);
    std::ostringstream out;
    out << "retry #" << retry_count << " scheduled at " << buffer;
    return out.str();
}

std::filesystem::path ResolveFileSourcePath(const ServerConfig& config, const FileRecord& file) {
    if (!file.stored_path.empty()) {
        const std::filesystem::path stored(file.stored_path);
        if (stored.is_absolute()) {
            return stored;
        }
        return std::filesystem::path(config.storage_root) / stored;
    }
    return std::filesystem::path(config.storage_root) / file.id / file.original_name;
}

}  // namespace

struct RoomTaskManager::OutputTracker {
    void Remember(const std::string& line) {
        std::lock_guard<std::mutex> lock(mutex);
        lines.push_back(line);
        if (lines.size() > 12) {
            lines.pop_front();
        }
    }

    std::vector<std::string> Excerpt(std::size_t limit) const {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<std::string> out;
        const std::size_t start = lines.size() > limit ? lines.size() - limit : 0;
        for (std::size_t i = start; i < lines.size(); ++i) {
            out.push_back(lines[i]);
        }
        return out;
    }

    mutable std::mutex mutex;
    std::deque<std::string> lines;
};

struct RoomTaskManager::RunningRoom {
#ifdef _WIN32
    HANDLE process_handle = nullptr;
    HANDLE stdout_read = nullptr;
    DWORD process_id = 0;
#else
    pid_t process_handle = -1;
    int stdout_read = -1;
    pid_t process_id = -1;
#endif
    std::shared_ptr<OutputTracker> tracker = std::make_shared<OutputTracker>();
    std::atomic<bool> stop_requested{false};
    std::atomic<bool> running_marked{false};
};

RoomTaskManager::RoomTaskManager(const ServerConfig& config, PersistentStore& store)
    : config_(config), store_(store) {}

RoomTaskManager::~RoomTaskManager() {
    Shutdown();
}

bool RoomTaskManager::Recover(std::string& error) {
    const std::vector<RoomRecord> rooms = store_.ListRooms(true, "");
    const std::int64_t now = NowUnix();
    for (const auto& room : rooms) {
        if (room.managed_status != "active") {
            continue;
        }

        std::string room_error;
        if (!ValidateRoomForManagedStart(room, room_error)) {
            RoomRecord ignored;
            if (!MoveRoomToFailed(room.id, "recovery blocked: " + room_error, &ignored, error)) {
                return false;
            }
            continue;
        }

        const std::int64_t delay = room.runtime_status == "retry_wait" && room.next_retry_at > now ? room.next_retry_at - now : 0;
        const std::uint64_t token = InvalidateRetryToken(room.id);
        if (delay > 0) {
            ArmRetryTimer(room.id, token, delay);
            continue;
        }

        RoomStatePatch patch;
        patch.set_last_start_attempt_at = true;
        patch.last_start_attempt_at = now;
        patch.set_next_retry_at = true;
        patch.next_retry_at = 0;
        patch.append_log = true;
        patch.log_line = "Recovered managed room launch";

        RoomRecord updated;
        if (!store_.SetRoomState(room.id, "active", "starting", patch, updated, error)) {
            return false;
        }
        if (!LaunchRoom(room.id, updated, error)) {
            return false;
        }
    }
    return true;
}

bool RoomTaskManager::ValidateRoomForManagedStart(const RoomRecord& room, std::string& error) const {
    if (ResolveFfmpegExecutable(config_).empty()) {
        error = "ffmpeg executable not found";
        return false;
    }
    if (Trim(room.name).empty()) {
        error = "name is required";
        return false;
    }
    if (Trim(room.rtmp_url).empty()) {
        error = "rtmp_url is required";
        return false;
    }
    if (room.mode == "network") {
        if (Trim(room.input_url).empty()) {
            error = "input_url is required for network mode";
            return false;
        }
        return true;
    }
    if (room.mode == "file") {
        if (Trim(room.file_id).empty()) {
            error = "file_id is required for file mode";
            return false;
        }
        FileRecord file;
        if (!store_.GetFileByID(room.file_id, file) || file.status != "ready" || file.user_id != room.owner_id) {
            error = "file not found";
            return false;
        }
        const std::filesystem::path source = ResolveFileSourcePath(config_, file);
        if (!std::filesystem::exists(source) || !std::filesystem::is_regular_file(source)) {
            error = "file source is missing";
            return false;
        }
        return true;
    }
    error = "unsupported room mode";
    return false;
}

bool RoomTaskManager::StartRoom(const std::string& room_id, RoomRecord& out, std::string& error) {
    RoomRecord room;
    if (!store_.GetRoomByID(room_id, room)) {
        error = "room not found";
        return false;
    }
    if (room.managed_status == "active") {
        out = room;
        error = "room is already active";
        return false;
    }

    std::string validation_error;
    if (!ValidateRoomForManagedStart(room, validation_error)) {
        out = room;
        error = validation_error;
        return false;
    }

    UserRecord owner;
    if (!store_.GetUserByID(room.owner_id, owner)) {
        error = "room owner not found";
        return false;
    }
    if (owner.status != "active") {
        error = "room owner account is unavailable";
        return false;
    }
    const std::int64_t now = NowUnix();
    if (owner.subscription_ends_at > 0 && owner.subscription_ends_at < now) {
        error = "subscription expired";
        return false;
    }
    const StorageUsage usage = store_.UsageForUser(owner);
    if (usage.active_room_count >= owner.max_active_rooms) {
        error = "active room quota exceeded";
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutting_down_) {
            error = "room task manager is shutting down";
            return false;
        }
        if (running_.find(room_id) != running_.end()) {
            error = "room process is already running";
            return false;
        }
    }

    RoomStatePatch patch;
    patch.set_last_error = true;
    patch.last_error.clear();
    patch.set_retry_count = true;
    patch.retry_count = 0;
    patch.set_next_retry_at = true;
    patch.next_retry_at = 0;
    patch.set_last_start_attempt_at = true;
    patch.last_start_attempt_at = now;
    patch.set_activated_at = true;
    patch.activated_at = now;
    patch.set_stopped_at = true;
    patch.stopped_at = 0;
    patch.append_log = true;
    patch.log_line = "Room activated";

    if (!store_.SetRoomState(room_id, "active", "starting", patch, out, error)) {
        return false;
    }
    InvalidateRetryToken(room_id);
    return LaunchRoom(room_id, out, error);
}

bool RoomTaskManager::LaunchRoom(const std::string& room_id, RoomRecord& out, std::string& error) {
    RoomRecord room;
    if (!store_.GetRoomByID(room_id, room)) {
        error = "room not found";
        return false;
    }
    if (room.managed_status != "active") {
        error = "room is not active";
        return false;
    }

    std::string validation_error;
    if (!ValidateRoomForManagedStart(room, validation_error)) {
        return MoveRoomToFailed(room_id, validation_error, &out, error);
    }

    const std::string ffmpeg_exec = ResolveFfmpegExecutable(config_);
    if (ffmpeg_exec.empty()) {
        return MoveRoomToFailed(room_id, "ffmpeg executable not found", &out, error);
    }

    std::vector<std::string> args = {
        ffmpeg_exec,
        "-loglevel", "info",
    };
    if (room.mode == "file") {
        FileRecord file;
        if (!store_.GetFileByID(room.file_id, file) || file.status != "ready" || file.user_id != room.owner_id) {
            return MoveRoomToFailed(room_id, "file not found", &out, error);
        }
        const std::filesystem::path source = ResolveFileSourcePath(config_, file);
        args.push_back("-re");
        args.push_back("-stream_loop");
        args.push_back("-1");
        args.push_back("-i");
        args.push_back(source.string());
    } else {
        args.push_back("-i");
        args.push_back(room.input_url);
    }
    args.push_back("-c");
    args.push_back("copy");
    args.push_back("-f");
    args.push_back("flv");
    args.push_back("-flvflags");
    args.push_back("no_duration_filesize");
    args.push_back("-rtmp_buffer");
    args.push_back("5000");
    args.push_back("-rtmp_live");
    args.push_back("live");
    args.push_back(room.rtmp_url);

#ifdef _WIN32
    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    HANDLE read_pipe = nullptr;
    HANDLE write_pipe = nullptr;
    if (!CreatePipe(&read_pipe, &write_pipe, &sa, 0)) {
        return MoveRoomToRetry(room_id, LastProcessError("CreatePipe failed"), NowUnix(), &out, error);
    }
    if (!SetHandleInformation(read_pipe, HANDLE_FLAG_INHERIT, 0)) {
        CloseHandle(read_pipe);
        CloseHandle(write_pipe);
        return MoveRoomToRetry(room_id, LastProcessError("SetHandleInformation failed"), NowUnix(), &out, error);
    }

    std::string command_line = JoinCommandLine(args);
    std::vector<char> command_buffer(command_line.begin(), command_line.end());
    command_buffer.push_back('\0');

    STARTUPINFOA startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = write_pipe;
    startup.hStdError = write_pipe;

    PROCESS_INFORMATION process{};
    if (!CreateProcessA(nullptr, command_buffer.data(), nullptr, nullptr, TRUE, CREATE_NO_WINDOW, nullptr, nullptr, &startup, &process)) {
        CloseHandle(read_pipe);
        CloseHandle(write_pipe);
        return MoveRoomToRetry(room_id, LastProcessError("CreateProcess failed"), NowUnix(), &out, error);
    }
    CloseHandle(write_pipe);
    CloseHandle(process.hThread);
#else
    int pipe_fds[2] = {-1, -1};
    if (::pipe(pipe_fds) != 0) {
        return MoveRoomToRetry(room_id, LastProcessError("pipe failed"), NowUnix(), &out, error);
    }

    const int read_pipe = pipe_fds[0];
    const int write_pipe = pipe_fds[1];
    if (::fcntl(read_pipe, F_SETFD, FD_CLOEXEC) == -1) {
        ::close(read_pipe);
        ::close(write_pipe);
        return MoveRoomToRetry(room_id, LastProcessError("fcntl failed"), NowUnix(), &out, error);
    }

    const pid_t process_id = ::fork();
    if (process_id < 0) {
        ::close(read_pipe);
        ::close(write_pipe);
        return MoveRoomToRetry(room_id, LastProcessError("fork failed"), NowUnix(), &out, error);
    }

    if (process_id == 0) {
        ::dup2(write_pipe, STDOUT_FILENO);
        ::dup2(write_pipe, STDERR_FILENO);
        const int null_input = ::open("/dev/null", O_RDONLY);
        if (null_input >= 0) {
            ::dup2(null_input, STDIN_FILENO);
            if (null_input > STDERR_FILENO) {
                ::close(null_input);
            }
        }
        ::close(read_pipe);
        ::close(write_pipe);

        std::vector<char*> argv;
        argv.reserve(args.size() + 1);
        for (std::string& arg : args) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);
        ::execvp(ffmpeg_exec.c_str(), argv.data());
        std::_Exit(127);
    }

    ::close(write_pipe);
#endif

    auto running = std::make_shared<RunningRoom>();
    running->process_handle =
#ifdef _WIN32
        process.hProcess;
    running->stdout_read = read_pipe;
    running->process_id = process.dwProcessId;
#else
        process_id;
    running->stdout_read = read_pipe;
    running->process_id = process_id;
#endif

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutting_down_) {
            running->stop_requested.store(true);
#ifdef _WIN32
            TerminateProcess(process.hProcess, 1);
            CloseHandle(process.hProcess);
            CloseHandle(read_pipe);
#else
            ::kill(process_id, SIGKILL);
            ::close(read_pipe);
#endif
            error = "room task manager is shutting down";
            return false;
        }
        running_[room_id] = running;
    }

    RoomRecord logged_room;
    std::string ignored_error;
    store_.AppendRoomLog(room_id, "FFmpeg relay process started (pid=" + std::to_string(running->process_id) + ")", logged_room, ignored_error);
    out = room;

    StartOutputReader(room_id, running);
    WaitForProcessExit(room_id, running);
    return true;
}

bool RoomTaskManager::MoveRoomToRetry(const std::string& room_id, const std::string& last_error, std::int64_t last_exit_at,
                                      RoomRecord* out, std::string& error) {
    RoomRecord room;
    if (!store_.GetRoomByID(room_id, room)) {
        error = "room not found";
        return false;
    }

    const int retry_count = room.retry_count + 1;
    const std::int64_t next_retry_at = NowUnix() + std::max<std::int64_t>(1, config_.room_retry_delay_seconds);
    RoomStatePatch patch;
    patch.set_last_error = true;
    patch.last_error = last_error;
    patch.set_retry_count = true;
    patch.retry_count = retry_count;
    patch.set_next_retry_at = true;
    patch.next_retry_at = next_retry_at;
    patch.set_last_exit_at = true;
    patch.last_exit_at = last_exit_at;
    patch.append_log = true;
    patch.log_line = FormatRetryLog(next_retry_at, retry_count);

    RoomRecord updated;
    if (!store_.SetRoomState(room_id, "active", "retry_wait", patch, updated, error)) {
        return false;
    }

    const std::uint64_t token = InvalidateRetryToken(room_id);
    ArmRetryTimer(room_id, token, std::max<std::int64_t>(1, config_.room_retry_delay_seconds));
    if (out != nullptr) {
        *out = updated;
    }
    return true;
}

bool RoomTaskManager::MoveRoomToFailed(const std::string& room_id, const std::string& last_error, RoomRecord* out, std::string& error) {
    RoomStatePatch patch;
    patch.set_last_error = true;
    patch.last_error = last_error;
    patch.set_next_retry_at = true;
    patch.next_retry_at = 0;
    patch.append_log = true;
    patch.log_line = last_error;

    RoomRecord updated;
    if (!store_.SetRoomState(room_id, "active", "failed", patch, updated, error)) {
        return false;
    }
    if (out != nullptr) {
        *out = updated;
    }
    return true;
}

bool RoomTaskManager::StopRoom(const std::string& room_id, RoomRecord& out, std::string& error) {
    RoomRecord room;
    if (!store_.GetRoomByID(room_id, room)) {
        error = "room not found";
        return false;
    }
    if (room.managed_status != "active") {
        out = room;
        error = "room is not active";
        return false;
    }

    const std::int64_t now = NowUnix();
    std::shared_ptr<RunningRoom> running;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++retry_tokens_[room_id];
        const auto it = running_.find(room_id);
        if (it != running_.end()) {
            running = it->second;
            running->stop_requested.store(true);
        }
    }

    if (running == nullptr) {
        RoomStatePatch patch;
        patch.set_last_error = true;
        patch.last_error.clear();
        patch.set_next_retry_at = true;
        patch.next_retry_at = 0;
        patch.set_last_exit_at = true;
        patch.last_exit_at = now;
        patch.set_stopped_at = true;
        patch.stopped_at = now;
        patch.append_log = true;
        patch.log_line = "Room stopped";
        return store_.SetRoomState(room_id, "inactive", "idle", patch, out, error);
    }

    RoomStatePatch patch;
    patch.set_last_error = true;
    patch.last_error.clear();
    patch.set_next_retry_at = true;
    patch.next_retry_at = 0;
    patch.set_stopped_at = true;
    patch.stopped_at = now;
    patch.append_log = true;
    patch.log_line = "Room stopping";
    if (!store_.SetRoomState(room_id, "inactive", "stopping", patch, out, error)) {
        return false;
    }
#ifdef _WIN32
    if (!TerminateProcess(running->process_handle, 1)) {
        error = LastProcessError("TerminateProcess failed");
        return false;
    }
#else
    if (::kill(running->process_handle, SIGKILL) != 0 && errno != ESRCH) {
        error = LastProcessError("kill failed");
        return false;
    }
#endif
    return WaitForIdle(room_id, out, error);
}

bool RoomTaskManager::WaitForIdle(const std::string& room_id, RoomRecord& out, std::string& error) {
    for (int attempt = 0; attempt < 50; ++attempt) {
        RoomRecord room;
        if (!store_.GetRoomByID(room_id, room)) {
            error = "room not found";
            return false;
        }
        if (room.managed_status == "inactive" && room.runtime_status == "idle") {
            out = room;
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!store_.GetRoomByID(room_id, out)) {
        error = "room not found";
        return false;
    }
    return true;
}

bool RoomTaskManager::DeleteRoom(const std::string& room_id, std::string& error) {
    RoomRecord room;
    if (!store_.GetRoomByID(room_id, room)) {
        error = "room not found";
        return false;
    }
    if (room.managed_status == "active") {
        error = "room is still active; stop the room first";
        return false;
    }
    if (room.runtime_status == "stopping") {
        error = "room is still stopping";
        return false;
    }
    if (room.runtime_status != "idle") {
        error = "room is not idle yet";
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++retry_tokens_[room_id];
        if (running_.find(room_id) != running_.end()) {
            error = "room process is still running";
            return false;
        }
    }
    return store_.DeleteRoom(room_id, error);
}

void RoomTaskManager::StartOutputReader(const std::string& room_id, const std::shared_ptr<RunningRoom>& running) {
    std::thread([this, room_id, running]() {
        std::string carry;
        char buffer[4096];
        for (;;) {
#ifdef _WIN32
            DWORD bytes_read = 0;
            const bool ok = ReadFile(running->stdout_read, buffer, sizeof(buffer), &bytes_read, nullptr) != 0;
            if (!ok || bytes_read == 0) {
                break;
            }
#else
            const ssize_t bytes_read = ::read(running->stdout_read, buffer, sizeof(buffer));
            if (bytes_read <= 0) {
                break;
            }
#endif
            carry.append(buffer, buffer + bytes_read);
            for (;;) {
                std::size_t split = carry.find_first_of("\r\n");
                if (split == std::string::npos) {
                    break;
                }
                std::string line = Trim(carry.substr(0, split));
                carry.erase(0, split + 1);
                if (!line.empty()) {
                    HandleOutputLine(room_id, running, line);
                }
            }
        }
        const std::string tail = Trim(carry);
        if (!tail.empty()) {
            HandleOutputLine(room_id, running, tail);
        }
        if (
#ifdef _WIN32
            running->stdout_read != nullptr
#else
            running->stdout_read >= 0
#endif
        ) {
#ifdef _WIN32
            CloseHandle(running->stdout_read);
            running->stdout_read = nullptr;
#else
            ::close(running->stdout_read);
            running->stdout_read = -1;
#endif
        }
    }).detach();
}

void RoomTaskManager::WaitForProcessExit(const std::string& room_id, const std::shared_ptr<RunningRoom>& running) {
    std::thread([this, room_id, running]() {
#ifdef _WIN32
        WaitForSingleObject(running->process_handle, INFINITE);
        DWORD exit_code = 0;
        GetExitCodeProcess(running->process_handle, &exit_code);
        CloseHandle(running->process_handle);
        running->process_handle = nullptr;
#else
        int status = 0;
        while (::waitpid(running->process_handle, &status, 0) < 0) {
            if (errno != EINTR) {
                break;
            }
        }
        int exit_code = -1;
        if (WIFEXITED(status)) {
            exit_code = WEXITSTATUS(status);
        } else if (WIFSIGNALED(status)) {
            exit_code = 128 + WTERMSIG(status);
        }
        running->process_handle = -1;
#endif

        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = running_.find(room_id);
            if (it != running_.end() && it->second.get() == running.get()) {
                running_.erase(it);
            }
        }

        RoomRecord room;
        if (!store_.GetRoomByID(room_id, room)) {
            return;
        }

        const std::int64_t now = NowUnix();
        std::string error;
        if (running->stop_requested.load() || room.managed_status != "active") {
            RoomStatePatch patch;
            patch.set_last_error = true;
            patch.last_error.clear();
            patch.set_next_retry_at = true;
            patch.next_retry_at = 0;
            patch.set_last_exit_at = true;
            patch.last_exit_at = now;
            patch.set_stopped_at = true;
            patch.stopped_at = room.stopped_at > 0 ? room.stopped_at : now;
            patch.append_log = true;
            patch.log_line = "Room stopped";
            RoomRecord ignored;
            if (!store_.SetRoomState(room_id, "inactive", "idle", patch, ignored, error)) {
                std::cerr << "set room idle failed: " << error << std::endl;
            }
            return;
        }

        const std::vector<std::string> excerpt = running->tracker->Excerpt(4);
        if (!excerpt.empty()) {
            RoomRecord ignored;
            std::string append_error;
            if (!store_.AppendRoomLog(room_id, "recent ffmpeg output before failure (" + std::to_string(excerpt.size()) + " lines):", ignored, append_error)) {
                std::cerr << "append room log failed: " << append_error << std::endl;
            }
            for (const auto& line : excerpt) {
                if (!store_.AppendRoomLog(room_id, "[context] " + line, ignored, append_error)) {
                    std::cerr << "append room log failed: " << append_error << std::endl;
                }
            }
        }

        RoomRecord ignored;
        std::string append_error;
        const std::string exit_line = exit_code == 0 ? "process exited unexpectedly" : "process exited: code " + std::to_string(exit_code);
        if (!store_.AppendRoomLog(room_id, exit_line, ignored, append_error)) {
            std::cerr << "append room log failed: " << append_error << std::endl;
        }
        if (!MoveRoomToRetry(room_id, exit_line, now, nullptr, error)) {
            std::cerr << "move room to retry failed: " << error << std::endl;
        }
    }).detach();
}

void RoomTaskManager::HandleOutputLine(const std::string& room_id, const std::shared_ptr<RunningRoom>& running, const std::string& line) {
    running->tracker->Remember(line);
    const bool is_progress = IsProgressLine(line);
    if (is_progress && !running->running_marked.exchange(true)) {
        RoomStatePatch patch;
        patch.set_last_error = true;
        patch.last_error.clear();
        patch.set_retry_count = true;
        patch.retry_count = 0;
        patch.set_next_retry_at = true;
        patch.next_retry_at = 0;
        patch.set_last_running_at = true;
        patch.last_running_at = NowUnix();
        RoomRecord ignored;
        std::string error;
        if (!store_.SetRoomState(room_id, "active", "running", patch, ignored, error)) {
            std::cerr << "mark room running failed: " << error << std::endl;
        }
    }

    RoomRecord ignored;
    std::string error;
    if (!store_.AppendRoomLog(room_id, line, ignored, error)) {
        std::cerr << "append room log failed: " << error << std::endl;
    }
}

void RoomTaskManager::ArmRetryTimer(const std::string& room_id, std::uint64_t token, std::int64_t delay_seconds) {
    std::thread([this, room_id, token, delay_seconds]() {
        if (delay_seconds > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(delay_seconds));
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutting_down_) {
                return;
            }
            const auto it = retry_tokens_.find(room_id);
            if (it == retry_tokens_.end() || it->second != token) {
                return;
            }
        }

        RoomRecord room;
        if (!store_.GetRoomByID(room_id, room) || room.managed_status != "active" || room.runtime_status != "retry_wait") {
            return;
        }

        RoomStatePatch patch;
        patch.set_next_retry_at = true;
        patch.next_retry_at = 0;
        patch.set_last_start_attempt_at = true;
        patch.last_start_attempt_at = NowUnix();
        patch.append_log = true;
        patch.log_line = "Retrying room launch";

        RoomRecord updated;
        std::string error;
        if (!store_.SetRoomState(room_id, "active", "starting", patch, updated, error)) {
            std::cerr << "set room starting failed: " << error << std::endl;
            return;
        }
        if (!LaunchRoom(room_id, updated, error)) {
            std::cerr << "retry launch failed: " << error << std::endl;
        }
    }).detach();
}

std::uint64_t RoomTaskManager::InvalidateRetryToken(const std::string& room_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return ++retry_tokens_[room_id];
}

void RoomTaskManager::Shutdown() {
    std::vector<std::shared_ptr<RunningRoom>> to_stop;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutting_down_) {
            return;
        }
        shutting_down_ = true;
        for (auto& entry : running_) {
            entry.second->stop_requested.store(true);
            to_stop.push_back(entry.second);
        }
        running_.clear();
        retry_tokens_.clear();
    }

    for (const auto& running : to_stop) {
        if (
#ifdef _WIN32
            running->process_handle != nullptr
#else
            running->process_handle > 0
#endif
        ) {
#ifdef _WIN32
            TerminateProcess(running->process_handle, 1);
#else
            ::kill(running->process_handle, SIGKILL);
#endif
        }
    }
}
