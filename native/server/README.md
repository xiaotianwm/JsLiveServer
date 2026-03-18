# JsLive Native Server

This is the first C++ service-side migration baseline for JsLive.

Current scope:

- integrated HTTP API and RTMP listener in one process
- health and readiness endpoints
- login, logout, and session resolution
- `me` endpoint
- admin dashboard, trends, system config, users, rooms, files, uploads list endpoints
- user-side rooms, files, uploads list endpoints
- room create, start, stop, and delete endpoints for user/admin paths
- packaged config resolution prefers `server.conf` beside the executable
- relative `database.path` and `storage.root` are resolved from the executable directory
- flat-file persistence with seeded demo data

Build:

```powershell
powershell -ExecutionPolicy Bypass -File .\native\server\build.ps1
```

```bash
cd ./native/server
./build.sh
```

Direct CMake usage:

```powershell
cmake -S .\native\server -B .\native\server\build\cmake -G "MinGW Makefiles" -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=C:\msys64\ucrt64\bin\g++.exe
cmake --build .\native\server\build\cmake --config Release
```

```bash
cmake -S ./native/server -B ./native/server/build/cmake -DCMAKE_BUILD_TYPE=Release
cmake --build ./native/server/build/cmake --config Release
```

Run:

```powershell
cd .\native\server\build
.\jslive_native_server.exe
```

```bash
cd ./native/server/build
./jslive_native_server
```

Notes:

- Service build is now driven by CMake.
- Packaged output is written to `native/server/build`, while CMake's own files live in `native/server/build/cmake`.
- `server.conf` in the executable directory has higher priority than any startup argument.
- Windows build packages `ffmpeg.exe` into `native/server/build`.
- Linux build packages `ffmpeg` into `native/server/build` when a bundled binary exists under `gosrc/bin/ffmpeg/linux-amd64`, `gosrc/server/runtime/ffmpeg/linux-amd64`, or `gosrc/third_party/ffmpeg/linux-amd64`.
- If no bundled Linux ffmpeg is present, the service falls back to `ffmpeg` from `PATH`.

Seed accounts:

- `admin / admin123`
- `user / user123`
