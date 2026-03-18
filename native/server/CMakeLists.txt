cmake_minimum_required(VERSION 3.20)

project(jslive_native_server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(JSLIVE_SERVER_PACKAGE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build")
file(MAKE_DIRECTORY "${JSLIVE_SERVER_PACKAGE_DIR}")

set(JSLIVE_SERVER_SOURCES
    src/runtime/rtmp/amf0.cpp
    src/config/config.cpp
    src/app/main.cpp
    src/net/net.cpp
    src/runtime/rooms/room_task_manager.cpp
    src/runtime/rooms/room_urls.cpp
    src/runtime/rtmp/rtmp_auth.cpp
    src/runtime/rtmp/rtmp_server.cpp
    src/app/server.cpp
    src/handlers/admin/server_admin_monitoring.cpp
    src/handlers/auth/server_auth.cpp
    src/handlers/media/server_files.cpp
    src/handlers/live/server_rooms.cpp
    src/handlers/live/server_rtmp_services.cpp
    src/handlers/admin/server_users.cpp
    src/shared/http/server_shared.cpp
    src/shared/users/server_user_utils.cpp
    src/handlers/media/server_uploads.cpp
    src/storage/persistence/store.cpp
    src/runtime/rtmp/stream_manager.cpp
)

add_executable(jslive_native_server ${JSLIVE_SERVER_SOURCES})
target_include_directories(jslive_native_server PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")

if(MSVC)
    target_compile_options(jslive_native_server PRIVATE /W4)
else()
    target_compile_options(jslive_native_server PRIVATE -Wall -Wextra -pedantic)
endif()

find_package(Threads REQUIRED)
target_link_libraries(jslive_native_server PRIVATE Threads::Threads)

if(WIN32)
    target_link_libraries(jslive_native_server PRIVATE ws2_32)
endif()

set_target_properties(jslive_native_server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${JSLIVE_SERVER_PACKAGE_DIR}"
    RUNTIME_OUTPUT_DIRECTORY_DEBUG "${JSLIVE_SERVER_PACKAGE_DIR}"
    RUNTIME_OUTPUT_DIRECTORY_RELEASE "${JSLIVE_SERVER_PACKAGE_DIR}"
    RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO "${JSLIVE_SERVER_PACKAGE_DIR}"
    RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL "${JSLIVE_SERVER_PACKAGE_DIR}"
)

set(JSLIVE_SERVER_CONFIG_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/config/server.conf")
set(JSLIVE_SERVER_CONFIG_OUTPUT "${JSLIVE_SERVER_PACKAGE_DIR}/server.conf")
configure_file("${JSLIVE_SERVER_CONFIG_SOURCE}" "${JSLIVE_SERVER_CONFIG_OUTPUT}" COPYONLY)

if(WIN32)
    set(JSLIVE_SERVER_FFMPEG_CANDIDATES
        "${CMAKE_CURRENT_SOURCE_DIR}/../../gosrc/bin/ffmpeg/windows-amd64/ffmpeg.exe"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../gosrc/server/runtime/ffmpeg/windows-amd64/ffmpeg.exe"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../gosrc/third_party/ffmpeg/windows-amd64/ffmpeg.exe"
    )
    set(JSLIVE_SERVER_FFMPEG_OUTPUT "${JSLIVE_SERVER_PACKAGE_DIR}/ffmpeg.exe")
else()
    set(JSLIVE_SERVER_FFMPEG_CANDIDATES
        "${CMAKE_CURRENT_SOURCE_DIR}/../../gosrc/bin/ffmpeg/linux-amd64/ffmpeg"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../gosrc/server/runtime/ffmpeg/linux-amd64/ffmpeg"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../gosrc/third_party/ffmpeg/linux-amd64/ffmpeg"
    )
    set(JSLIVE_SERVER_FFMPEG_OUTPUT "${JSLIVE_SERVER_PACKAGE_DIR}/ffmpeg")
endif()

set(JSLIVE_SERVER_FFMPEG_SOURCE "")
foreach(candidate IN LISTS JSLIVE_SERVER_FFMPEG_CANDIDATES)
    if(EXISTS "${candidate}")
        set(JSLIVE_SERVER_FFMPEG_SOURCE "${candidate}")
        break()
    endif()
endforeach()

if(JSLIVE_SERVER_FFMPEG_SOURCE)
    configure_file("${JSLIVE_SERVER_FFMPEG_SOURCE}" "${JSLIVE_SERVER_FFMPEG_OUTPUT}" COPYONLY)
    if(NOT WIN32)
        file(CHMOD "${JSLIVE_SERVER_FFMPEG_OUTPUT}"
            PERMISSIONS
                OWNER_READ OWNER_WRITE OWNER_EXECUTE
                GROUP_READ GROUP_EXECUTE
                WORLD_READ WORLD_EXECUTE
        )
    endif()
    message(STATUS "Bundled FFmpeg copied to: ${JSLIVE_SERVER_FFMPEG_OUTPUT}")
else()
    message(STATUS "Bundled FFmpeg not found for this platform; server will use ffmpeg from PATH")
endif()

message(STATUS "Server package directory: ${JSLIVE_SERVER_PACKAGE_DIR}")
