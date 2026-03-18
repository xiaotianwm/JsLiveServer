# Ubuntu Server Source Package Without FFmpeg

这个目录是给 Ubuntu 单独准备的服务端源码包。
这个版本不包含 ffmpeg 二进制，编译和运行时会直接使用 Ubuntu 系统里安装的 `ffmpeg`。

目录内容：

- `native/server`：服务端源码、配置、CMake 构建脚本
- `build-server.sh`：Ubuntu 下一键编译脚本

在 Ubuntu 上执行：

```bash
cd ubuntu-server-src
chmod +x build-server.sh
./build-server.sh
```

如果系统缺少编译环境，先安装：

```bash
sudo apt update
sudo apt install -y build-essential cmake ffmpeg
```

编译完成后的产物：

```bash
native/server/build/jslive_native_server
native/server/build/server.conf
```

启动方式：

```bash
cd native/server/build
./jslive_native_server
```
