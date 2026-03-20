# SSH 网络设备批量执行工具

这是一个基于 Python 的小工具，用于通过 SSH 登录网络设备，读取 `commands.txt` 中的命令并执行，然后分别记录运行日志和命令结果。

支持能力：

- 从 `device.json` 读取单台或多台设备信息
- 从 `commands.txt` 读取要执行的命令
- 多设备并行执行
- 支持设备失败自动重试（默认 3 次）
- 支持单设备总执行超时控制，避免卡死
- 自动记录运行日志
- 每台设备每次执行生成独立结果文件
- 支持失败设备清单输出
- 支持失败原因分类统计
- 支持忽略失败并返回成功退出码
- 登录后自动检测设备原始 prompt，并记录到日志
- 命令修改 prompt 后自动切换后续命令的 prompt 检测值
- 自动清洗终端控制字符，避免结果文件出现乱码

## 文件说明

- `ssh_command_runner.py`
  主脚本
- `device.json`
  设备信息配置文件
- `commands.txt`
  要执行的设备命令列表
- `log/run.log`
  脚本运行日志
- `log/failed_devices.log`
  失败设备清单（默认路径，可通过参数修改）
- `<设备IP>_result_<时间戳>.log`
  每台设备每次执行生成的结果文件
- `max-workers.md`
  并发参数说明

## 环境要求

- Python 3.10+
- 已安装依赖：`paramiko`

如果当前在这个虚拟环境中执行，Python 路径可以直接使用：

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python
```

## device.json 格式

### 单台设备格式

```json
{
  "host": "135.251.214.93",
  "port": 22,
  "username": "aluadmin",
  "password": "your_password"
}
```

### 多台设备格式

```json
{
  "devices": [
    {
      "host": "135.251.214.93",
      "port": 22,
      "username": "aluadmin",
      "password": "your_password"
    },
    {
      "host": "135.251.214.94",
      "port": 22,
      "username": "aluadmin",
      "password": "your_password"
    }
  ]
}
```

也支持直接使用数组格式：

```json
[
  {
    "host": "135.251.214.93",
    "port": 22,
    "username": "aluadmin",
    "password": "your_password"
  },
  {
    "host": "135.251.214.94",
    "port": 22,
    "username": "aluadmin",
    "password": "your_password"
  }
]
```

## commands.txt 格式

`commands.txt` 中每行写一条设备命令。

规则如下：

- 空行会被忽略
- 以 `#` 开头的行会被忽略
- 脚本会自动先执行以下内置初始化命令，无需写入 `commands.txt`：
  - `environment inhibit-alarms`
  - `environment prompt "script>#"`（可通过 `--prompt` 覆盖）
- 建议第一条业务命令先关闭分页

示例：

```text
show software-mngt oswp
show equipment slot
show equipment ont interface
```

## 执行方式

### 按默认配置执行

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py
```

### 限制最大并发数为 10

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --max-workers 10
```

### 设置设备重试次数为 3 次

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --retries 3
```

### 设置每台设备总超时为 300 秒

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --session-timeout 300
```

### 即使有失败设备也返回成功退出码

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --ignore-failures
```

### 临时覆盖单台设备地址

仅适用于单设备模式：

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --host 135.251.214.93
```

### 临时指定结果文件

仅适用于单设备模式：

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --result-file /tmp/device_result.log
```

## 结果输出

### 运行日志

运行日志统一写入：

```text
log/run.log
```

日志中会带设备 IP 前缀，方便区分并发执行时的输出。

并且会输出失败原因分类统计信息。

### 失败设备清单

默认写入：

```text
log/failed_devices.log
```

可通过参数 `--failed-devices-file` 指定路径。

失败设备清单中会记录：

- Host
- 失败原因分类（如 timeout / authentication / connection 等）
- 原始错误信息
- 对应结果文件路径

### 结果文件

每次执行时，每台设备会单独生成一个结果文件，命名格式如下：

```text
<设备IP>_result_<时间戳>.log
```

例如：

```text
135.251.214.93_result_20260317_211901.log
```

结果文件中记录：

- 执行时间
- 设备 IP
- 结果文件路径
- 命令文件路径
- 每条命令及其执行输出

## 并发说明

使用 `--max-workers` 控制最大并发数量。

例如：

- `--max-workers 1`：串行执行
- `--max-workers 5`：最多同时执行 5 台设备
- `--max-workers 10`：最多同时执行 10 台设备

如果不指定该参数，默认并发数等于 `device.json` 中的设备数量。

更多说明可查看：`max-workers.md`

## Prompt 检测说明

- 脚本登录后会先自动执行内置命令 `environment prompt "script>#"` 作为默认 prompt。
- 如果你传入 `--prompt`，脚本会改为执行 `environment prompt "<你的prompt>"`，并使用该值做完成判定。
- 运行日志会输出以下信息，便于排查：
  - `Prompt value (configured)`
  - `Prompt value (detected)`
  - `Prompt value (effective)`

如果你希望固定使用某个 prompt，可直接传 `--prompt`，会覆盖自动检测结果。

## 失败处理与退出码

- 默认行为：只要有任意设备失败，脚本退出码为 `1`。
- `--ignore-failures`：即使有设备失败，脚本也返回 `0`。
- 失败设备会写入 `log/failed_devices.log`（或你指定的 `--failed-devices-file` 路径）。
- 运行日志会输出失败原因分类统计，例如：`timeout`、`authentication`、`connection`。

> 调试模式说明：在 VS Code Debug 运行时，脚本不会主动抛出 `SystemExit` 中断调试器；失败原因请以 `log/run.log` 为准。

## 推荐执行模板

### 稳妥模式（推荐生产先用）

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --max-workers 10 --retries 3 --connect-timeout 8 --session-timeout 300
```

### 流水线友好模式（有失败也返回成功）

```bash
/home/phonix/myWorkDir/code/envs/venv2/bin/python /home/phonix/myWorkDir/code/envs/venv2/ssh_command_runner.py --max-workers 10 --retries 3 --session-timeout 300 --ignore-failures
```

## 常见建议

- 生产环境建议从较小并发开始，例如 `5` 或 `10`
- 如果设备响应较慢，可以适当调大 `--command-timeout`（必须大于 120）
- 建议设置合理的 `--session-timeout`，防止坏设备长时间占用线程
- 网络波动场景建议保留默认 `--retries 3`
- 如果设备会分页，建议在命令列表中先写关闭分页命令
- 如果命令输出很大，建议避免一次执行过多重命令

## 常用参数

```text
--device-file        指定设备配置文件
--commands-file      指定命令文件
--log-file           指定运行日志文件
--result-file        指定结果文件，仅单设备模式可用
--connect-timeout    SSH 连接超时时间
--command-timeout    单条命令执行等待时间（必须大于120秒）
--command-interval   命令输出稳定等待时间
--max-workers        最大并发数
--session-timeout    每台设备总执行超时时间
--retries            每台设备失败重试次数
--failed-devices-file 失败设备清单输出文件
--ignore-failures    忽略失败设备并返回成功退出码
--prompt             指定命令完成检测的 prompt
```