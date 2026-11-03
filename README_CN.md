<div align="center">
  <img src="assets/images/banner.jpeg" alt="Octop Gateway" width="600" />
  <h1>Octop Gateway</h1>
</div>

<p align="center">
  <strong>多平台 IM 通道桥接 —— 用一套抽象接口，让 AI Agent 连接任意即时通讯平台。</strong>
</p>

<p align="center">
  <a href="https://www.python.org/downloads/"><img alt="Python 3.12+" src="https://img.shields.io/badge/python-3.12%2B-blue?logo=python&logoColor=white" /></a>
  <a href="https://github.com/TencentCloud/octop-gateway/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green" /></a>
  <a href="https://pypi.org/project/octop-gateway/"><img src="https://img.shields.io/pypi/v/octop-gateway" alt="PyPI" /></a>
  <a href="https://github.com/astral-sh/ruff"><img alt="Code Style: Ruff" src="https://img.shields.io/badge/code%20style-ruff-000000?logo=ruff&logoColor=white" /></a>
  <a href="https://github.com/TencentCloud/octop-gateway"><img alt="GitHub stars" src="https://img.shields.io/github/stars/TencentCloud/octop-gateway?style=social" /></a>
</p>

<p align="center">
  <a href="README.md">English</a> · <b>中文</b>
</p>

<p align="center">
  <a href="#什么是-octop-gateway">什么是 Octop Gateway？</a> ·
  <a href="#为什么选择-octop-gateway">为什么选择 Octop Gateway？</a> ·
  <a href="#如何使用">如何使用</a> ·
  <a href="#文档">文档</a>
</p>

---

## 什么是 Octop Gateway？

**octop-gateway** 是一个面向 AI Agent 与机器人的多平台 IM 通道桥接库，提供统一的消息抽象。它连接各大主流 IM 平台，把每一条入站消息归一为同一条处理管线——你的 Agent 逻辑**只写一次**，即可在飞书、钉钉、QQ、企业微信、微信 iLink、元宝、小艺、MQTT、Telegram 与 Discord 上运行。

> Harness Gateway 的设计目标：你只需写一个异步消息处理器，网关便会为每个平台处理连接、解析、媒体与回传，并以统一的接口交付给你。

## 为什么选择 Octop Gateway？

| | 特性 | 说明 |
|---|------|------|
| 🔌 | **10 个平台，一个处理器** | 飞书、钉钉、QQ、企业微信、微信 iLink、元宝、小艺、MQTT、Telegram、Discord —— 全部共用一套接口 |
| 🧩 | **统一抽象** | `BaseChannel` 把各平台的差异，收敛为通用的 `InboundMessage` / `MessageEvent` 模型 |
| 📨 | **流式事件** | 你的处理器是异步生成器，产出 `MessageEvent`，原生支持逐字流式输出 |
| 💾 | **可插拔媒体** | `MediaBackend` 负责附件存储，默认提供 `FileSystemMediaBackend` |
| 🚦 | **约束控制** | 按通道设置限流、超时与"正在输入"状态 |
| 📡 | **主动推送** | `push_text` / `push_content` / `push_to_all` 用于主动消息 |
| 🏢 | **多租户** | 可同时运行多个同类型通道，彼此隔离 |
| 🐍 | **Pythonic** | 纯 `asyncio`、类型完备的模型，没有隐式魔法 |

### 核心技术

| 层级 | 技术 |
|------|------|
| **语言** | Python 3.12+ |
| **通道模型** | `BaseChannel` + 10 个内置平台适配器 |
| **消息模型** | `InboundMessage` / `MessageEvent` / `ContentPart` |
| **编排** | `ChannelManager`（异步队列 + 工作池） |
| **媒体** | `MediaBackend`（默认 `FileSystemMediaBackend`） |
| **约束** | 限流 / 超时 / 输入状态 |
| **构建 / 质量** | hatchling · ruff · mypy · pytest |

### 功能特性

#### 已支持平台

| 平台 | 通道类型 | 传输方式 | 文本 | 媒体 |
|------|----------|----------|------|------|
| 飞书（Lark） | `feishu` | WebSocket + REST | ✅ | ✅ |
| 钉钉 | `dingtalk` | Stream | ✅ | ✅ |
| QQ | `qq` | WebSocket | ✅ | ✅ |
| 企业微信 | `wecom` | 回调 + API | ✅ | ✅ |
| 微信 iLink | `weixin` | — | ✅ | ✅ |
| 元宝 | `yuanbao` | — | ✅ | ✅ |
| 小艺 | `xiaoyi` | — | ✅ | ✅ |
| MQTT | `mqtt` | MQTT | ✅ | ✅ |
| Telegram | `telegram` | 长轮询 | ✅ | ✅ |

##### Discord

使用 Bot Token + Gateway 长连接；在开发者门户开启 Message Content Intent。`allow_all_channels` 默认 `True`（包括未填写此字段的旧配置），允许机器人有权限访问的所有服务器频道。设为 `False` 后仅允许 `allowed_channel_ids` 指定的频道，列表为空则拒绝服务器频道消息。私聊始终仅允许 `allowed_user_ids` 中的用户，列表为空则拒绝私聊。频道默认需要直接 @机器人，线程继承父频道权限。支持 HTTP 代理、附件、typing、长回复分段。参见 [`examples/discord_bot.py`](examples/discord_bot.py)。

#### 统一消息抽象
- `InboundMessage` 携带文本、结构化的 `ContentPart`（文本 / 图片 / 视频 / 音频 / 文件）以及 `ChannelSubject`。
- 你的处理器是 `Callable[[InboundMessage], AsyncIterator[MessageEvent]]`——可产出 `MESSAGE` 表示完整文本，`DELTA` 表示逐字流式，`COMPLETED` 表示刷出。
- `ChannelConfig` 是每个平台的类型化 dataclass；`BaseChannel` 定义 `start` / `stop` / `parse_inbound` / `_send_*`。

#### 自定义通道
继承 `BaseChannel`，通过 `ChannelManager.add_channel(...)` 注册，管线其余部分（媒体、约束、推送）无需改动即可工作。

#### 飞书响应卡片

飞书默认将一次请求的回复集中在一张持续更新的共享卡片中：业务步骤和当前工作直接展示，技术过程在次要区域折叠查看。
事件合并后每秒最多更新一次；当前步骤下只显示最新一条灰色活动提示，公开业务说明与真实工具事件原地替换。
不根据执行指令推断“编写代码”，不把工具返回当作业务完成，切换步骤时清除上一阶段的提示。
等待圆圈使用客户端连续播放的 GIF（0.8 秒一圈），已用时间、当前步骤持续时间和最近活动时间约每十秒刷新。
每个通道只尝试上传一次内置动图，最多等待两秒；上传失败时使用静态圆圈，不影响正常回复。
这些时间是网关观察窗口，不代表模型或工具的精确执行耗时。动效仅表示等待下一次更新，
不发起模型/业务查询，不自动推进步骤；相同计划或保活不会重置活动时间。完成、失败或停止后撤掉动效。

卡片采用 JSON 2.0（飞书客户端 7.20 及以上），使用已有的应用消息发送、更新接口。
原生折叠面板无需配置卡片回调服务器。文件和图片仍以原生消息交付。
长正文在生成时仅显示节选，结束时完整分段发送；卡片发送或更新失败时降级为最终文本，不重新执行模型或工具。
完整终态卡片被拒绝时，正文送达后再用仅含固定文案的简洁卡片关闭原进度卡；正文发送失败时显示“回复未送达”，不声称发送成功。
简洁终态也保留“步骤尚未全部完成”等真实状态，不把响应结束等同于业务验收。
设置 `FeishuConfig(streaming_cards=False, ...)` 可恢复原有展示。

继续遵循 `show_tool_hints` 和 `show_thinking` 配置。
只有上游实际提供 `arguments`、`output`/`result` 元数据时才展示提交及返回节选，并对常见凭据字段脱敏。
仅含名称的工具结束事件显示“已返回”，不据此声称成功或验收通过。
业务步骤模式读取成功执行的标准 `write_todos` 更新，由上游通过可选的
`TYPING.metadata.task_progress` 事件发布（version 为 1、source 为 `write_todos`，
steps 包含 title 及 pending/in_progress/completed 状态）。需要配套安装 Octop
事件投影与仅面向飞书的公开进度中间件；沿用现有计划工具，不需要业务技能或平台模板。
模型尚未发布计划时，卡片明确显示“业务步骤尚未同步”。工具返回、等待时长和回复结束
均不会自动完成业务步骤。公开说明与可选的思考展示分开。旧上游仍可使用基础正文/工具卡片。

当前限制：步骤仍由模型主动汇报，可能在实际工作已进入后续阶段后才集中更新。
已写入中间计划不等于飞书客户端已实时显示；正式验收还需核对事件转发、卡片更新回执及实际显示。
本实现不通过定时补勾或重放动画伪造中间阶段。

#### 约束、媒体与推送
- **约束** —— 按通道设置限流、响应超时与"正在输入"状态。
- **媒体** —— 可插拔 `MediaBackend`，附件随意落盘。
- **推送** —— `push_text` / `push_content` 定向发送，`push_to_all` 用于广播。

#### 多租户
可同时运行多个同类型通道（例如两个飞书应用对应两个团队），各自以 `channel_id` 隔离。

## 如何使用

### 环境要求
- **Python 3.12+**
- 所接入平台的凭据

### 1. 安装

```bash
# 核心库
pip install octop-gateway

# 附带示例 / Agent 集成依赖
pip install "octop-gateway[examples]"
```

### 2. 最小回声机器人（Telegram）

```python
import asyncio, os
from collections.abc import AsyncIterator

from octop_gateway import ChannelManager, InboundMessage, MessageEvent
from octop_gateway.channels.telegram import TelegramConfig


async def echo(message: InboundMessage) -> AsyncIterator[MessageEvent]:
    yield MessageEvent.text(f"Echo: {message.text}")
    yield MessageEvent.completed()


async def main():
    manager = ChannelManager(processor=echo)
    await manager.start()
    await manager.add_telegram_channel(TelegramConfig(bot_token=os.environ["TELEGRAM_BOT_TOKEN"]))
    await asyncio.Event().wait()


asyncio.run(main())
```

### 3. 接入更多平台

```python
import asyncio, os
from collections.abc import AsyncIterator

from octop_gateway import ChannelManager, InboundMessage, MessageEvent
from octop_gateway.channels.dingtalk import DingTalkConfig
from octop_gateway.channels.feishu import FeishuConfig
from octop_gateway.channels.qq import QQConfig


async def unified_bot(msg: InboundMessage) -> AsyncIterator[MessageEvent]:
    yield MessageEvent.text(f"[{msg.channel_type}] {msg.text}")
    yield MessageEvent.completed()


async def main():
    manager = ChannelManager(processor=unified_bot, workers_per_channel=4)
    await manager.start()

    await manager.add_feishu_channel(
        FeishuConfig(app_id=os.environ["FEISHU_APP_ID"], app_secret=os.environ["FEISHU_APP_SECRET"])
    )
    await manager.add_qq_channel(
        QQConfig(app_id=os.environ["QQ_APP_ID"], token=os.environ["QQ_TOKEN"], secret=os.environ["QQ_SECRET"])
    )
    await manager.add_dingtalk_channel(
        DingTalkConfig(app_key=os.environ["DINGTALK_APP_KEY"], app_secret=os.environ["DINGTALK_APP_SECRET"])
    )
    await asyncio.Event().wait()


asyncio.run(main())
```

将 `.env.example` 复制为 `.env` 进行环境变量配置。

## 文档

- [什么是 Octop Gateway？](#什么是-octop-gateway)
- [为什么选择 Octop Gateway？](#为什么选择-octop-gateway)
- [如何使用](#如何使用)

### 架构

```
ChannelManager
 ├─ 异步队列 + 工作池（每通道）
 ├─ add_channel(BaseChannel) / add_*_channel(...)
 ├─ push_text / push_content / push_to_all
 └─ 每通道一个 BaseChannel
      ├─ start / stop
      ├─ parse_inbound → InboundMessage
      └─ _send_text / _send_content / _send_media

MessageProcessor: InboundMessage → AsyncIterator[MessageEvent]
```

每个 `BaseChannel` 管理自己的传输；管理器负责调度、媒体、约束与分发。你的处理器只看到归一化后的事件流。

### 群命令权限（QQ 示例）

使用公共群策略的通道默认启用群内命令保护，Python 配置对象及未填写该字段的旧 JSON 配置也会启用。下面以 QQ 为例：

```json
{
  "group_context": {
    "enabled": true,
    "owner_commands_only": true,
    "owner_ids": []
  }
}
```

`owner_ids` 填写可信发送者的 `author.member_openid`，与消息元数据中的
`sender_id` 匹配；不要填写 QQ 号、群 ID 或 Octop 用户 ID。白名单为空时，
所有成员的群内命令都被忽略，可先通过 Dashboard/CLI 管理。
两个字段均支持在现有 `group_context.groups` 中按群覆盖。

仅匹配消息开头符合完整语法的 Octop 内置斜杠命令及别名，例如 `/model`、
`/new`、`/help`、`/delete`、`/stop`。消息中间的 `/model`、URL、
`/tmp/file` 等路径、代码块和未登记的自定义斜杠名称仍按普通消息处理。
Octop 新增命令时，应同步 `group_context.py` 中的内置名称表。

非所有者命令在宿主控制回调、媒体下载和处理器调用前静默丢弃，也不会进入
临时群上下文。关闭 `group_context.enabled` 只关闭触发和上下文处理，命令
保护继续生效；显式设置 `owner_commands_only: false` 才会放开群命令。
私聊和消息发送流程沿用现有行为。该默认值同样适用于其他使用公共群策略的通道；如需放开群命令，应显式设置 `owner_commands_only: false`。

该策略不限制自然语言请求或 Agent 工具权限，应用仍需配置适当的工具权限。

### 开发

**环境要求：** Python 3.12+、[uv](https://docs.astral.sh/uv/)

```bash
make install          # pip install -e ".[dev,examples]"
make all              # 格式化 + 类型检查 + 测试
```

## 🤝 贡献

1. Fork 本仓库
2. 创建特性分支（`git checkout -b feature/amazing-feature`）
3. 提交前运行 `make all`
4. 向 `main` 发起 Pull Request

通道实现规范见 [AGENTS.md](AGENTS.md)。

分支、PR 与发版流程见 [CONTRIBUTING.md](CONTRIBUTING.md)（`release/*` → `main` 合并后自动发布到 PyPI）。

## 🔗 相关项目

| 项目 | 说明 |
|------|------|
| [octop-harness](https://github.com/TencentCloud/octop-harness) | 驱动网关处理器的 Agent 运行时 |
| [octop-memory](https://github.com/TencentCloud/octop-memory) | 面向网关 Agent 的记忆系统 |
| [octop-browser](https://github.com/TencentCloud/octop-browser) | 供 Agent 使用的浏览器自动化 |
| [Octop](https://github.com/TencentCloud/Octop) | 组合 Harness 技术栈的自托管助手 |

## 📄 许可证

本项目基于 [MIT 许可证](LICENSE) 开源。

## ✨ 贡献者

感谢所有贡献者：

<a href="https://github.com/TencentCloud/octop-gateway/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=TencentCloud/octop-gateway" />
</a>
