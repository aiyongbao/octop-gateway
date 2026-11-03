"""Per-response Feishu card rendering and bounded, coalesced delivery.

Only gateway events are displayed. A TOOL_END without a status means returned,
not successful. Business steps require explicit task-list metadata; tool events
never imply business completion.
"""

# ruff: noqa: RUF001 -- intentional Chinese user-facing card labels

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

from octop_gateway.models import ContentPart, MessageEvent, MessageEventType
from octop_gateway.utils import close_event_stream, extract_media, extract_text

logger = logging.getLogger(__name__)

# Stay below the 30KB message-card limit, including JSON escaping and components.
# Reserve card space for native step rows; longer answers still go out whole in posts.
_ANSWER_BYTES = 9000
_DETAIL_BYTES = 500
_VISIBLE_TOOLS = 4
_UPDATE_INTERVAL = 1.0
# The GIF animates in the client; patches only refresh events and elapsed time.
_HEARTBEAT_INTERVAL = 10.0
_DELIVERY_TIMEOUT = 10.0
_SECRET_KEY = re.compile(r"(?i)(token|secret|password|credential|authorization|api[_-]?key|cookie)")


def text_chunks(text: str, limit: int = 10000) -> Iterator[str]:
    """Split without losing Unicode characters or exceeding a byte budget."""
    current: list[str] = []
    size = 0
    for char in text:
        width = len(char.encode("utf-8"))
        if size + width > limit:
            yield "".join(current)
            current, size = [], 0
        current.append(char)
        size += width
    if current:
        yield "".join(current)


def _clip(text: str, limit: int) -> str:
    """Budget serialized bytes too: control characters expand when JSON encoded."""
    if len(json.dumps(text, ensure_ascii=False).encode("utf-8")) <= limit:
        return text
    low, high = 0, len(text)
    suffix = "\n…（节选）"
    while low < high:
        middle = (low + high + 1) // 2
        if len(json.dumps(text[:middle] + suffix, ensure_ascii=False).encode("utf-8")) <= limit:
            low = middle
        else:
            high = middle - 1
    return text[:low] + suffix


def _detail(value: Any) -> str:
    """Redact common credential fields before showing optional tool metadata."""

    def redact(item: Any) -> Any:
        if isinstance(item, dict):
            return {k: "[REDACTED]" if _SECRET_KEY.search(str(k)) else redact(v) for k, v in item.items()}
        if isinstance(item, list):
            return [redact(v) for v in item]
        return item

    if isinstance(value, str):
        with contextlib.suppress(ValueError, TypeError):
            value = json.loads(value)
    text = value if isinstance(value, str) else json.dumps(redact(value), ensure_ascii=False, default=str)
    text = re.sub(r"(?i)(bearer\s+)\S+", r"\1[REDACTED]", text)
    text = re.sub(
        r"(?i)((?:token|secret|password|api[_-]?key|authorization|cookie)[\"']?\s*[:=]\s*)[^\s,;]+",
        r"\1[REDACTED]",
        text,
    )
    # Tool data is text, not executable card markup or mentions.
    return _clip(text.replace("<", "&lt;").replace(">", "&gt;").replace("`", "'"), _DETAIL_BYTES)


def _markdown(content: str) -> dict[str, str]:
    return {"tag": "markdown", "content": content}


def _panel(title: str, content: list[dict[str, Any]], element_id: str) -> dict[str, Any]:
    return {
        "tag": "collapsible_panel",
        "element_id": element_id,
        "expanded": False,
        "header": {"title": {"tag": "plain_text", "content": title}},
        "elements": content,
    }


@dataclass
class _Tool:
    name: str
    call_id: str
    started: float = field(default_factory=time.monotonic)
    ended: float | None = None
    status: str = "调用中"
    arguments: str = ""
    result: str = ""
    step_generation: int = 0


@dataclass
class _Step:
    title: str
    status: str


def _business_steps(value: Any) -> list[_Step] | None:
    """Accept explicit producer plans; never infer work from tool names."""
    if not isinstance(value, dict) or value.get("version") != 1 or value.get("source") != "write_todos":
        return None
    rows = value.get("steps")
    if not isinstance(rows, list) or len(rows) > 12:
        return None
    steps = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        title, status = row.get("title"), row.get("status")
        if not isinstance(title, str) or not title.strip() or status not in ("pending", "in_progress", "completed"):
            return None
        # A business label is text, not a mention or a Markdown action.
        title = title.strip().replace("<", "&lt;").replace(">", "&gt;")
        title = re.sub(r"([\\`*_\[\]])", r"\\\1", title)
        steps.append(_Step(title=_clip(title, 400), status=status))
    return steps


def _loading_image(image_key: str) -> dict[str, Any]:
    return {
        "tag": "img",
        "img_key": image_key,
        "alt": {"tag": "plain_text", "content": "等待结果"},
        "size": "tiny",
        "scale_type": "crop_center",
        "transparent": True,
        "preview": False,
    }


def _step_row(step: _Step, *, running: bool, image_key: str) -> dict[str, Any]:
    """Use native columns for alignment; reserve the blue surface for live work."""
    background = "default"
    title = step.title
    if step.status == "completed":
        icon, color, label = "✓", "green", "已完成"
    elif step.status == "pending":
        icon, color, label = "○", "grey", "待开始"
        title = f"<font color='grey'>{title}</font>"
    elif running:
        icon, color, label = "◌", "blue", "进行中"
        title = f"**<font color='blue'>{title}</font>**"
        background = "blue-50"
    else:
        icon, color, label = "!", "orange", "本轮未完成"

    return {
        "tag": "column_set",
        "flex_mode": "none",
        "horizontal_spacing": "8px",
        "background_style": background,
        "columns": [
            {
                "tag": "column",
                "width": "20px",
                "padding": "0px 0px 0px 6px",
                "vertical_align": "center",
                "elements": [
                    _loading_image(image_key)
                    if image_key and running and step.status == "in_progress"
                    else _markdown(f"<font color='{color}'>{icon}</font>")
                ],
            },
            {
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "padding": "6px 0px",
                "elements": [_markdown(title)],
            },
            {
                "tag": "column",
                "width": "auto",
                "padding": "0px 8px 0px 0px",
                "vertical_align": "center",
                "elements": [{**_markdown(f"<font color='{color}'>{label}</font>"), "text_size": "notation"}],
            },
        ],
    }


@dataclass
class FeishuCardState:
    """Local state for exactly one inbound response; never shared by subjects."""

    show_tools: bool = True
    show_thinking: bool = False
    started: float = field(default_factory=time.monotonic)
    last_activity: float = field(default_factory=time.monotonic)
    status: str = "running"
    answer_parts: list[str] = field(default_factory=list)
    thinking: str = ""
    tools: list[_Tool] = field(default_factory=list)
    total_tools: int = 0
    error: str = ""
    answer_in_messages: bool = False
    revision: int = 0
    goal: str = ""
    steps: list[_Step] = field(default_factory=list)
    plan_updated_at: float | None = None
    current_step_started: float | None = None
    note_parts: list[str] = field(default_factory=list)
    last_note: str = ""
    latest_hint: str = ""
    step_generation: int = 0
    loading_image_key: str = ""

    def _tool_hint(self, event: MessageEvent, tool: _Tool) -> None:
        """Expose event facts, never shell contents or inferred business results."""
        key = str(event.metadata.get("tool_key") or event.metadata.get("tool_name") or "").rsplit("/", 1)[-1]
        if key == "write_todos" or tool.step_generation != self.step_generation:
            return
        label = {
            "execute": "执行指令",
            "read_file": "读取文件",
            "write_file": "写入文件",
            "edit_file": "修改文件",
            "grep": "检索资料",
            "glob": "查找文件",
        }.get(key, tool.name)
        self.latest_hint = (
            f"正在{label}" if tool.ended is None else f"{label} · {'工具报错' if tool.status == '失败' else '已返回'}"
        )
        # Start the next public segment fresh, preserving the whole final answer.
        self.note_parts.clear()
        self.last_note = ""

    @property
    def answer(self) -> str:
        """Return the whole public answer, including earlier FLUSH segments."""
        return "".join(self.answer_parts)

    def observe(self, event: MessageEvent) -> None:
        """Reduce actual events without inferring internal success from labels."""
        now = time.monotonic()
        # Transport keepalives / unchanged plans must not look like fresh work.
        if event.type not in (MessageEventType.TYPING, MessageEventType.FLUSH):
            self.last_activity = now
        self.revision += 1
        text = extract_text(event.content)
        if event.type == MessageEventType.DELTA:
            self.answer_parts.append(text)
            self.note_parts.append(text)
            if text:
                self.latest_hint = "".join(self.note_parts)
        elif event.type == MessageEventType.MESSAGE and text:
            if self.answer_parts and not self.answer.endswith("\n\n"):
                self.answer_parts.append("\n\n")
            self.answer_parts.append(text)
            self.last_note = text
            self.note_parts.clear()
            self.latest_hint = text
        elif event.type == MessageEventType.FLUSH:
            if self.note_parts:
                self.last_note = "".join(self.note_parts)
                self.note_parts.clear()
            if self.answer_parts and not self.answer.endswith("\n\n"):
                self.answer_parts.append("\n\n")
        elif event.type in (MessageEventType.THINKING, MessageEventType.THINKING_DELTA):
            if self.show_thinking:
                self.thinking = _clip(self.thinking + text, 2000)
        elif event.type == MessageEventType.TOOL_START and self.show_tools:
            meta = event.metadata
            call_id = str(meta.get("tool_call_id") or meta.get("call_id") or "")
            if call_id and any(t.call_id == call_id for t in self.tools):
                return
            self.total_tools += 1
            self.tools.append(
                _Tool(
                    name=_clip(str(meta.get("display_name") or meta.get("tool_name") or "工具"), 160),
                    call_id=call_id,
                    arguments=_detail(meta["arguments"]) if "arguments" in meta else "",
                    step_generation=self.step_generation,
                )
            )
            self._tool_hint(event, self.tools[-1])
        elif event.type == MessageEventType.TOOL_END and self.show_tools:
            meta = event.metadata
            call_id = str(meta.get("tool_call_id") or meta.get("call_id") or "")
            name = _clip(str(meta.get("display_name") or meta.get("tool_name") or "工具"), 160)
            tool = next(
                (t for t in self.tools if (t.call_id == call_id if call_id else t.ended is None and t.name == name)),
                None,
            )
            if tool is not None and tool.ended is not None:
                return
            if tool is None:
                self.total_tools += 1
                tool = _Tool(name=name, call_id=call_id, step_generation=self.step_generation)
                self.tools.append(tool)
            tool.ended = time.monotonic()
            tool.name = name
            failed = meta.get("status") in ("error", "failed") or bool(meta.get("error"))
            tool.status = "失败" if failed else "已返回"
            for key in ("output", "result", "error"):
                if key in meta:
                    tool.result = _detail(meta[key])
                    break
            self._tool_hint(event, tool)
        elif event.type == MessageEventType.TYPING:
            steps = _business_steps(event.metadata.get("task_progress"))
            if steps is not None and steps != self.steps:
                before = [s.title for s in self.steps if s.status == "in_progress"]
                active = [s.title for s in steps if s.status == "in_progress"]
                if before != active:
                    self.current_step_started = now if active else None
                    self.step_generation += 1
                    self.latest_hint = ""
                    self.note_parts.clear()
                    self.last_note = ""
                self.steps = steps
                self.plan_updated_at = now
                self.last_activity = now
        elif event.type == MessageEventType.ERROR:
            self.status, self.error = "failed", _detail(event.error or "处理失败，请稍后重试。")
        elif event.type == MessageEventType.COMPLETED:
            self.status = "completed"

    def render(self, clean: Callable[[str], str]) -> dict[str, Any]:
        """Build a compact schema-2.0 card with native, callback-free folds."""
        now = time.monotonic()
        elapsed = max(0, int(now - self.started))
        title, color = {
            "running": ("正在处理", "blue"),
            "completed": ("本次响应结束", "green"),
            "failed": ("处理失败", "red"),
            "cancelled": ("已停止", "grey"),
        }[self.status]
        if self.status == "running" and self.goal:
            title = _clip(self.goal, 160)
        unfinished = [s for s in self.steps if s.status != "completed"]
        if self.status == "completed" and unfinished:
            title, color = "本轮回复结束 · 步骤尚未全部完成", "orange"
        elements: list[dict[str, Any]] = []
        hint_shown = False
        for step in self.steps:
            elements.append(_step_row(step, running=self.status == "running", image_key=self.loading_image_key))
            if self.status == "running" and step.status == "in_progress" and self.latest_hint and not hint_shown:
                # One muted event, replaced in place. Escape public/tool text so
                # links, mentions and markup cannot impersonate card controls.
                hint = _detail(" ".join(clean(self.latest_hint).split()))
                hint = re.sub(r"([\\*_\[\]])", r"\\\1", hint)
                elements.append(
                    {
                        **_markdown(f"<font color='grey'>· {hint}</font>"),
                        "element_id": "latest_activity",
                        "text_size": "notation",
                        "margin": "0px 8px 4px 34px",
                    }
                )
                hint_shown = True

        answer = clean(self.answer).strip()
        if self.status == "running":
            note = clean("".join(self.note_parts) or self.last_note).strip()
            if note and not self.steps:
                elements.append(_markdown(_clip(note, 2000)))
            elif not self.steps:
                elements.append(_markdown("正在处理请求，业务步骤尚未同步。"))
        elif self.answer_in_messages:
            elements.append(_markdown("正文较长，已在下方分段发送。"))
        elif answer:
            elements.append(_markdown(_clip(answer, _ANSWER_BYTES)))
        elif self.status == "completed":
            elements.append(_markdown("本次执行结束，但没有返回正文。请检查执行详情。"))
        if self.error:
            elements.append(_markdown(f"**未完成：** {self.error}"))

        # Meaningless name-only tool rows never displace an explicit business
        # plan. One flat fold also avoids duplicated nested-panel headings on
        # clients which fall back to showing all collapsible content.
        visible = self.tools[-_VISIBLE_TOOLS:]
        if self.tools and (not self.steps or any(t.arguments or t.result for t in visible)):
            entries: list[dict[str, Any]] = []
            for tool in visible:
                status = tool.status if tool.ended is not None or self.status == "running" else "未确认返回"
                duration = int((tool.ended or now) - tool.started)
                content = f"**{tool.name}** · {status} · 观察用时 {duration} 秒"
                if tool.arguments:
                    content += f"\n提交：\n{tool.arguments}"
                if tool.result:
                    content += f"\n返回：\n{tool.result}"
                entries.append(_markdown(content))
            if self.total_tools > len(visible):
                entries.append(_markdown(f"仅展示最近 {len(visible)} 次调用，累计 {self.total_tools} 次。"))
            entries.append(_markdown("“已返回”仅表示收到结束事件，不代表业务验收通过。"))
            elements.append(_panel(f"技术执行明细 · {self.total_tools} 次工具调用", entries, "process"))
        if self.thinking:
            elements.append(_panel("思考过程（节选）", [_markdown(_clip(self.thinking, 500))], "thinking"))
        status_line = f"已用 {elapsed} 秒"
        if self.status == "running":
            if not any(step.status == "in_progress" for step in self.steps):
                status_line = f"◌ {status_line}"
            if self.current_step_started is not None:
                duration = max(0, int(now - self.current_step_started))
                status_line += f" · 本步 {duration} 秒"
            age = max(0, int(now - self.last_activity))
            status_line += f" · 最近活动 {age} 秒前"
        # A single small, muted footer avoids competing with the task content.
        elements.append({**_markdown(f"<font color='grey'>{status_line}</font>"), "text_size": "notation"})

        return {
            "schema": "2.0",
            "config": {"update_multi": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "body": {"elements": elements, "vertical_spacing": "4px"},
        }


class FeishuCardReply:
    """Coalesce events independently of slow network updates and close on exit."""

    def __init__(
        self,
        state: FeishuCardState,
        *,
        create: Callable[[dict[str, Any]], Awaitable[str]],
        update: Callable[[str, dict[str, Any]], Awaitable[None]],
        send_text: Callable[[str], Awaitable[None]],
        send_media: Callable[[ContentPart], Awaitable[None]],
        clean: Callable[[str], str],
    ) -> None:
        self.state = state
        self._create, self._update = create, update
        self._send_text, self._send_media = send_text, send_media
        self._clean = clean

    async def run(self, events: AsyncIterator[MessageEvent]) -> bool:
        """Consume a producer once; card failures never restart model work."""
        card_id = ""
        updater: asyncio.Task[None] | None = None
        succeeded = False

        async def refresh() -> None:
            revision, last_update = -1, time.monotonic()
            while True:
                await asyncio.sleep(_UPDATE_INTERVAL)
                now = time.monotonic()
                if self.state.revision == revision and now - last_update < _HEARTBEAT_INTERVAL:
                    continue
                rendered_revision = self.state.revision
                try:
                    async with asyncio.timeout(_DELIVERY_TIMEOUT):
                        await self._update(card_id, self.state.render(self._clean))
                except Exception:
                    logger.warning("Feishu progress update unavailable; defer to final delivery", exc_info=True)
                    return
                revision, last_update = rendered_revision, now

        try:
            try:
                async with asyncio.timeout(_DELIVERY_TIMEOUT):
                    card_id = await self._create(self.state.render(self._clean))
            except Exception:
                logger.warning("Feishu progress card unavailable; use final-text delivery", exc_info=True)
            if card_id:
                updater = asyncio.create_task(refresh(), name="feishu-progress-card")
            async for event in events:
                self.state.observe(event)
                if event.type == MessageEventType.MESSAGE:
                    for media in extract_media(event.content):
                        await self._send_media(media)
                if event.type == MessageEventType.COMPLETED:
                    succeeded = True
                    break
                if event.type == MessageEventType.ERROR:
                    break
            else:
                self.state.status = "failed"
                self.state.error = "执行流提前结束，未收到完成事件。"
        except asyncio.CancelledError:
            self.state.status = "cancelled"
            raise
        except Exception:
            logger.exception("Feishu response processing failed")
            self.state.status, self.state.error = "failed", "处理过程中发生错误，请查看执行详情。"
        finally:
            if updater:
                updater.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await updater
            await close_event_stream(events)
            try:
                await self._finish(card_id)
            except Exception:
                succeeded = False
                logger.exception("Feishu final response delivery failed")
        return succeeded

    async def _final_update(self, card_id: str) -> None:
        async with asyncio.timeout(_DELIVERY_TIMEOUT):
            await self._update(card_id, self.state.render(self._clean))

    async def _send_posts(self, text: str) -> None:
        # Bound each request, not the whole sequence: long answers must not be
        # truncated solely because several successful requests took >10s total.
        for part in text_chunks(text):
            async with asyncio.timeout(_DELIVERY_TIMEOUT):
                await self._send_text(part)

    async def _finish(self, card_id: str) -> None:
        try:
            await self._deliver_final(card_id)
        except Exception:
            if card_id:
                await self._close_compact(card_id, delivered=False)
            raise

    async def _close_compact(self, card_id: str, *, delivered: bool) -> None:
        """End stale live cards without resubmitting content rejected by Feishu."""
        title, color = {
            "completed": ("本次响应结束", "green"),
            "failed": ("处理失败", "red"),
            "cancelled": ("已停止", "grey"),
        }.get(self.state.status, ("本轮回复结束", "grey"))
        note = "回复已在下方发送。"
        if self.state.status == "completed" and any(s.status != "completed" for s in self.state.steps):
            title, color = "本轮回复结束 · 步骤尚未全部完成", "orange"
            note += "业务步骤仍有未确认项。"
        if not delivered:
            title, color = "回复未送达", "orange"
            note = "本轮执行已停止，完整回复发送失败，请检查消息投递。"
        elapsed = max(0, int(time.monotonic() - self.state.started))
        # Only fixed gateway text: no model Markdown, step titles, tool payloads
        # or images which might have caused the original card rejection.
        card = {
            "schema": "2.0",
            "config": {"update_multi": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "body": {"elements": [_markdown(note), _markdown(f"已结束 · 用时 {elapsed} 秒")]},
        }
        try:
            async with asyncio.timeout(_DELIVERY_TIMEOUT):
                await self._update(card_id, card)
        except Exception:
            logger.warning("Feishu compact terminal card update unavailable", exc_info=True)

    async def _deliver_final(self, card_id: str) -> None:
        answer = self._clean(self.state.answer).strip()
        # Long answers remain complete in messages; only the live preview is clipped.
        if (
            card_id
            and self.state.status != "cancelled"
            and len(json.dumps(answer, ensure_ascii=False).encode("utf-8")) > _ANSWER_BYTES
        ):
            # End the live indicator before sending multiple posts. A later
            # post failure must not leave an already-finished turn "running".
            try:
                await self._final_update(card_id)
            except Exception:
                logger.warning("Feishu long-answer card finalization unavailable", exc_info=True)
            await self._send_posts(answer)
            self.state.answer_in_messages = True
        if card_id:
            try:
                await self._final_update(card_id)
                return
            except Exception:
                logger.warning("Feishu final card update unavailable", exc_info=True)
        if self.state.answer_in_messages:
            if card_id:
                await self._close_compact(card_id, delivered=True)
            return
        fallback = answer
        if self.state.status in ("failed", "cancelled"):
            fallback += "\n\n" + (self.state.error or "本次执行已停止。")
        if not fallback.strip():
            fallback = "本次执行结束，但没有返回正文。请检查执行详情。"
        await self._send_posts(fallback)
        self.state.answer_in_messages = True
        if card_id:
            await self._close_compact(card_id, delivered=True)
