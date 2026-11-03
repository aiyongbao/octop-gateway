"""Offline regression tests for the Feishu per-turn progress card."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig
from octop_gateway.channels.feishu_cards import FeishuCardState
from octop_gateway.models import ChannelSubject, InboundMessage, MessageEvent, TextContent


def make_channel(processor):
    channel = FeishuChannel(processor, FeishuConfig(app_id="test", app_secret="test"))
    channel._send_progress_card = AsyncMock(return_value="om_card")
    channel._update_progress_card = AsyncMock()
    channel._send_text = AsyncMock()
    channel._deliver = AsyncMock(return_value={"code": 0})
    channel._get_progress_image_key = AsyncMock(return_value="")
    return channel


def inbound(name="alice"):
    subject = ChannelSubject(subject_id=name)
    return InboundMessage(channel_id="test", content=[TextContent(text="test")], channel_subject=subject), subject


def business_progress(steps):
    from octop_gateway.models import MessageEventType

    return MessageEvent(
        type=MessageEventType.TYPING,
        metadata={"task_progress": {"version": 1, "source": "write_todos", "steps": steps}},
    )


@pytest.mark.asyncio
async def test_one_card_collects_tools_and_streamed_answer():
    async def processor(message):
        yield MessageEvent.delta("Checking. ")
        yield MessageEvent.flush()
        yield MessageEvent.tool_start("execute")
        yield MessageEvent.tool_end("execute")
        yield MessageEvent.delta("Done.")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    assert await channel._process_inbound(*inbound()) is True
    channel._send_progress_card.assert_awaited_once()
    channel._send_text.assert_not_awaited()
    card = channel._update_progress_card.call_args.args[1]
    rendered = json.dumps(card, ensure_ascii=False)
    assert "Checking." in rendered and "Done." in rendered
    assert "execute" in rendered and "已返回" in rendered
    assert "执行成功" not in rendered
    assert '"tag": "collapsible_panel"' in rendered


@pytest.mark.asyncio
async def test_error_is_terminal_and_closes_producer():
    closed = False

    async def processor(message):
        nonlocal closed
        try:
            yield MessageEvent.delta("Partial")
            yield MessageEvent.error_event("Source unavailable")
            pytest.fail("ERROR must stop consuming the producer")
        finally:
            closed = True

    channel = make_channel(processor)
    assert await channel._process_inbound(*inbound()) is False
    assert closed
    final = json.dumps(channel._update_progress_card.call_args.args[1], ensure_ascii=False)
    assert "Source unavailable" in final and "Partial" in final and "失败" in final


@pytest.mark.asyncio
async def test_card_failure_falls_back_once_without_rerunning_model():
    calls = 0

    async def processor(message):
        nonlocal calls
        calls += 1
        for _ in range(8):
            yield MessageEvent.tool_start("read")
            yield MessageEvent.tool_end("read")
        yield MessageEvent.delta("Final answer")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._send_progress_card.side_effect = RuntimeError("denied")
    assert await channel._process_inbound(*inbound()) is True
    assert calls == 1
    channel._deliver.assert_awaited_once()
    assert "Final answer" in channel._deliver.call_args.kwargs["content"]


@pytest.mark.asyncio
async def test_concurrent_turns_keep_separate_card_ids_and_content():
    ready = asyncio.Event()
    entered = 0

    async def processor(message):
        nonlocal entered
        entered += 1
        if entered == 2:
            ready.set()
        await ready.wait()
        yield MessageEvent.delta(message.channel_subject.subject_id)
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._send_progress_card.side_effect = ["om_alice", "om_bob"]
    assert await asyncio.gather(
        channel._process_inbound(*inbound("alice")), channel._process_inbound(*inbound("bob"))
    ) == [True, True]
    cards = {call.args[0]: json.dumps(call.args[1]) for call in channel._update_progress_card.call_args_list}
    assert "alice" in cards["om_alice"] and "bob" not in cards["om_alice"]
    assert "bob" in cards["om_bob"] and "alice" not in cards["om_bob"]


@pytest.mark.asyncio
async def test_card_update_failure_delivers_complete_answer_as_text():
    async def processor(message):
        yield MessageEvent.delta("Answer without loss")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._update_progress_card.side_effect = RuntimeError("unavailable")
    assert await channel._process_inbound(*inbound()) is True
    assert "Answer without loss" in channel._deliver.call_args.kwargs["content"]
    channel._deliver.assert_awaited_once()


@pytest.mark.asyncio
async def test_rejected_final_card_sends_answer_then_closes_original_with_compact_card():
    calls = 0

    async def processor(message):
        nonlocal calls
        calls += 1
        yield business_progress([{"title": "核对数据", "status": "completed"}])
        yield MessageEvent.delta("Report with rejected content")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    updates = []

    async def update(message_id, card):
        text = json.dumps(card, ensure_ascii=False)
        updates.append((message_id, text))
        if "Report with rejected content" in text:
            raise RuntimeError("Feishu progress card update failed (code=230099)")
        channel._deliver.assert_awaited_once()  # Only claim sent after delivery.

    channel._update_progress_card.side_effect = update
    assert await channel._process_inbound(*inbound()) is True
    assert calls == 1 and len(updates) == 2
    assert updates[-1][0] == "om_card" and "回复已在下方发送" in updates[-1][1]
    assert "最近活动" not in updates[-1][1] and "◌" not in updates[-1][1]
    assert "Report with rejected content" not in updates[-1][1]
    assert "本次响应结束" in updates[-1][1]


@pytest.mark.asyncio
async def test_rejected_card_and_failed_post_close_with_delivery_failure_not_sent():
    async def processor(message):
        yield MessageEvent.delta("answer")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._deliver.return_value = {"code": 230001}
    updates = []

    async def update(message_id, card):
        updates.append(card)
        if "answer" in json.dumps(card):
            raise RuntimeError("230099")

    channel._update_progress_card.side_effect = update
    assert await channel._process_inbound(*inbound()) is False
    final = json.dumps(updates[-1], ensure_ascii=False)
    assert "回复未送达" in final and "回复已在下方发送" not in final
    assert "最近活动" not in final and "◌" not in final


@pytest.mark.asyncio
async def test_compact_terminal_fallback_does_not_complete_unverified_steps():
    async def processor(message):
        yield business_progress([{"title": "尚未核验", "status": "in_progress"}])
        yield MessageEvent.delta("answer")
        yield MessageEvent.completed()

    channel = make_channel(processor)

    async def update(message_id, card):
        if "answer" in json.dumps(card):
            raise RuntimeError("230099")

    channel._update_progress_card.side_effect = update
    assert await channel._process_inbound(*inbound()) is True
    final = channel._update_progress_card.call_args.args[1]
    assert final["header"]["template"] == "orange"
    assert "步骤尚未全部完成" in json.dumps(final, ensure_ascii=False)
    assert "回复已在下方发送" in json.dumps(final, ensure_ascii=False)
    assert "answer" not in json.dumps(final)


@pytest.mark.asyncio
async def test_long_unicode_answer_is_delivered_whole_in_bounded_messages():
    answer = "经营分析😀\n" * 4000

    async def processor(message):
        yield MessageEvent.delta(answer)
        yield MessageEvent.completed()

    channel = make_channel(processor)
    assert await channel._process_inbound(*inbound()) is True
    parts = [json.loads(c.kwargs["content"])["zh_cn"]["content"][0][0]["text"] for c in channel._deliver.call_args_list]
    assert "".join(parts) == answer.strip()
    assert all(len(p.encode()) <= 10000 for p in parts)
    assert "正文较长" in json.dumps(channel._update_progress_card.call_args.args[1], ensure_ascii=False)


@pytest.mark.asyncio
async def test_early_eof_is_not_success_and_preserves_partial_answer():
    async def processor(message):
        yield MessageEvent.delta("partial")

    channel = make_channel(processor)
    assert await channel._process_inbound(*inbound()) is False
    card = json.dumps(channel._update_progress_card.call_args.args[1], ensure_ascii=False)
    assert "partial" in card and "未收到完成事件" in card


@pytest.mark.asyncio
async def test_cancellation_finalizes_card_and_closes_producer():
    started, closed = asyncio.Event(), asyncio.Event()

    async def processor(message):
        try:
            yield MessageEvent.tool_start("execute")
            started.set()
            await asyncio.Event().wait()
        finally:
            closed.set()

    channel = make_channel(processor)
    task = asyncio.create_task(channel._process_inbound(*inbound()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    card = json.dumps(channel._update_progress_card.call_args.args[1], ensure_ascii=False)
    assert "已停止" in card and "未确认返回" in card
    assert not any(t.get_name() == "feishu-progress-card" for t in asyncio.all_tasks())


@pytest.mark.asyncio
async def test_tools_and_thinking_preferences_are_respected():
    async def processor(message):
        yield MessageEvent.thinking_delta("private test reasoning")
        yield MessageEvent.tool_start("hidden-tool")
        yield MessageEvent.tool_end("hidden-tool")
        yield MessageEvent.delta("public answer")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel.constraints.show_tool_hints = False
    assert await channel._process_inbound(*inbound()) is True
    card = json.dumps(channel._update_progress_card.call_args.args[1])
    assert "hidden-tool" not in card and "private test reasoning" not in card
    assert "public answer" in card


def test_card_size_is_bounded_with_many_tools_and_json_escape_expansion():
    state = FeishuCardState(show_thinking=True)
    for index in range(40):
        state.observe(MessageEvent.tool_start("\x00" * 100, call_id=str(index), arguments={"data": "\x00" * 900}))
    for index in range(40):
        state.observe(MessageEvent.tool_end("\x00" * 100, call_id=str(index), output="\x00" * 900))
    state.observe(MessageEvent.thinking("\x00" * 9000))
    state.observe(MessageEvent.delta("\x00" * 20000))
    card = state.render(lambda x: x)
    assert len(json.dumps(card, ensure_ascii=False).encode()) < 28000
    assert state.total_tools == 40
    assert all(t.status == "已返回" for t in state.tools)


def test_optional_tool_details_are_folded_and_credential_fields_redacted():
    state = FeishuCardState()
    state.observe(MessageEvent.tool_start("query", arguments={"sql": "SELECT 1", "api_key": "never-show-me"}))
    state.observe(
        MessageEvent.tool_end("query", status="error", output={"authorization": "never-show-me", "error": "bad SQL"})
    )
    text = json.dumps(state.render(lambda x: x), ensure_ascii=False)
    assert "SELECT 1" in text and "bad SQL" in text
    assert "never-show-me" not in text and "[REDACTED]" in text and "失败" in text


@pytest.mark.asyncio
async def test_media_stays_a_native_attachment_not_a_fake_card_delivery_receipt():
    from octop_gateway.models import FileContent

    async def processor(message):
        yield MessageEvent(content=[FileContent(filename="report.xlsx", local_path="fake")])
        yield MessageEvent.text("Report ready")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._send_media = AsyncMock()
    assert await channel._process_inbound(*inbound()) is True
    channel._send_media.assert_awaited_once()
    final = json.dumps(channel._update_progress_card.call_args.args[1])
    assert "Report ready" in final and "file_key" not in final


@pytest.mark.asyncio
async def test_original_topic_routing_is_frozen_for_later_output():
    async def processor(message):
        message.channel_subject.metadata["message_id"] = "om_next_turn"
        yield MessageEvent.delta("final")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._send_progress_card.side_effect = RuntimeError("unsupported")
    message, subject = inbound()
    subject.metadata = {"message_id": "om_this_turn", "thread_id": "topic"}
    assert await channel._process_inbound(message, subject) is True
    assert channel._deliver.call_args.args[0].metadata["message_id"] == "om_this_turn"


@pytest.mark.asyncio
async def test_legacy_delivery_can_be_selected_explicitly():
    async def processor(message):
        yield MessageEvent.text("legacy")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._config.streaming_cards = False
    assert await channel._process_inbound(*inbound()) is True
    channel._send_progress_card.assert_not_awaited()
    channel._send_text.assert_awaited_once()


@pytest.mark.asyncio
async def test_card_create_returns_platform_message_id_and_keeps_topic_reply():
    async def processor(message):
        yield MessageEvent.completed()

    channel = FeishuChannel(processor, FeishuConfig(app_id="test", app_secret="test"))
    channel._reply_message = AsyncMock(return_value={"code": 0, "data": {"message_id": "om_created"}})
    channel._send_message = AsyncMock()
    subject = ChannelSubject(subject_id="oc_group", metadata={"thread_id": "topic", "message_id": "om_parent"})
    assert await channel._send_progress_card(subject, {"schema": "2.0"}) == "om_created"
    assert channel._reply_message.call_args.args[0] == "om_parent"
    channel._send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_card_update_uses_patch_and_validates_internal_error_code():
    channel = make_channel(lambda message: None)
    response = AsyncMock()
    response.json.return_value = {"code": 230001}
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=False)
    http = MagicMock()
    http.patch.return_value = context
    channel._ensure_http = AsyncMock(return_value=http)
    channel._get_auth_headers = AsyncMock(return_value={})
    with pytest.raises(RuntimeError, match="230001"):
        await FeishuChannel._update_progress_card(channel, "om_id", {"schema": "2.0"})
    assert http.patch.call_args.args[0].endswith("/im/v1/messages/om_id")
    assert json.loads(http.patch.call_args.kwargs["json"]["content"])["schema"] == "2.0"


@pytest.mark.asyncio
async def test_slow_live_patch_does_not_block_tokens_or_hide_new_revisions(monkeypatch):
    import octop_gateway.channels.feishu_cards as cards

    monkeypatch.setattr(cards, "_UPDATE_INTERVAL", 0.001)
    monkeypatch.setattr(cards, "_HEARTBEAT_INTERVAL", 100)
    patch_started, second_token_seen, second_patch_seen = asyncio.Event(), asyncio.Event(), asyncio.Event()
    patches = []

    async def update(message_id, card):
        patches.append(json.dumps(card))
        if len(patches) == 1:
            patch_started.set()
            await second_token_seen.wait()
        elif "second token" in patches[-1]:
            second_patch_seen.set()

    async def processor(message):
        yield MessageEvent.delta("first token ")
        await patch_started.wait()
        yield MessageEvent.delta("second token")
        second_token_seen.set()
        await second_patch_seen.wait()
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._update_progress_card.side_effect = update
    assert await asyncio.wait_for(channel._process_inbound(*inbound()), timeout=1) is True
    assert len(patches) >= 3
    assert "second token" in patches[-1]


@pytest.mark.asyncio
async def test_heartbeat_uses_observed_activity_and_stops_on_completion(monkeypatch):
    import octop_gateway.channels.feishu_cards as cards

    monkeypatch.setattr(cards, "_UPDATE_INTERVAL", 0.001)
    monkeypatch.setattr(cards, "_HEARTBEAT_INTERVAL", 0.001)
    heartbeats, finish = [], asyncio.Event()

    async def update(message_id, card):
        heartbeats.append(json.dumps(card, ensure_ascii=False))
        if len(heartbeats) == 2:
            finish.set()

    async def processor(message):
        yield MessageEvent.tool_start("long tool")
        await finish.wait()
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._update_progress_card.side_effect = update
    assert await asyncio.wait_for(channel._process_inbound(*inbound()), timeout=1) is True
    assert len(heartbeats) == 3
    assert "最近活动" in heartbeats[0]
    assert "最近活动" not in heartbeats[-1]


@pytest.mark.asyncio
async def test_failed_final_text_delivery_does_not_mark_processing_success():
    async def processor(message):
        yield MessageEvent.delta("answer")
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._send_progress_card.side_effect = RuntimeError("unavailable")
    channel._deliver.return_value = {"code": 230001}
    assert await channel._process_inbound(*inbound()) is False


@pytest.mark.asyncio
async def test_long_post_failure_does_not_leave_card_running_or_claim_sent():
    async def processor(message):
        yield MessageEvent.delta("text " * 4000)
        yield MessageEvent.completed()

    channel = make_channel(processor)
    channel._deliver.return_value = {"code": 230001}
    assert await channel._process_inbound(*inbound()) is False
    card = channel._update_progress_card.call_args.args[1]
    assert card["header"]["title"]["content"] == "回复未送达"
    assert "已在下方分段发送" not in json.dumps(card, ensure_ascii=False)


@pytest.mark.asyncio
async def test_multiple_successful_posts_do_not_share_one_total_timeout(monkeypatch):
    import octop_gateway.channels.feishu_cards as cards

    monkeypatch.setattr(cards, "_DELIVERY_TIMEOUT", 0.05)

    async def processor(message):
        yield MessageEvent.delta("x" * 30000)
        yield MessageEvent.completed()

    async def slow_post(*args, **kwargs):
        await asyncio.sleep(0.025)
        return {"code": 0}

    channel = make_channel(processor)
    channel._deliver.side_effect = slow_post
    assert await channel._process_inbound(*inbound()) is True
    assert channel._deliver.await_count == 3


def test_explicit_business_steps_are_primary_without_generic_tool_noise():
    state = FeishuCardState(goal="分别复核三平台投流")
    state.observe(MessageEvent.tool_start("execute", call_id="e1"))
    state.observe(MessageEvent.tool_end("execute", call_id="e1"))
    state.observe(
        business_progress(
            [
                {"title": "确认分析范围", "status": "completed"},
                {"title": "核验 ADQ 来源和日期覆盖", "status": "in_progress"},
                {"title": "逐平台分析并交付", "status": "pending"},
            ]
        )
    )
    card = state.render(lambda x: x)
    rendered = json.dumps(card, ensure_ascii=False)
    assert card["header"]["title"]["content"] == "分别复核三平台投流"
    assert rendered.count("核验 ADQ 来源和日期覆盖") == 1
    assert "进行中" in rendered and "已完成" in rendered and "待开始" in rendered
    assert "execute" not in rendered and "%" not in rendered


def test_tool_events_never_complete_business_steps_and_no_business_percentage():
    state = FeishuCardState()
    state.observe(business_progress([{"title": "核验退款口径", "status": "in_progress"}]))
    for index in range(20):
        state.observe(MessageEvent.tool_start("execute", call_id=str(index)))
        state.observe(MessageEvent.tool_end("execute", call_id=str(index)))
    assert state.steps[0].status == "in_progress"
    state.observe(MessageEvent.completed())
    card = state.render(lambda x: x)
    assert card["header"]["template"] == "orange"
    assert "本轮未完成" in json.dumps(card, ensure_ascii=False)


def test_invalid_or_replayed_plan_does_not_reset_business_activity_clock():
    state = FeishuCardState()
    event = business_progress([{"title": "核对订单", "status": "in_progress"}])
    state.observe(event)
    timestamp = state.plan_updated_at
    state.observe(event)
    assert state.plan_updated_at == timestamp
    state.observe(business_progress([{"title": "错误进度", "status": "imagined"}]))
    assert state.steps[0].title == "核对订单"


def test_latest_business_note_is_public_text_and_preserves_final_full_answer():
    state = FeishuCardState(show_thinking=False)
    state.observe(MessageEvent.thinking("not a public task update"))
    state.observe(MessageEvent.delta("已核对千川，准备读取 ADQ。"))
    state.observe(MessageEvent.flush())
    state.observe(MessageEvent.delta("ADQ 来源仍待核验，暂未给出判断。"))
    running = json.dumps(state.render(lambda x: x), ensure_ascii=False)
    assert "ADQ 来源仍待核验" in running and "not a public task update" not in running
    state.observe(MessageEvent.completed())
    final = json.dumps(state.render(lambda x: x), ensure_ascii=False)
    assert "已核对千川" in final and "ADQ 来源仍待核验" in final


def test_business_plan_is_independent_of_tool_visibility_preference():
    state = FeishuCardState(show_tools=False)
    state.observe(business_progress([{"title": "校验报告", "status": "in_progress"}]))
    assert "校验报告" in json.dumps(state.render(lambda x: x), ensure_ascii=False)


def test_call_identity_deduplicates_replay_and_corrects_final_tool_name():
    state = FeishuCardState()
    state.observe(MessageEvent.tool_start("exec", call_id="1"))
    state.observe(MessageEvent.tool_start("execute", call_id="1"))
    state.observe(MessageEvent.tool_end("execute", call_id="1"))
    state.observe(MessageEvent.tool_end("execute", call_id="1"))
    assert state.total_tools == 1 and state.tools[0].name == "execute"


def test_maximum_plan_and_details_stay_within_card_byte_limit():
    state = FeishuCardState(show_thinking=True)
    state.observe(business_progress([{"title": "\x00" * 500, "status": "in_progress"} for _ in range(12)]))
    for index in range(20):
        state.observe(MessageEvent.tool_start("\x00" * 100, call_id=str(index), arguments="\x00" * 1000))
        state.observe(MessageEvent.tool_end("\x00" * 100, call_id=str(index), output="\x00" * 1000))
    state.observe(MessageEvent.thinking("\x00" * 1000))
    state.observe(MessageEvent.delta("\x00" * 30000))
    state.observe(MessageEvent.completed())
    assert len(json.dumps(state.render(lambda x: x), ensure_ascii=False).encode()) < 28000


def test_wait_indicator_does_not_invent_business_progress(monkeypatch):
    import octop_gateway.channels.feishu_cards as cards

    clock = [100.0]
    monkeypatch.setattr(cards.time, "monotonic", lambda: clock[0])
    state = FeishuCardState(started=clock[0], last_activity=clock[0], loading_image_key="img_spinner")
    event = business_progress([{"title": "核对数据来源", "status": "in_progress"}])
    state.observe(event)
    first = json.dumps(state.render(lambda x: x), ensure_ascii=False)
    observed_at = state.last_activity
    clock[0] = 103.0
    state.observe(event)  # Repeating a plan is not a new execution event.
    second = json.dumps(state.render(lambda x: x), ensure_ascii=False)
    assert "img_spinner" in first and "img_spinner" in second
    assert state.last_activity == observed_at
    assert "本步 3 秒" in second and "最近活动 3 秒前" in second
    assert state.steps[0].status == "in_progress" and "已完成" not in second
    assert "%" not in second


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
def test_terminal_cards_remove_wait_animation(status):
    state = FeishuCardState(status=status)
    text = json.dumps(state.render(lambda x: x), ensure_ascii=False)
    assert "等待下一次更新" not in text
    assert all(frame not in text for frame in ("◌", "◉", "●"))


def test_current_step_clock_only_restarts_when_the_active_step_changes(monkeypatch):
    import octop_gateway.channels.feishu_cards as cards

    clock = [100.0]
    monkeypatch.setattr(cards.time, "monotonic", lambda: clock[0])
    state = FeishuCardState(started=clock[0], last_activity=clock[0])
    state.observe(business_progress([{"title": "核对来源", "status": "in_progress"}]))
    clock[0] = 110.0
    state.observe(
        business_progress([{"title": "核对来源", "status": "in_progress"}, {"title": "交付", "status": "pending"}])
    )
    assert "本步 10 秒" in json.dumps(state.render(lambda x: x), ensure_ascii=False)
    clock[0] = 115.0
    state.observe(
        business_progress([{"title": "核对来源", "status": "completed"}, {"title": "交付", "status": "in_progress"}])
    )
    assert "本步 0 秒" in json.dumps(state.render(lambda x: x), ensure_ascii=False)


def test_business_body_uses_compact_colored_rows_and_one_secondary_footer():
    state = FeishuCardState(goal="标题保持原样")
    state.observe(
        business_progress(
            [
                {"title": "核对来源", "status": "completed"},
                {"title": "分析投放差异", "status": "in_progress"},
                {"title": "交付结果", "status": "pending"},
            ]
        )
    )
    card = state.render(lambda x: x)
    body = card["body"]
    text = json.dumps(body, ensure_ascii=False)
    assert card["header"]["title"]["content"] == "标题保持原样"
    assert all(text.count(title) == 1 for title in ("核对来源", "分析投放差异", "交付结果"))
    assert "接下来" not in text and "等待下一次更新" not in text
    rows = [element for element in body["elements"] if element["tag"] == "column_set"]
    assert len(rows) == 3 and rows[1]["background_style"] == "blue-50"
    assert "color='blue'" in json.dumps(rows[1], ensure_ascii=False)
    assert "color='green'" in json.dumps(rows[0], ensure_ascii=False)
    assert "color='grey'" in json.dumps(rows[2], ensure_ascii=False)
    footer = body["elements"][-1]
    assert footer["text_size"] == "notation" and "color='grey'" in footer["content"]
    assert "\n" not in footer["content"]


def latest_hint(state):
    return [el for el in state.render(lambda x: x)["body"]["elements"] if el.get("element_id") == "latest_activity"]


def test_only_latest_activity_is_under_current_step_and_replaced_by_public_finding():
    state = FeishuCardState()
    state.observe(
        business_progress([{"title": "核对商品", "status": "in_progress"}, {"title": "交付", "status": "pending"}])
    )
    state.observe(MessageEvent.tool_start("读取文件", tool_key="read_file", call_id="read"))
    assert "正在读取文件" in latest_hint(state)[0]["content"]
    state.observe(MessageEvent.tool_end("读取文件", tool_key="read_file", call_id="read"))
    assert "已返回" in latest_hint(state)[0]["content"]
    state.observe(MessageEvent.tool_start("执行指令", tool_key="execute", call_id="exec"))
    hint = latest_hint(state)
    assert len(hint) == 1 and "正在执行指令" in hint[0]["content"] and "读取文件" not in hint[0]["content"]
    state.observe(MessageEvent.delta("已找到商品汇总，正在核对销量合计。"))
    hint = latest_hint(state)
    assert len(hint) == 1 and "已找到商品汇总" in hint[0]["content"] and "正在执行指令" not in hint[0]["content"]
    body = state.render(lambda x: x)["body"]["elements"]
    assert body.index(hint[0]) == 1  # Before the next pending business step.
    assert hint[0]["text_size"] == "notation" and "color='grey'" in hint[0]["content"]
    state.observe(MessageEvent.completed())
    assert not latest_hint(state)
    assert "已找到商品汇总" in state.answer


def test_step_switch_drops_stale_hint_but_preserves_whole_answer():
    state = FeishuCardState()
    state.observe(business_progress([{"title": "读取", "status": "in_progress"}]))
    state.observe(MessageEvent.delta("已读到昨日订单。"))
    state.observe(MessageEvent.tool_start("旧查询", call_id="old"))
    state.observe(
        business_progress([{"title": "读取", "status": "completed"}, {"title": "核验", "status": "in_progress"}])
    )
    assert not latest_hint(state)
    state.observe(MessageEvent.tool_end("旧查询", call_id="old"))
    assert not latest_hint(state)  # A late result belongs to the old step.
    state.observe(MessageEvent.delta("正在核验退款覆盖。"))
    assert "昨日订单" not in latest_hint(state)[0]["content"]
    assert "已读到昨日订单" in state.answer


def test_activity_labels_do_not_leak_payloads_or_claim_code_writing_from_execute():
    state = FeishuCardState()
    state.observe(business_progress([{"title": "核验", "status": "in_progress"}]))
    state.observe(
        MessageEvent.tool_start("执行指令", tool_key="execute", call_id="x", arguments={"token": "secret-value"})
    )
    text = latest_hint(state)[0]["content"]
    assert "secret-value" not in text and "代码" not in text
    state.observe(MessageEvent.tool_end("执行指令", tool_key="execute", call_id="x", status="error"))
    assert "工具报错" in latest_hint(state)[0]["content"]
    assert state.steps[0].status == "in_progress"
    state.observe(MessageEvent.tool_start("写入文件", tool_key="write_file", call_id="write"))
    assert "正在写入文件" in latest_hint(state)[0]["content"]
    state.observe(MessageEvent.thinking("private reasoning"))
    assert "private reasoning" not in latest_hint(state)[0]["content"]


def test_hidden_tools_and_plan_tools_do_not_replace_public_finding():
    state = FeishuCardState(show_tools=False)
    state.observe(business_progress([{"title": "核验", "status": "in_progress"}]))
    state.observe(MessageEvent.delta("正在核对商品合计。"))
    state.observe(MessageEvent.tool_start("execute"))
    assert "正在核对商品合计" in latest_hint(state)[0]["content"]
    state.show_tools = True
    state.observe(MessageEvent.tool_start("更新计划", tool_key="write_todos", call_id="plan"))
    state.observe(MessageEvent.tool_end("更新计划", tool_key="write_todos", call_id="plan"))
    assert "正在核对商品合计" in latest_hint(state)[0]["content"]


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
def test_client_spinner_is_removed_at_terminal_state(status):
    state = FeishuCardState(loading_image_key="img_spinner")
    state.observe(business_progress([{"title": "核验", "status": "in_progress"}]))
    assert "img_spinner" in json.dumps(state.render(lambda x: x))
    state.status = status
    assert "img_spinner" not in json.dumps(state.render(lambda x: x))


@pytest.mark.asyncio
async def test_progress_gif_is_uploaded_once_for_concurrent_turns():
    async def processor(message):
        yield MessageEvent.completed()

    channel = FeishuChannel(processor, FeishuConfig(app_id="test", app_secret="test"))
    channel._upload_image_to_feishu = AsyncMock(return_value="img_spinner")
    assert (
        await asyncio.gather(channel._get_progress_image_key(), channel._get_progress_image_key())
        == ["img_spinner"] * 2
    )
    channel._upload_image_to_feishu.assert_awaited_once()
    media = channel._upload_image_to_feishu.call_args.args[0]
    assert media.mime_type == "image/gif"
    import base64

    data = base64.b64decode(media.data)
    assert data.startswith(b"GIF89a") and b"NETSCAPE2.0" in data


@pytest.mark.asyncio
async def test_progress_gif_upload_failure_is_cached_without_breaking_reply():
    async def processor(message):
        yield MessageEvent.completed()

    channel = FeishuChannel(processor, FeishuConfig(app_id="test", app_secret="test"))
    channel._upload_image_to_feishu = AsyncMock(side_effect=RuntimeError("resource unavailable"))
    assert await channel._get_progress_image_key() == ""
    assert await channel._get_progress_image_key() == ""
    channel._upload_image_to_feishu.assert_awaited_once()
