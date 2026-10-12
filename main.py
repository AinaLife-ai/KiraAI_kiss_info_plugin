# -*- coding: utf-8 -*-
"""
KISS 官方情报插件（kiss_info_plugin）v1.0.1

- 抓取 KISS 官网，获取 COM3D2/CM3D2/CRC3D/CR EditSystem 最新版本、公告、活动、DLC
- 轻量 HTTP 抓取 + 正则解析，只提取关键字段，不把整页 HTML 丢给 LLM
- 本地缓存增量比对：命中缓存零 token 秒回，无变化不重复抓取
- 两级深度：brief=只版本号/标题，full=含链接等更多细节
- 入口：LLM 工具 kiss_info_query（默认开）+ /kiss 命令（默认关）

v1.0.1 变更（兼容 + 稳定性，功能语义不变）
  1. 修正 3.0/2.x 下两个不存在的 API 调用：`ctx.send_to` → `ctx.send_message_chain`、
     `event.reply` → `ctx.send_message_chain(event.session.sid, …)`
     （此前自动推送推不出去、命令一条回复都发不出来，异常被吞）
  2. 修正 `event.user_id` 取不到发送者导致命令主人校验恒为假；改用 `event.message.sender.user_id`
  3. 抓取/缓存 I/O 全部移出事件循环（asyncio.to_thread），多源并发抓取
     （原先 6 个源串行阻塞，最坏可卡住事件循环 ~100s）
  4. 缓存改为**结构化数据**（零 token 秒回且内容保真）；老格式自动升级
     （旧格式命中时只回一行拼接串，版本-only 源还会显示 `date=None`）
  5. 自动检查任务纳入生命周期（terminate 取消、禁用即停、配置改动即时生效）；
     抓取失败只在日志与工具输出里体现，不再每轮推送给群
  6. 推送/开关命令真正落到插件配置文件（此前写进了没人读的 kiss_info_config.json）
  7. depth=full 现在真的更详细（更多条目 + 原文链接）；schema/manifest 补全
"""
from __future__ import annotations

import asyncio
import json
import re
import time
import urllib.request
from pathlib import Path
from typing import List, Optional

from core.plugin import BasePlugin, logger, on, Priority, register
from core.chat import MessageChain
from core.chat.message_elements import Text
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.utils.path_utils import get_config_path

PLUGIN_ID = "kiss_info_plugin"
OWNER_ID = "769690776"          # 紫小贱：命令里的主人校验保留原设计
CACHE_VERSION = 2               # 结构化缓存；旧版本字符串缓存自动升级

# ============ 数据源定义 ============
SOURCES = {
    "cm3d2": {"name": "CM3D2", "url": "https://www.kisskiss.tv/cm3d2/update.php", "type": "update"},
    "com3d2": {"name": "COM3D2&2.5", "url": "https://com3d2.jp/update/", "type": "update"},
    "crc3d": {"name": "CRC3D", "url": "https://crc3d3.jp/", "type": "news"},
    "cres": {"name": "CR EditSystem", "url": "https://cr-edit.jp/update", "type": "update"},
    "news": {"name": "KISS公告", "url": "https://www.kisskiss.tv/kiss/diary.php", "type": "news"},
    "dlc_shop": {"name": "DLC商店", "url": "https://www.kisskiss.tv/kiss/diary.php?category=DLC", "type": "news"},
}

DEFAULT_COMMANDS = ["/kiss"]
MAX_ITEMS_BRIEF = 5
MAX_ITEMS_FULL = 12


def _decode(raw: bytes, charset: Optional[str]) -> str:
    """按页面声明的编码解码，失败再退 utf-8 / cp932（避免 Shift-JIS 页面变乱码）。"""
    for enc in (charset, "utf-8", "cp932"):
        if not enc:
            continue
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def _fetch(url: str, timeout: int = 5) -> Optional[str]:
    """轻量抓取页面，失败重试 2 次（同步函数，调用方一律走线程池）。"""
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 (KISS-Info-Bot)"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                try:
                    charset = resp.headers.get_content_charset()
                except Exception:
                    charset = None
                return _decode(resp.read(), charset)
        except Exception:
            if attempt == 2:
                return None
            time.sleep(1)
    return None


def _parse_update(html: Optional[str]) -> dict:
    """解析更新页，提取版本号和日期。"""
    if not html:
        return {}
    versions = re.findall(r"Ver\.?\s*(\d+\.\d+(?:\.\d+)?)", html, re.IGNORECASE)
    dates = re.findall(r"(\d{4})[/年.](\d{1,2})[/月.](\d{1,2})", html)
    latest = versions[0] if versions else None
    latest_date = None
    if dates:
        y, m, d = dates[0]
        latest_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return {"version": latest, "date": latest_date}


def _parse_news(html: Optional[str]) -> List[dict]:
    """解析公告/新闻页，提取标题、日期与链接。"""
    if not html:
        return []
    items = []
    for m in re.finditer(
        r'<table class="blog_frame_top">.*?<th[^>]*>([^<]+)</th>.*?<td[^>]*>.*?<a[^>]*href="([^"]*)"[^>]*>([^<]+)</a>',
        html, re.S | re.I,
    ):
        date_raw, href, title = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        dm = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_raw)
        if dm:
            y, mo, d = dm.groups()
            date = f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
        else:
            date = date_raw
        items.append({"date": date, "title": title, "link": href})
    if items:
        return items
    # 兜底：老结构（无 href 也能出标题）
    for m in re.finditer(
        r'<table class="blog_frame_top">.*?<th[^>]*>([^<]+)</th>.*?<td[^>]*>.*?<a[^>]*>([^<]+)</a>',
        html, re.S | re.I,
    ):
        date_raw, title = m.group(1).strip(), m.group(2).strip()
        dm = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_raw)
        date = f"{dm.group(1)}-{dm.group(2).zfill(2)}-{dm.group(3).zfill(2)}" if dm else date_raw
        items.append({"date": date, "title": title})
    return items


def _parse_crc3d(html: Optional[str]) -> List[dict]:
    """解析 CRC3D 官网 NEWS 板块。"""
    if not html:
        return []
    items = []
    pattern = re.findall(
        r"(\d{4})[年/.](\d{1,2})[月/.](\d{1,2})[日]?\s*([^<\n]{5,80})", html
    )
    for y, m, d, title in pattern[:10]:
        items.append({"date": f"{y}-{m.zfill(2)}-{d.zfill(2)}", "title": title.strip()})
    return items


def _fetch_source(key: str, timeout: int) -> dict:
    """抓取并解析单个数据源（同步，调用方走线程池）。"""
    src = SOURCES[key]
    html = _fetch(src["url"], timeout)
    if html is None:
        return {"source": key, "name": src["name"], "error": "抓取失败"}
    if src["type"] == "update":
        return {"source": key, "name": src["name"], **_parse_update(html)}
    if key == "crc3d":
        return {"source": key, "name": src["name"], "items": _parse_crc3d(html)}
    return {"source": key, "name": src["name"], "items": _parse_news(html)}


def _fingerprint(data: dict) -> str:
    """计算数据指纹用于增量比对（版本源按版本+日期，新闻源按前若干条）。"""
    if "version" in data:
        return f"{data.get('version')}|{data.get('date') or ''}"
    items = data.get("items", [])
    return "|".join(f"{i.get('date', '')}{i.get('title', '')}" for i in items[:5])


def _format(data: dict, depth: str = "brief") -> str:
    """把结构化结果格式化成可读文本。brief 只给版本号/标题；full 给更多条目与链接。"""
    full = depth == "full"
    if "error" in data:
        return f"[{data.get('name')}] {data['error']}"
    if "version" in data:
        if not data.get("version"):
            return f"[{data.get('name')}] 暂无版本信息"
        line = f"[{data.get('name')}] 最新版本 {data['version']}"
        if data.get("date"):
            line += f"（{data['date']}更新）"
        return line
    if "items" in data:
        items = data.get("items") or []
        if not items:
            return f"[{data.get('name')}] 暂无动态"
        limit = MAX_ITEMS_FULL if full else MAX_ITEMS_BRIEF
        lines = [f"[{data.get('name')}] 最新动态（{len(items)} 条）："]
        for it in items[:limit]:
            date = it.get("date") or ""
            title = it.get("title") or ""
            line = f"- {date} {title}".rstrip()
            if full and it.get("link"):
                line += f"\n  {it['link']}"
            lines.append(line)
        if len(items) > limit:
            lines.append(f"…还有 {len(items) - limit} 条，想看更多可以直接翻官网")
        return "\n".join(lines)
    return f"[{data.get('name')}] 无数据"


class KissInfoPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        sec = cfg.get("section_main", {})
        self.enabled = bool(sec.get("enabled", True))
        self.enable_llm_tool = bool(sec.get("enable_llm_tool", True))
        self.enable_command = bool(sec.get("enable_command", False))
        self.content_depth = str(sec.get("content_depth", "brief"))
        self.auto_check = bool(sec.get("auto_check", False))
        self.check_interval = int(sec.get("check_interval", 86400))
        self.push_targets = self._as_list(sec.get("push_targets", []))
        srcs = cfg.get("section_sources", {})
        self.source_keys = [k for k in SOURCES if bool(srcs.get(f"src_{k}", True))]
        self.commands = self._as_list(sec.get("commands", DEFAULT_COMMANDS)) or list(DEFAULT_COMMANDS)
        self.cache_path: Optional[Path] = None
        self._auto_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()          # 串行化抓取，避免工具与自动检查同时开抓

    @staticmethod
    def _as_list(value) -> List[str]:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value or "").splitlines() if v.strip()]

    # ==================== 生命周期 ====================

    async def initialize(self):
        self.cache_path = self.ctx.get_plugin_data_dir() / "kiss_info_cache.json"
        await asyncio.to_thread(self.cache_path.parent.mkdir, parents=True, exist_ok=True)
        logger.info(
            f"[kiss-info] KISS 官方情报插件加载完成 v1.0.1 "
            f"(llm_tool={self.enable_llm_tool} cmd={self.enable_command} "
            f"auto_check={self.auto_check} depth={self.content_depth} "
            f"sources={self.source_keys} push={self.push_targets})"
        )
        self._sync_auto_task()

    async def terminate(self):
        """取消后台任务（可重入：热重载会再次调用）。"""
        await self._stop_auto_task()

    # ==================== 自动检查任务 ====================

    def _sync_auto_task(self):
        """按当前配置（启用 + 自动检查 + 有推送目标）启停后台任务。"""
        should_run = bool(self.auto_check and self.push_targets)
        running = self._auto_task is not None and not self._auto_task.done()
        if should_run and not running:
            self._auto_task = asyncio.ensure_future(self._auto_check_loop())
        elif not should_run and running:
            self._auto_task.cancel()
            self._auto_task = None

    async def _stop_auto_task(self):
        task, self._auto_task = self._auto_task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

    async def _auto_check_loop(self):
        """后台自动检查，有更新时推送到配置目标。"""
        while True:
            try:
                if self.enabled:
                    changes, errors = await self._check_all(force=False)
                    if changes:
                        msgs = [_format(d, self.content_depth) for d in changes]
                        await self._push("\n".join(msgs))
                    if errors:
                        logger.warning(
                            "[kiss-info] 本轮 %d 个源抓取失败：%s",
                            len(errors), "、".join(e.get("name") or e.get("source", "?") for e in errors),
                        )
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("[kiss-info] 自动检查异常")
            try:
                await asyncio.sleep(max(int(self.check_interval), 60))
            except asyncio.CancelledError:
                return

    async def _push(self, text: str):
        for target in list(self.push_targets):
            try:
                await self.ctx.send_message_chain(target, MessageChain([Text(text)]))
            except Exception as e:
                logger.warning(f"[kiss-info] 推送到 {target} 失败: {e}")

    # ==================== 缓存 ====================

    def _load_cache(self) -> dict:
        if self.cache_path and self.cache_path.exists():
            try:
                data = json.loads(self.cache_path.read_text(encoding="utf-8"))
                return data if isinstance(data, dict) else {}
            except Exception:
                return {}
        return {}

    def _save_cache(self, cache: dict):
        if self.cache_path:
            self.cache_path.write_text(
                json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    # ==================== 抓取逻辑 ====================

    async def _check_all(self, force: bool = False):
        """增量检查所有启用数据源，返回 (有变化的结果, 抓取失败的结果)。"""
        async with self._lock:
            cache = await asyncio.to_thread(self._load_cache)
            keys = list(self.source_keys)
            fetched = await asyncio.gather(
                *[asyncio.to_thread(_fetch_source, k, 5) for k in keys]
            ) if keys else []
            changes, errors = [], []
            for key, data in zip(keys, fetched):
                if "error" in data:
                    errors.append(data)
                    continue
                data = dict(data)
                data["_fp"] = _fingerprint(data)
                if force or cache.get(key, {}).get("fp") != data["_fp"]:
                    changes.append(data)
                cache[key] = {"fp": data["_fp"], "at": int(time.time()), "data": data}
            cache["_v"] = CACHE_VERSION
            await asyncio.to_thread(self._save_cache, cache)
            return changes, errors

    async def _check_cached(self) -> List[dict]:
        """命中缓存直接返回（零网络、零 token）；无缓存则先抓一次。"""
        cache = await asyncio.to_thread(self._load_cache)
        results = []
        for key in self.source_keys:
            entry = cache.get(key)
            if isinstance(entry, dict) and isinstance(entry.get("data"), dict):
                data = dict(entry["data"])
                data.pop("_fp", None)
                results.append(data)
        if results:
            return results
        changes, errors = await self._check_all(force=True)
        return changes or errors

    async def _fetch_month(self, yyyymm: str) -> List[dict]:
        """抓取指定月份（YYYYMM）的 KISS 博客归档，返回该月公告列表。"""
        url = f"https://www.kisskiss.tv/kiss/diary.php?date={yyyymm}"
        html = await asyncio.to_thread(_fetch, url, 8)
        if not html:
            return []
        return _parse_news(html)

    # ==================== LLM 工具 ====================

    @register.tool(
        name="kiss_info_query",
        description=(
            "查询 KISS 官方情报：COM3D2/CM3D2/CRC3D/CR EditSystem 的最新版本号、更新日期、"
            "官方公告、活动、DLC 情况。命中缓存直接返回（零 token 秒回），想看最新可传 "
            "force=true 强制重新抓取。返回结构化文本，你需要把日文标题翻译成中文展示给用户。"
        ),
        params={
            "type": "object",
            "properties": {
                "force": {
                    "type": "boolean",
                    "description": "是否强制重新抓取最新信息（默认 false=用缓存）",
                    "default": False,
                },
                "depth": {
                    "type": "string",
                    "enum": ["brief", "full"],
                    "description": "内容深度：brief=只版本号/标题，full=含更多条目与原文链接",
                },
            },
        },
    )
    async def tool_query(self, event: KiraMessageBatchEvent, force: bool = False, depth: str = "") -> str:
        if not self.enabled:
            return "KISS 官方情报插件未启用"
        if not self.enable_llm_tool:
            return "KISS 官方情报的 LLM 工具已关闭（可在插件配置中开启）"
        eff_depth = depth if depth in ("brief", "full") else self.content_depth
        try:
            if force:
                changes, errors = await self._check_all(force=True)
                data = changes + errors
            else:
                data = await self._check_cached()
            if not data:
                return "KISS 各源暂无数据"
            return "\n".join(_format(d, eff_depth) for d in data)
        except Exception as e:
            logger.exception("[kiss-info] 工具调用失败")
            return f"KISS 官方情报查询失败：{e}"

    # ==================== 命令 ====================

    @on.im_message(priority=Priority.HIGH)
    async def on_cmd(self, event: KiraMessageEvent, *_):
        if not self.enabled or not self.enable_command:
            return
        text = "".join(m.text for m in event.message.chain if isinstance(m, Text)).strip()
        cmd = self._match_command(text)
        if cmd is None:
            return
        arg = text[len(cmd):].strip()
        try:
            await self._handle_cmd(event, arg)
        except Exception:
            logger.exception("[kiss-info] 命令处理失败")
        event.discard()

    def _match_command(self, text: str) -> Optional[str]:
        if not text:
            return None
        hit = None
        for c in self.commands:
            if text.startswith(c) and (hit is None or len(c) > len(hit)):
                hit = c
        return hit

    @staticmethod
    def _sender_id(event: KiraMessageEvent) -> str:
        """取发送者 ID：3.0/2.x 都在 event.message.sender 上（哨兵字段 user_id 不存在）。"""
        try:
            sender = getattr(getattr(event, "message", None), "sender", None)
            sid = getattr(sender, "user_id", "") if sender is not None else ""
            return str(sid or "")
        except Exception:
            return ""

    async def _reply(self, event: KiraMessageEvent, text: str):
        """把文本回给当前会话（KiraMessageEvent 上没有 reply 方法，走统一发送入口）。"""
        try:
            sid = event.session.sid
        except Exception:
            return
        await self.ctx.send_message_chain(sid, MessageChain([Text(text)]))

    async def _handle_cmd(self, event: KiraMessageEvent, arg: str):
        sender = self._sender_id(event)
        parts = arg.split()
        sub = parts[0] if parts else "查"

        if sub == "查":
            data = await self._check_cached()
            msgs = [_format(d, self.content_depth) for d in data] or ["KISS 各源暂无数据"]
        elif sub == "详情":
            changes, errors = await self._check_all(force=True)
            msgs = [_format(d, "full") for d in (changes + errors)] or ["暂无数据"]
        elif sub == "历史":
            if len(parts) < 2:
                msgs = ["用法：/kiss 历史 YYYYMM（如 202608）"]
            else:
                yyyymm = parts[1]
                if not re.fullmatch(r"\d{6}", yyyymm):
                    msgs = ["月份格式不对，要 YYYYMM 如 202608"]
                else:
                    items = await self._fetch_month(yyyymm)
                    if not items:
                        msgs = [f"{yyyymm} 月没抓到公告，可能月份太早或页面结构变了"]
                    else:
                        lines = [f"{yyyymm} 月 KISS 公告（{len(items)}条）："]
                        for it in items:
                            lines.append(f"- {it['date']} {it['title']}")
                        msgs = ["\n".join(lines)]
        elif sub == "推送":
            if sender != OWNER_ID:
                msgs = ["只有主人能配置这个插件哦"]
            elif len(parts) < 2:
                msgs = ["用法：/kiss 推送 群号 或 QQ号（可多个，空格分隔）"]
            else:
                full = [self._normalize_target(t) for t in parts[1:]]
                full = [t for t in full if t]
                await self._update_config({"push_targets": full, "auto_check": True})
                msgs = [f"已配置推送作用域：{full}，有更新时会推送到这些目标"]
        elif sub == "开":
            if sender != OWNER_ID:
                msgs = ["只有主人能配置这个插件哦"]
            else:
                await self._update_config({"auto_check": True})
                state = "已开启自动推送" if self.push_targets else "已开启自动检查，但还没配置推送目标（/kiss 推送 群号）"
                msgs = [state]
        elif sub == "关":
            if sender != OWNER_ID:
                msgs = ["只有主人能配置这个插件哦"]
            else:
                await self._update_config({"auto_check": False})
                msgs = ["已关闭自动推送"]
        else:
            msgs = ["用法：/kiss 查 | 详情 | 历史 YYYYMM | 推送 群号 | 开 | 关"]

        for m in msgs:
            await self._reply(event, m)

    @staticmethod
    def _normalize_target(raw: str) -> str:
        """把用户输入规范成作用域 ID：裸数字→qq:gm:<号>；已带 adapter:type:id 的原样保留。"""
        t = str(raw or "").strip()
        if not t:
            return ""
        if t.isdigit():
            return f"qq:gm:{t}"
        return t

    async def _update_config(self, updates: dict) -> bool:
        """改插件配置：内存 + 落盘（data/config/plugins/<id>.json），并同步后台任务。

        注：这里写的是**真实的插件配置**，面板与框架读的就是它；
        历史版本写的是 data/plugin_data/kiss_info_config.json（没人读，等于没生效）。
        """
        try:
            sec = self.plugin_cfg.setdefault("section_main", {})
            for k, v in updates.items():
                sec[k] = v
                if k == "push_targets":
                    self.push_targets = self._as_list(v)
                elif k == "auto_check":
                    self.auto_check = bool(v)
            path = get_config_path() / "plugins" / f"{PLUGIN_ID}.json"
            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(
                path.write_text,
                json.dumps(self.plugin_cfg, ensure_ascii=False, indent=4),
                "utf-8",
            )
            self._sync_auto_task()
            return True
        except Exception as e:
            logger.error(f"[kiss-info] 配置回写失败: {e}")
            return False
