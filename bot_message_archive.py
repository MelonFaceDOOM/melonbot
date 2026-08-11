"""Stealth Discord message/reaction archive.

No Discord commands. Enable via MESSAGE_ARCHIVE_* env vars.
Operator status: local JSON file (MESSAGE_ARCHIVE_STATUS_PATH).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import asyncpg
import discord
from discord.ext import commands

from bot_helpers import upsert_guild, upsert_user
from config import (
    MESSAGE_ARCHIVE_GUILD_IDS,
    MESSAGE_ARCHIVE_STATUS_PATH,
)
from db_mixin import DbMixin

log = logging.getLogger("melonbot.message_archive")

BACKFILL_BATCH_SLEEP_S = 1.0
BACKFILL_IDLE_SLEEP_S = 60.0
CHANNEL_RESCAN_SLEEP_S = 300.0
STATUS_WRITE_INTERVAL_S = 30.0
HISTORY_PAGE_SIZE = 100

# Channel types that can have readable message history.
_ARCHIVABLE_TYPES = {
    discord.ChannelType.text,
    discord.ChannelType.news,
    discord.ChannelType.public_thread,
    discord.ChannelType.private_thread,
    discord.ChannelType.news_thread,
    discord.ChannelType.forum,  # catalog only; history is on threads
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emoji_key_from_partial(emoji: Any) -> tuple[str, Optional[int], Optional[str], bool]:
    """Return (emoji_key, emoji_id, emoji_name, is_custom)."""
    emoji_id = getattr(emoji, "id", None)
    if emoji_id is not None:
        name = getattr(emoji, "name", None) or str(emoji_id)
        return (f"c:{emoji_id}", int(emoji_id), name, True)
    # Unicode / PartialEmoji without id
    name = getattr(emoji, "name", None)
    if name:
        return (f"u:{name}", None, name, False)
    raw = str(emoji)
    return (f"u:{raw}", None, raw, False)


def _emoji_key_from_reaction(reaction: discord.Reaction) -> tuple[str, Optional[int], Optional[str], bool]:
    return _emoji_key_from_partial(reaction.emoji)


def _message_raw(message: discord.Message) -> dict:
    embeds = []
    for e in message.embeds:
        try:
            embeds.append(e.to_dict())
        except Exception:
            pass
    stickers = []
    for s in message.stickers:
        stickers.append({"id": s.id, "name": s.name, "format": getattr(s.format, "value", None)})
    return {
        "embeds": embeds,
        "stickers": stickers,
        "mention_everyone": message.mention_everyone,
        "mentions": [u.id for u in message.mentions],
        "role_mentions": [r.id for r in message.role_mentions],
        "channel_mentions": [c.id for c in message.channel_mentions],
        "flags": int(message.flags.value) if message.flags else 0,
    }


def _reference_id(message: discord.Message) -> Optional[int]:
    ref = message.reference
    if ref is None:
        return None
    return ref.message_id


def _thread_id(message: discord.Message) -> Optional[int]:
    channel = message.channel
    if isinstance(channel, discord.Thread):
        return channel.id
    return None


def _parent_id(channel: discord.abc.GuildChannel | discord.Thread) -> Optional[int]:
    if isinstance(channel, discord.Thread):
        return channel.parent_id
    category = getattr(channel, "category_id", None)
    return category


def _can_read_history(channel: discord.abc.GuildChannel | discord.Thread, me: discord.Member) -> bool:
    try:
        perms = channel.permissions_for(me)
    except Exception:
        return False
    return bool(perms.view_channel and perms.read_message_history)


class MessageArchiveCog(DbMixin, commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.guild_ids = MESSAGE_ARCHIVE_GUILD_IDS
        self.status_path = Path(MESSAGE_ARCHIVE_STATUS_PATH)
        self._backfill_task: Optional[asyncio.Task] = None
        self._status_task: Optional[asyncio.Task] = None
        self._started = False
        self._backfill_running = False
        self._current_channel_id: Optional[int] = None
        self._messages_upserted_total = 0
        self._last_error: Optional[str] = None
        self._lock = asyncio.Lock()

    async def aclose(self) -> None:
        for task in (self._backfill_task, self._status_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log.warning("archive task shutdown error: %s", e)
        self._backfill_task = None
        self._status_task = None
        await self._write_status()

    def _allowed_guild(self, guild_id: Optional[int]) -> bool:
        if guild_id is None:
            return False
        if not self.guild_ids:
            return False
        return guild_id in self.guild_ids

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        if self._started:
            return
        self._started = True
        if not self.guild_ids:
            log.warning(
                "MESSAGE_ARCHIVE_ENABLED but MESSAGE_ARCHIVE_GUILD_IDS empty; "
                "archiving nothing (fail closed)."
            )
            await self._write_status()
            return
        try:
            await self._bootstrap_channels()
        except Exception as e:
            self._last_error = str(e)
            log.exception("message archive bootstrap failed")
        self._status_task = asyncio.create_task(
            self._status_loop(), name="message_archive_status"
        )
        self._backfill_task = asyncio.create_task(
            self._backfill_loop(), name="message_archive_backfill"
        )
        await self._write_status()
        log.info(
            "message archive started for guilds=%s status=%s",
            sorted(self.guild_ids),
            self.status_path,
        )

    async def _bootstrap_channels(self) -> None:
        for guild in self.bot.guilds:
            if not self._allowed_guild(guild.id):
                continue
            await upsert_guild(self.db, guild)
            await self._sync_guild_channels(guild)

    async def _sync_guild_channels(self, guild: discord.Guild) -> None:
        me = guild.me
        if me is None:
            return

        channels: list[discord.abc.GuildChannel | discord.Thread] = []
        for ch in guild.channels:
            if ch.type in _ARCHIVABLE_TYPES:
                channels.append(ch)
        for thread in guild.threads:
            channels.append(thread)

        for ch in channels:
            await self._upsert_channel_row(guild, ch, me)

    async def _upsert_channel_row(
        self,
        guild: discord.Guild,
        channel: discord.abc.GuildChannel | discord.Thread,
        me: discord.Member,
    ) -> None:
        # Forum channels themselves have no linear history; mark skipped.
        if channel.type == discord.ChannelType.forum:
            readable = False
            status = "skipped"
        else:
            readable = _can_read_history(channel, me)
            status = "pending" if readable else "skipped"

        name = getattr(channel, "name", None)
        if name is not None:
            name = str(name)[:128]
        parent_id = _parent_id(channel)
        channel_type = int(channel.type.value) if channel.type is not None else None

        try:
            await self.db.execute(
                """
                INSERT INTO message_archive.channels (
                    id, guild_id, name, channel_type, parent_id, skip,
                    backfill_status, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE SET
                    guild_id = EXCLUDED.guild_id,
                    name = EXCLUDED.name,
                    channel_type = EXCLUDED.channel_type,
                    parent_id = EXCLUDED.parent_id,
                    skip = EXCLUDED.skip,
                    -- Only flip pending/skipped when not already mid-crawl or done
                    backfill_status = CASE
                        WHEN message_archive.channels.backfill_status IN (
                            'in_progress', 'complete', 'failed'
                        ) AND EXCLUDED.backfill_status = 'pending'
                            THEN message_archive.channels.backfill_status
                        WHEN EXCLUDED.skip OR EXCLUDED.backfill_status = 'skipped'
                            THEN 'skipped'
                        WHEN message_archive.channels.backfill_status = 'skipped'
                             AND EXCLUDED.backfill_status = 'pending'
                            THEN 'pending'
                        ELSE message_archive.channels.backfill_status
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                channel.id,
                guild.id,
                name,
                channel_type,
                parent_id,
                not readable,
                status,
            )
        except asyncpg.exceptions.PostgresError as e:
            log.error("upsert channel %s failed: %s", channel.id, e)
            self._last_error = str(e)

    # ------------------------------------------------------------------
    # Live message events
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.guild is None or not self._allowed_guild(message.guild.id):
            return
        if message.type == discord.MessageType.thread_created:
            return
        try:
            await self._ensure_channel_for_message(message)
            await self._upsert_message(message)
        except Exception as e:
            self._last_error = str(e)
            log.exception("on_message archive failed for %s", message.id)

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        guild_id = payload.guild_id
        if not self._allowed_guild(guild_id):
            return
        data = payload.data
        message_id = payload.message_id
        channel_id = payload.channel_id
        content = data.get("content")
        edited_raw = data.get("edited_timestamp")
        edited_at = None
        if edited_raw:
            try:
                edited_at = datetime.fromisoformat(edited_raw.replace("Z", "+00:00"))
            except ValueError:
                edited_at = datetime.now(timezone.utc)

        # Prefer full Message if cached
        message = payload.cached_message
        if message is not None:
            try:
                await self._ensure_channel_for_message(message)
                await self._upsert_message(message)
                return
            except Exception as e:
                self._last_error = str(e)
                log.exception("cached edit upsert failed for %s", message_id)

        if content is None and edited_at is None:
            return
        try:
            await self.db.execute(
                """
                UPDATE message_archive.messages
                SET content = COALESCE($2, content),
                    edited_at = COALESCE($3, edited_at),
                    raw = COALESCE(raw, '{}'::jsonb) || $4::jsonb
                WHERE id = $1
                """,
                message_id,
                content,
                edited_at,
                json.dumps({"partial_edit": True, "channel_id": channel_id}),
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("raw edit update failed for %s: %s", message_id, e)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if not self._allowed_guild(payload.guild_id):
            return
        try:
            await self.db.execute(
                """
                UPDATE message_archive.messages
                SET deleted_at = COALESCE(deleted_at, CURRENT_TIMESTAMP)
                WHERE id = $1
                """,
                payload.message_id,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("soft-delete failed for %s: %s", payload.message_id, e)

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        if not self._allowed_guild(payload.guild_id):
            return
        ids = list(payload.message_ids)
        if not ids:
            return
        try:
            await self.db.execute(
                """
                UPDATE message_archive.messages
                SET deleted_at = COALESCE(deleted_at, CURRENT_TIMESTAMP)
                WHERE id = ANY($1::bigint[])
                """,
                ids,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("bulk soft-delete failed: %s", e)

    # ------------------------------------------------------------------
    # Reactions
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if not self._allowed_guild(payload.guild_id):
            return
        await self._bump_reaction(payload.message_id, payload.emoji, delta=1)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        if not self._allowed_guild(payload.guild_id):
            return
        await self._bump_reaction(payload.message_id, payload.emoji, delta=-1)

    @commands.Cog.listener()
    async def on_raw_reaction_clear(self, payload: discord.RawReactionClearEvent):
        if not self._allowed_guild(payload.guild_id):
            return
        try:
            await self.db.execute(
                "DELETE FROM message_archive.reactions WHERE message_id = $1",
                payload.message_id,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("reaction clear failed for %s: %s", payload.message_id, e)

    @commands.Cog.listener()
    async def on_raw_reaction_clear_emoji(self, payload: discord.RawReactionClearEmojiEvent):
        if not self._allowed_guild(payload.guild_id):
            return
        key, _, _, _ = _emoji_key_from_partial(payload.emoji)
        try:
            await self.db.execute(
                """
                DELETE FROM message_archive.reactions
                WHERE message_id = $1 AND emoji_key = $2
                """,
                payload.message_id,
                key,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("reaction clear emoji failed for %s: %s", payload.message_id, e)

    async def _bump_reaction(self, message_id: int, emoji: Any, delta: int) -> None:
        key, emoji_id, emoji_name, is_custom = _emoji_key_from_partial(emoji)
        # Only adjust if message exists (avoid FK errors on unknown messages)
        try:
            exists = await self.db.fetchval(
                "SELECT 1 FROM message_archive.messages WHERE id = $1",
                message_id,
            )
            if not exists:
                return
            if delta >= 0:
                await self.db.execute(
                    """
                    INSERT INTO message_archive.reactions (
                        message_id, emoji_key, emoji_id, emoji_name, is_custom, count
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (message_id, emoji_key) DO UPDATE SET
                        count = message_archive.reactions.count + $6,
                        emoji_id = COALESCE(EXCLUDED.emoji_id, message_archive.reactions.emoji_id),
                        emoji_name = COALESCE(EXCLUDED.emoji_name, message_archive.reactions.emoji_name)
                    """,
                    message_id,
                    key,
                    emoji_id,
                    emoji_name,
                    is_custom,
                    delta,
                )
            else:
                await self.db.execute(
                    """
                    UPDATE message_archive.reactions
                    SET count = GREATEST(0, count + $3)
                    WHERE message_id = $1 AND emoji_key = $2
                    """,
                    message_id,
                    key,
                    delta,
                )
                await self.db.execute(
                    """
                    DELETE FROM message_archive.reactions
                    WHERE message_id = $1 AND emoji_key = $2 AND count = 0
                    """,
                    message_id,
                    key,
                )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("reaction bump failed for %s: %s", message_id, e)

    # ------------------------------------------------------------------
    # Channel catalog events
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        if not self._allowed_guild(channel.guild.id):
            return
        if channel.type not in _ARCHIVABLE_TYPES:
            return
        me = channel.guild.me
        if me is None:
            return
        await upsert_guild(self.db, channel.guild)
        await self._upsert_channel_row(channel.guild, channel, me)

    @commands.Cog.listener()
    async def on_guild_channel_update(
        self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel
    ):
        if not self._allowed_guild(after.guild.id):
            return
        if after.type not in _ARCHIVABLE_TYPES and before.type not in _ARCHIVABLE_TYPES:
            return
        me = after.guild.me
        if me is None:
            return
        await self._upsert_channel_row(after.guild, after, me)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        if not self._allowed_guild(channel.guild.id):
            return
        try:
            await self.db.execute(
                """
                UPDATE message_archive.channels
                SET skip = TRUE,
                    backfill_status = 'skipped',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                channel.id,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("channel delete mark failed for %s: %s", channel.id, e)

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        if not self._allowed_guild(thread.guild.id):
            return
        me = thread.guild.me
        if me is None:
            return
        await upsert_guild(self.db, thread.guild)
        await self._upsert_channel_row(thread.guild, thread, me)

    @commands.Cog.listener()
    async def on_thread_update(self, before: discord.Thread, after: discord.Thread):
        if not self._allowed_guild(after.guild.id):
            return
        me = after.guild.me
        if me is None:
            return
        await self._upsert_channel_row(after.guild, after, me)

    @commands.Cog.listener()
    async def on_thread_delete(self, thread: discord.Thread):
        if not self._allowed_guild(thread.guild.id):
            return
        try:
            await self.db.execute(
                """
                UPDATE message_archive.channels
                SET skip = TRUE,
                    backfill_status = 'skipped',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                thread.id,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("thread delete mark failed for %s: %s", thread.id, e)

    # ------------------------------------------------------------------
    # Persist helpers
    # ------------------------------------------------------------------

    async def _ensure_channel_for_message(self, message: discord.Message) -> None:
        guild = message.guild
        if guild is None:
            return
        await upsert_guild(self.db, guild)
        channel = message.channel
        me = guild.me
        if me is None:
            return
        if isinstance(channel, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
            await self._upsert_channel_row(guild, channel, me)
        else:
            # Fallback: insert minimal channel row so FK succeeds
            try:
                await self.db.execute(
                    """
                    INSERT INTO message_archive.channels (
                        id, guild_id, name, channel_type, skip, backfill_status, updated_at
                    )
                    VALUES ($1, $2, $3, $4, FALSE, 'pending', CURRENT_TIMESTAMP)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    channel.id,
                    guild.id,
                    getattr(channel, "name", None),
                    int(getattr(channel.type, "value", 0)),
                )
            except asyncpg.exceptions.PostgresError as e:
                log.error("minimal channel insert failed: %s", e)

    async def _upsert_message(self, message: discord.Message) -> None:
        guild = message.guild
        if guild is None:
            return
        author = message.author
        await upsert_user(self.db, author)

        reference_id = _reference_id(message)
        thread_id = _thread_id(message)
        has_attachments = bool(message.attachments)
        raw = json.dumps(_message_raw(message))
        created_at = message.created_at
        edited_at = message.edited_at
        is_bot = bool(getattr(author, "bot", False))
        webhook_id = message.webhook_id
        message_type = int(message.type.value) if message.type is not None else None

        try:
            await self.db.execute(
                """
                INSERT INTO message_archive.messages (
                    id, guild_id, channel_id, author_id, content,
                    created_at, edited_at, message_type, is_bot, webhook_id,
                    reference_id, thread_id, has_attachments, raw
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14::jsonb
                )
                ON CONFLICT (id) DO UPDATE SET
                    content = EXCLUDED.content,
                    edited_at = EXCLUDED.edited_at,
                    message_type = EXCLUDED.message_type,
                    has_attachments = EXCLUDED.has_attachments,
                    raw = EXCLUDED.raw,
                    deleted_at = message_archive.messages.deleted_at
                """,
                message.id,
                guild.id,
                message.channel.id,
                author.id,
                message.content,
                created_at,
                edited_at,
                message_type,
                is_bot,
                webhook_id,
                reference_id,
                thread_id,
                has_attachments,
                raw,
            )
            self._messages_upserted_total += 1
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("message upsert failed for %s: %s", message.id, e)
            return

        if message.attachments:
            await self._upsert_attachments(message)
        if message.reactions:
            await self._upsert_reactions_from_message(message)

    async def _upsert_attachments(self, message: discord.Message) -> None:
        for att in message.attachments:
            try:
                await self.db.execute(
                    """
                    INSERT INTO message_archive.attachments (
                        id, message_id, filename, content_type, size, url
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (id) DO UPDATE SET
                        filename = EXCLUDED.filename,
                        content_type = EXCLUDED.content_type,
                        size = EXCLUDED.size,
                        url = EXCLUDED.url
                    """,
                    att.id,
                    message.id,
                    att.filename,
                    att.content_type,
                    att.size,
                    att.url,
                )
            except asyncpg.exceptions.PostgresError as e:
                log.error("attachment upsert failed for %s: %s", att.id, e)

    async def _upsert_reactions_from_message(self, message: discord.Message) -> None:
        for reaction in message.reactions:
            key, emoji_id, emoji_name, is_custom = _emoji_key_from_reaction(reaction)
            try:
                await self.db.execute(
                    """
                    INSERT INTO message_archive.reactions (
                        message_id, emoji_key, emoji_id, emoji_name, is_custom, count
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (message_id, emoji_key) DO UPDATE SET
                        count = EXCLUDED.count,
                        emoji_id = EXCLUDED.emoji_id,
                        emoji_name = EXCLUDED.emoji_name,
                        is_custom = EXCLUDED.is_custom
                    """,
                    message.id,
                    key,
                    emoji_id,
                    emoji_name,
                    is_custom,
                    int(reaction.count),
                )
            except asyncpg.exceptions.PostgresError as e:
                log.error("reaction upsert failed for %s: %s", message.id, e)

    # ------------------------------------------------------------------
    # Backfill
    # ------------------------------------------------------------------

    async def _backfill_loop(self) -> None:
        while True:
            try:
                channel_row = await self._claim_next_channel()
                if channel_row is None:
                    # Rescan catalogs periodically for new threads/channels
                    await self._bootstrap_channels()
                    channel_row = await self._claim_next_channel()
                if channel_row is None:
                    self._backfill_running = False
                    self._current_channel_id = None
                    await self._write_status()
                    await asyncio.sleep(CHANNEL_RESCAN_SLEEP_S)
                    continue

                self._backfill_running = True
                self._current_channel_id = channel_row["id"]
                await self._write_status()
                await self._backfill_channel(channel_row)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = str(e)
                log.exception("backfill loop error")
                await asyncio.sleep(BACKFILL_IDLE_SLEEP_S)

    async def _claim_next_channel(self) -> Optional[asyncpg.Record]:
        if not self.guild_ids:
            return None
        guild_ids = list(self.guild_ids)
        async with self._lock:
            row = await self.db.fetchrow(
                """
                SELECT id, guild_id, oldest_message_id, newest_message_id, backfill_status
                FROM message_archive.channels
                WHERE guild_id = ANY($1::bigint[])
                  AND skip = FALSE
                  AND backfill_status IN ('pending', 'failed', 'in_progress')
                ORDER BY
                    CASE backfill_status
                        WHEN 'in_progress' THEN 0
                        WHEN 'pending' THEN 1
                        WHEN 'failed' THEN 2
                        ELSE 3
                    END,
                    id
                LIMIT 1
                """,
                guild_ids,
            )
            if row is None:
                return None
            await self.db.execute(
                """
                UPDATE message_archive.channels
                SET backfill_status = 'in_progress',
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                row["id"],
            )
            return row

    async def _backfill_channel(self, channel_row: asyncpg.Record) -> None:
        channel_id = channel_row["id"]
        guild_id = channel_row["guild_id"]
        # Resume: when oldest_first, Discord's `after` means messages newer than this id.
        # We crawl oldest→newest, storing the furthest id we have reached as newest_message_id
        # while in progress; on first run both are null.
        cursor_after = channel_row["newest_message_id"]

        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden) as e:
                await self._mark_channel(channel_id, "failed", str(e))
                return
            except discord.HTTPException as e:
                await self._mark_channel(channel_id, "failed", str(e))
                return

        if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
            await self._mark_channel(channel_id, "skipped", "not a text-history channel")
            return

        guild = channel.guild
        if guild is None or guild.id != guild_id:
            await self._mark_channel(channel_id, "failed", "guild mismatch")
            return

        me = guild.me
        if me is None or not _can_read_history(channel, me):
            await self._mark_channel(channel_id, "skipped", "missing read history permission")
            return

        after_obj = discord.Object(id=cursor_after) if cursor_after else None
        batch: list[discord.Message] = []
        try:
            async for message in channel.history(
                limit=None,
                oldest_first=True,
                after=after_obj,
            ):
                if not self._allowed_guild(message.guild.id if message.guild else None):
                    continue
                batch.append(message)
                if len(batch) >= HISTORY_PAGE_SIZE:
                    await self._persist_backfill_batch(channel_id, batch)
                    batch.clear()
                    await asyncio.sleep(BACKFILL_BATCH_SLEEP_S)

            if batch:
                await self._persist_backfill_batch(channel_id, batch)
                batch.clear()

            await self._mark_channel(channel_id, "complete", None)
        except discord.Forbidden as e:
            await self._mark_channel(channel_id, "skipped", str(e))
        except discord.HTTPException as e:
            retry = getattr(e, "retry_after", None)
            if retry:
                await asyncio.sleep(float(retry) + 0.5)
            await self._mark_channel(channel_id, "failed", str(e))
        except Exception as e:
            await self._mark_channel(channel_id, "failed", str(e))
            log.exception("backfill channel %s failed", channel_id)

    async def _persist_backfill_batch(
        self, channel_id: int, messages: list[discord.Message]
    ) -> None:
        if not messages:
            return
        for message in messages:
            await self._ensure_channel_for_message(message)
            await self._upsert_message(message)

        ids = [m.id for m in messages]
        oldest = min(ids)
        newest = max(ids)
        try:
            await self.db.execute(
                """
                UPDATE message_archive.channels
                SET oldest_message_id = LEAST(COALESCE(oldest_message_id, $2), $2),
                    newest_message_id = GREATEST(COALESCE(newest_message_id, $3), $3),
                    last_backfill_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                channel_id,
                oldest,
                newest,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("cursor update failed for channel %s: %s", channel_id, e)

    async def _mark_channel(
        self, channel_id: int, status: str, error: Optional[str]
    ) -> None:
        skip = status == "skipped"
        try:
            await self.db.execute(
                """
                UPDATE message_archive.channels
                SET backfill_status = $2,
                    skip = CASE WHEN $3 THEN TRUE ELSE skip END,
                    last_error = $4,
                    last_backfill_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                channel_id,
                status,
                skip,
                error,
            )
        except asyncpg.exceptions.PostgresError as e:
            self._last_error = str(e)
            log.error("mark channel %s %s failed: %s", channel_id, status, e)
        if error:
            self._last_error = error
        if self._current_channel_id == channel_id and status != "in_progress":
            self._current_channel_id = None
        await self._write_status()

    # ------------------------------------------------------------------
    # Status file
    # ------------------------------------------------------------------

    async def _status_loop(self) -> None:
        while True:
            try:
                await self._write_status()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("status write failed: %s", e)
            await asyncio.sleep(STATUS_WRITE_INTERVAL_S)

    async def _write_status(self) -> None:
        counts = {
            "pending": 0,
            "in_progress": 0,
            "complete": 0,
            "failed": 0,
            "skipped": 0,
        }
        message_count = 0
        try:
            if self.guild_ids and getattr(self.bot, "db_pool", None) is not None:
                rows = await self.db.fetch(
                    """
                    SELECT backfill_status, COUNT(*)::int AS n
                    FROM message_archive.channels
                    WHERE guild_id = ANY($1::bigint[])
                    GROUP BY backfill_status
                    """,
                    list(self.guild_ids),
                )
                for row in rows:
                    key = row["backfill_status"]
                    if key in counts:
                        counts[key] = row["n"]
                message_count = await self.db.fetchval(
                    """
                    SELECT COUNT(*)::bigint
                    FROM message_archive.messages
                    WHERE guild_id = ANY($1::bigint[])
                    """,
                    list(self.guild_ids),
                ) or 0
        except Exception as e:
            # DB may be unavailable during shutdown
            self._last_error = self._last_error or str(e)

        payload = {
            "updated_at": _utc_now_iso(),
            "enabled": True,
            "guild_ids": sorted(self.guild_ids),
            "backfill_running": self._backfill_running,
            "current_channel_id": self._current_channel_id,
            "channels": counts,
            "messages_in_db": int(message_count),
            "messages_upserted_total": self._messages_upserted_total,
            "last_error": self._last_error,
        }
        try:
            self.status_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.status_path.with_suffix(self.status_path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(self.status_path)
        except OSError as e:
            log.warning("could not write status file %s: %s", self.status_path, e)
