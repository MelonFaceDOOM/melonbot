import asyncio
import sys
import time
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

import discord

# Import the narration code without loading operator credentials from config/.env.
config = types.ModuleType("config")
config.google_narrate_key = "test-key"
config.fish_api_key = ""
config.gemini_api_key = ""
config.FISH_DEFAULT_VOICE_ID = ""
config.FISH_MODEL = "s2.1-pro"
config.GEMINI_MODEL = "gemini-3.8-flash-tts"
config.GEMINI_RPM = 9
config.GEMINI_FALLBACK_MODELS = ()
config.TTS_MONTHLY_BUDGET_USD = 10.0
with patch.dict(sys.modules, {"config": config}):
    import bot_narrate
    from tts_providers import SynthResult

OGG = b"OggS" + b"\x00" * 20


class SessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.session = bot_narrate.GuildVoiceSession(Mock(), 1, Mock())
        self.session._ensure_player = Mock()
        self.session._ensure_idle_timer = Mock()
        self.channel = Mock(id=10)
        self.channel.connect = AsyncMock()

    async def test_cancel_and_teardown_bump_generation(self):
        g = self.session.generation
        await self.session.cancel_playback()
        await self.session.teardown()
        self.assertEqual(self.session.generation, g + 2)

    async def test_adopts_connection_discord_still_holds(self):
        existing = Mock(spec=discord.VoiceClient)
        existing.is_connected.return_value = True
        existing.channel = self.channel
        self.channel.guild.voice_client = existing
        await self.session.ensure_connected(self.channel)
        self.channel.connect.assert_not_awaited()
        self.assertIs(self.session.voice_client, existing)

    async def test_full_playback_queue_drops_instead_of_blocking(self):
        self.session.queue = asyncio.Queue(maxsize=1)
        await self.session.enqueue(b"a")
        await asyncio.wait_for(self.session.enqueue(b"b"), timeout=1)  # would hang before
        self.assertEqual(self.session.queue.qsize(), 1)

    async def test_playback_done_callback_wakes_loop_from_audio_thread(self):
        vc = Mock()
        vc.is_connected.return_value = True

        def play(source, after):
            # discord.py calls `after` from its audio player thread
            import threading
            threading.Thread(target=after, args=(None,)).start()

        vc.play.side_effect = play
        self.session.voice_client = vc
        with patch.object(bot_narrate.discord, "FFmpegPCMAudio", Mock()):
            await asyncio.wait_for(self.session._play_one(OGG), timeout=2)


class CogTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.bot = Mock()
        self.cog = bot_narrate.NarrationCog(self.bot)
        self.guild = Mock(id=1)
        self.member = Mock()
        self.member.voice.channel = Mock(id=10)
        self.guild.get_member.return_value = self.member
        self.bot.get_guild.return_value = self.guild
        self.session = self.cog._get_session(1)
        self.session.ensure_connected = AsyncMock()
        self.session.enqueue = AsyncMock()
        self.cog._get_pref = AsyncMock(return_value={"enabled": True, "text_channel_id": 5})
        self.cog._resolve_voiceline_override = AsyncMock(return_value=None)
        self.cog.router.synth = AsyncMock(return_value=SynthResult(OGG, "ogg_opus", "google", None))

    async def narrate(self, text="hello there", gen=None):
        await self.cog._narrate_one(1, 2, text, "en-US-Wavenet-D", None, 1.0, 5,
                                    self.session.generation if gen is None else gen)

    async def test_message_queued_before_cancel_is_dropped(self):
        gen = self.session.generation
        await self.session.cancel_playback()
        await self.narrate(gen=gen)
        self.cog.router.synth.assert_not_awaited()
        self.session.ensure_connected.assert_not_awaited()

    async def test_prefs_rechecked_before_rejoining(self):
        self.cog._get_pref.return_value = {"enabled": False, "text_channel_id": 5}
        await self.narrate()
        self.session.ensure_connected.assert_not_awaited()
        self.cog.router.synth.assert_not_awaited()

    async def test_cancel_mid_message_stops_paying_for_chunks(self):
        async def synth_then_cancel(*a, **kw):
            self.session.generation += 1
            return SynthResult(OGG, "ogg_opus", "google", None)

        self.cog.router.synth.side_effect = synth_then_cancel
        long_text = " ".join(f"This is sentence number {i}." for i in range(40))
        await self.narrate(long_text)
        self.assertEqual(self.cog.router.synth.await_count, 1)
        self.session.enqueue.assert_not_awaited()

    async def test_all_chunks_kept_for_save(self):
        long_text = " ".join(f"This is sentence number {i}." for i in range(20))
        await self.narrate(long_text)
        clips = self.cog._last_narrated_audio[(1, 2)]
        self.assertEqual(len(clips), self.cog.router.synth.await_count)
        self.assertGreater(len(clips), 1)

    async def test_worker_survives_a_bad_message(self):
        self.cog._narrate_one = AsyncMock(side_effect=[RuntimeError("boom"), None])
        self.cog._enqueue_narration(1, ("a",))
        self.cog._enqueue_narration(1, ("b",))
        await asyncio.wait_for(self.cog._guild_queues[1].join(), timeout=1)
        self.assertEqual(self.cog._narrate_one.await_count, 2)
        self.cog._guild_workers[1].cancel()

    async def test_backlog_is_bounded(self):
        self.cog._narrate_one = AsyncMock(side_effect=lambda *a: asyncio.sleep(10))
        results = [self.cog._enqueue_narration(1, ("x",)) for _ in range(bot_narrate.GUILD_BACKLOG + 5)]
        self.assertIn(False, results)
        self.cog._guild_workers[1].cancel()

    async def test_voice_history_skips_repeat_of_last_voice(self):
        db = Mock()
        db.fetchrow = AsyncMock(return_value={"voice": "gemini:kore"})
        db.execute = AsyncMock()
        self.bot.db_pool = db
        await self.cog._record_voice_history(1, 2, "gemini:kore", "Kore")
        db.execute.assert_not_awaited()
        await self.cog._record_voice_history(1, 2, "gemini:puck", "Puck")
        self.assertEqual(db.execute.await_args.args[1:], (1, 2, "gemini:puck", "Puck"))

    async def _run_recent(self, parsed, *user_input, history=None):
        from datetime import datetime
        history = history if history is not None else [
            {"voice": f"gemini:v{i}", "voice_label": f"Voice {i}", "set_at": datetime(2026, 11, 3 - i % 3)}
            for i in range(20)]
        db = Mock()
        db.fetch = AsyncMock(return_value=history)
        self.bot.db_pool = db
        self.cog._get_pref.return_value = {"voice": "gemini:v0"}
        sent = []
        ctx = Mock(clean_prefix="!")
        ctx.author.id, ctx.author.name = 2, "melon"
        ctx.send = AsyncMock()
        with patch.object(bot_narrate, "get_guild_id", AsyncMock(return_value=1)), \
             patch.object(bot_narrate, "parse_user_input_for_mention", AsyncMock(return_value=parsed)), \
             patch.object(bot_narrate, "send_goodly", AsyncMock(side_effect=lambda c, m: sent.append(m))):
            await self.cog.narrate_recent.callback(self.cog, ctx, *user_input)
        return db, ctx, sent

    async def test_recent_defaults_to_you_and_15(self):
        db, ctx, sent = await self._run_recent((None, None, None))
        self.assertEqual(db.fetch.await_args.args[1:], (1, 2))
        body = sent[0]
        self.assertIn("RECENT VOICES FROM MELON", body)
        self.assertEqual(body.count("!narrate voice gemini:"), 15)
        self.assertIn("Voice 0 (Gemini) *", body)       # current voice is marked
        self.assertNotIn("Voice 1 (Gemini) *", body)

    async def test_recent_for_a_mentioned_user_with_page_size(self):
        db, ctx, sent = await self._run_recent(((3, 2), 77, "roko"), "[3,2]", "<@77>")
        self.assertEqual(db.fetch.await_args.args[1:], (1, 77))
        self.assertIn("VOICES FROM ROKO", sent[0])
        self.assertEqual(sent[0].count("!narrate voice gemini:"), 3)
        self.assertIn("gemini:v3", sent[0])              # page 2 of 3 = entries 3..5

    async def test_recent_unknown_name(self):
        db, ctx, sent = await self._run_recent((None, None, None), "nobodyhere")
        db.fetch.assert_not_awaited()
        self.assertIn("Couldn't find", ctx.send.await_args.args[0])

    async def test_recent_empty_history(self):
        db, ctx, sent = await self._run_recent((None, None, None), history=[])
        self.assertIn("No voice history", ctx.send.await_args.args[0])

    async def test_gemini_fallbacks_prefer_history_then_same_name_google(self):
        db = Mock()
        db.fetchrow = AsyncMock(side_effect=[
            {"voice": "fish:" + "a" * 32, "voice_label": "Guyzo"},                 # last non-Gemini voice
            {"voice_name": "en-US-Chirp3-HD-Despina", "nickname": "Elizabeth"},  # same name on Google
        ])
        db.fetch = AsyncMock()
        self.bot.db_pool = db
        refs = await self.cog._gemini_fallbacks(1, 2, "gemini:despina@gemini-2.5-flash-preview-tts")
        self.assertEqual(refs, [("fish:" + "a" * 32, "Guyzo"), ("en-US-Chirp3-HD-Despina", "Elizabeth")])
        self.assertEqual(db.fetchrow.await_args_list[1].args[1], "en-US-Chirp3-HD-despina")
        db.fetch.assert_not_awaited()
        self.assertEqual(await self.cog._gemini_fallbacks(1, 2, "gemini:despina@gemini-2.5-flash-preview-tts"), refs)
        self.assertEqual(db.fetchrow.await_count, 2)  # cached

    async def test_gemini_fallback_without_history_or_same_name_is_stable(self):
        pool = [{"voice_name": f"en-US-Chirp3-HD-V{i}", "nickname": f"N{i}"} for i in range(30)]
        db = Mock()
        db.fetchrow = AsyncMock(return_value=None)
        db.fetch = AsyncMock(return_value=pool)
        self.bot.db_pool = db
        first = await self.cog._gemini_fallbacks(1, 7, "gemini:storyteller-2")
        self.cog._fallback_cache.clear()
        again = await self.cog._gemini_fallbacks(1, 7, "gemini:storyteller-2")
        self.assertEqual(first, again)
        self.assertEqual(first, [("en-US-Chirp3-HD-V7", "N7")])

    async def test_gemini_out_of_quota_switches_the_users_voice_once(self):
        self.cog._gemini_fallbacks = AsyncMock(return_value=[("en-US-Chirp3-HD-Kore", "Sofia")])
        self.cog._switch_off_gemini = AsyncMock()
        self.cog._notify_fallback = AsyncMock()
        calls = []

        async def synth(voice, part, **kw):
            calls.append(voice)
            if voice.startswith("gemini:"):
                return SynthResult(OGG, "ogg_opus", "google", "gemini_limit", voice_ref_used="en-US-Chirp3-HD-Kore")
            return SynthResult(OGG, "ogg_opus", "google", None, voice_ref_used=voice)

        self.cog.router.synth = AsyncMock(side_effect=synth)
        long_text = " ".join(f"This is sentence number {i}." for i in range(20))
        await self.cog._narrate_one(1, 2, long_text, "gemini:kore", None, 1.0, 5, self.session.generation)
        self.cog._switch_off_gemini.assert_awaited_once()
        self.assertEqual(self.cog._switch_off_gemini.await_args.args[3:], ("gemini:kore", "en-US-Chirp3-HD-Kore", "Sofia"))
        self.assertEqual(calls[0], "gemini:kore")
        self.assertTrue(all(v == "en-US-Chirp3-HD-Kore" for v in calls[1:]))  # rest of the message uses it
        self.cog._notify_fallback.assert_not_awaited()

    async def test_gemini_error_uses_fallback_without_switching(self):
        self.cog._gemini_fallbacks = AsyncMock(return_value=[("en-US-Chirp3-HD-Kore", "Sofia")])
        self.cog._switch_off_gemini = AsyncMock()
        self.cog._notify_fallback = AsyncMock()
        self.cog.router.synth = AsyncMock(return_value=SynthResult(
            OGG, "ogg_opus", "google", "error", voice_ref_used="en-US-Chirp3-HD-Kore"))
        await self.cog._narrate_one(1, 2, "hello there", "gemini:kore", None, 1.0, 5, self.session.generation)
        self.cog._switch_off_gemini.assert_not_awaited()
        self.cog._notify_fallback.assert_awaited_once()

    async def test_switch_off_gemini_saves_and_announces(self):
        db = Mock()
        db.execute = AsyncMock()
        db.fetchrow = AsyncMock(return_value=None)
        self.bot.db_pool = db
        self.bot.command_prefix = "!"
        ch = Mock()
        ch.send = AsyncMock()
        self.guild.get_channel.return_value = ch
        self.member.display_name = "melon"
        await self.cog._switch_off_gemini(self.guild, 2, 5, "gemini:kore@gemini-2.5-flash-preview-tts",
                                          "en-US-Chirp3-HD-Kore", "Sofia")
        update = db.execute.await_args_list[0].args
        self.assertIn("UPDATE narrate_prefs", update[0])
        self.assertEqual(update[1:], (1, 2, "en-US-Chirp3-HD-Kore", "Sofia", "gemini:kore@gemini-2.5-flash-preview-tts"))
        msg = ch.send.await_args.args[0]
        self.assertIn("melon", msg)
        self.assertIn("!narrate voice gemini:kore@gemini-2.5-flash-preview-tts", msg)

    def test_bot_commands_are_not_narrated(self):
        bot = Mock(command_prefix="!")
        bot.get_command = lambda name: Mock() if name.lower() in {"quota", "narrate", "rate"} else None
        is_cmd = bot_narrate._message_is_bot_command
        self.assertTrue(is_cmd(bot, "!quota"))
        self.assertTrue(is_cmd(bot, "!Quota"))
        self.assertTrue(is_cmd(bot, "!narrate voice gemini:Kore"))
        self.assertTrue(is_cmd(bot, "  !rate 5"))
        self.assertFalse(is_cmd(bot, "!wow that's crazy"))
        self.assertFalse(is_cmd(bot, "! lol"))
        self.assertFalse(is_cmd(bot, "quota please"))
        self.assertFalse(is_cmd(bot, "!"))

    def test_rate_bucket_only_trips_on_floods(self):
        # A normal burst of chat, even with a pasted max-length message, is fine
        for _ in range(4):
            self.assertTrue(self.cog._take_rate_tokens(7, 60))
        self.assertTrue(self.cog._take_rate_tokens(7, bot_narrate.MAX_NARRATE_CHARS))
        # Pasting long messages back to back runs the bucket dry
        self.assertTrue(self.cog._take_rate_tokens(7, bot_narrate.MAX_NARRATE_CHARS))
        self.assertFalse(self.cog._take_rate_tokens(7, bot_narrate.MAX_NARRATE_CHARS))

    async def test_user_share_of_budget(self):
        db = Mock()
        db.fetchrow = AsyncMock(return_value={"est_usd": 4.99})
        db.execute = AsyncMock()
        self.bot.db_pool = db
        self.assertFalse(await self.cog._over_user_share(7))
        await self.cog._add_spend(1, 7, 0.02)
        self.assertTrue(await self.cog._over_user_share(7))   # 5.01 >= half of $10
        self.assertEqual(db.execute.await_count, 2)           # user row + server row
        self.assertEqual(db.fetchrow.await_count, 1)          # cached after the first read

    async def test_synth_cost_is_charged_to_the_user(self):
        self.cog.router.synth.return_value = SynthResult(OGG, "ogg_opus", "fish", None, 0.003)
        self.cog._add_spend = AsyncMock()
        await self.narrate()
        self.cog._add_spend.assert_awaited_once_with(1, 2, 0.003)

    async def test_user_over_share_gets_google_voice(self):
        self.cog._get_pref.return_value = {"enabled": True, "text_channel_id": 5,
                                           "voice": "fish:" + "a" * 32, "style": None, "rate": 1.0}
        self.cog._over_user_share = AsyncMock(return_value=True)
        self.cog._notify_fallback = AsyncMock()
        self.cog._enqueue_narration = Mock()
        msg = Mock(content="hello there everyone")
        msg.author.bot = False
        msg.author.id = 2
        msg.guild.id = 1
        msg.channel.id = 5
        self.bot.command_prefix = "!"
        await self.cog.on_message(msg)
        item = self.cog._enqueue_narration.call_args.args[1]
        self.assertEqual(item[2], bot_narrate.DEFAULT_GOOGLE_VOICE)
        self.cog._notify_fallback.assert_awaited_once()

    async def test_quota_shows_user_server_and_shared_budget(self):
        self.cog._user_spent = AsyncMock(return_value=1.25)
        self.cog._guild_spent = AsyncMock(return_value=3.5)
        self.cog.budget.spent = AsyncMock(return_value=4.0)
        ctx = Mock()
        ctx.send = AsyncMock()
        await self.cog.narrate_quota.callback(self.cog, ctx)
        embed = ctx.send.await_args.kwargs["embed"]
        values = {f.name: f.value for f in embed.fields}
        self.assertIn("$1.25 of your $5.00 limit", values["You"])
        self.assertIn("$3.50", values["This server"])
        self.assertIn("$4.00 of $10.00", values["Shared budget (all servers)"])
        self.assertIn("40%", values["Shared budget (all servers)"])

    async def test_quota_when_db_is_down(self):
        self.cog._user_spent = AsyncMock(return_value=0.0)
        self.cog._guild_spent = AsyncMock(return_value=None)
        self.cog.budget.spent = AsyncMock(return_value=float("inf"))
        ctx = Mock()
        ctx.send = AsyncMock()
        await self.cog.narrate_quota.callback(self.cog, ctx)
        values = {f.name: f.value for f in ctx.send.await_args.kwargs["embed"].fields}
        self.assertIn("unavailable", values["This server"])
        self.assertIn("unavailable", values["Shared budget (all servers)"])

    async def test_voices_embed(self):
        self.cog.fish, self.cog.gemini = Mock(), Mock()
        self.cog._get_pref.return_value = {"voice": "fish:" + "a" * 32, "voice_label": "Guyzo"}
        ctx = Mock(clean_prefix="?")
        embed = await self.cog._voices_embed(ctx)
        self.assertIn("Guyzo (Fish)", embed.description)
        names = [f.name for f in embed.fields]
        self.assertEqual(len(names), 5)
        self.assertTrue(all("?narrate" in f.value for f in embed.fields))
        self.assertFalse(any("!narrate" in f.value for f in embed.fields))
        self.assertTrue(all(len(f.value) <= 1024 for f in embed.fields))  # Discord field limit
        self.assertLess(len(embed), 6000)                                  # Discord embed limit

    async def test_gemini_voice_with_model(self):
        self.cog.gemini = Mock()
        self.cog.gemini.find_voice = AsyncMock(return_value={"id": "kore", "display_name": "Kore"})
        self.cog._apply_voice = AsyncMock()
        ctx = Mock()
        ctx.send = AsyncMock()
        await self.cog._set_gemini_voice(ctx, "Kore@2.5")
        ref, label, _ = self.cog._apply_voice.await_args.args[1:]
        self.assertEqual(ref, "gemini:kore@gemini-2.5-flash-preview-tts")
        self.assertEqual(label, "Kore · Gemini 2.5 Flash")
        self.cog.gemini.find_voice.assert_awaited_once_with("Kore")

        await self.cog._set_gemini_voice(ctx, "Kore")
        self.assertEqual(self.cog._apply_voice.await_args.args[1], "gemini:kore")

        self.cog._apply_voice.reset_mock()
        await self.cog._set_gemini_voice(ctx, "Kore@9.9")
        self.cog._apply_voice.assert_not_awaited()
        self.assertIn("2.5-pro", ctx.send.await_args.args[0])

    async def test_voices_embed_explains_model_choice(self):
        self.cog.fish, self.cog.gemini = Mock(), Mock()
        embed = await self.cog._voices_embed(Mock(clean_prefix="!"))
        gemini = next(f.value for f in embed.fields if f.name.startswith("✨"))
        self.assertIn("gemini:Kore@2.5", gemini)
        self.assertIn("`2.5-pro`", gemini)

    async def test_voices_embed_without_paid_providers(self):
        ctx = Mock(clean_prefix="!")
        embed = await self.cog._voices_embed(ctx)
        self.assertEqual([f.name for f in embed.fields], ["🔊 Google"])

    async def test_own_voice_recycle_is_not_treated_as_a_kick(self):
        self.cog._set_all_prefs_disabled = AsyncMock()
        self.session.teardown = AsyncMock()
        self.session.self_disconnect_at = time.monotonic()
        me = Mock(id=99)
        guild = Mock(id=1, me=me)
        member = Mock(id=99, guild=guild)
        await self.cog.on_voice_state_update(member, Mock(channel=Mock()), Mock(channel=None))
        self.cog._set_all_prefs_disabled.assert_not_awaited()

    async def test_real_kick_still_disables(self):
        self.cog._set_all_prefs_disabled = AsyncMock()
        self.session.teardown = AsyncMock()
        me = Mock(id=99)
        guild = Mock(id=1, me=me)
        member = Mock(id=99, guild=guild)
        await self.cog.on_voice_state_update(member, Mock(channel=Mock()), Mock(channel=None))
        self.cog._set_all_prefs_disabled.assert_awaited_once_with(1)


if __name__ == "__main__":
    unittest.main()
