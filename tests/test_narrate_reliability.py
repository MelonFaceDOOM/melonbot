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
        await self.cog._add_user_spend(7, 0.02)
        self.assertTrue(await self.cog._over_user_share(7))   # 5.01 >= half of $10
        db.execute.assert_awaited_once()
        self.assertEqual(db.fetchrow.await_count, 1)          # cached after the first read

    async def test_synth_cost_is_charged_to_the_user(self):
        self.cog.router.synth.return_value = SynthResult(OGG, "ogg_opus", "fish", None, 0.003)
        self.cog._add_user_spend = AsyncMock()
        await self.narrate()
        self.cog._add_user_spend.assert_awaited_once_with(2, 0.003)

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
