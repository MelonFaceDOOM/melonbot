import asyncio
import sys
import time
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

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


class NarrationIdleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.session = bot_narrate.GuildVoiceSession(Mock(), 123, Mock())
        # Drive the idle loop explicitly; no audio or Discord connection is needed.
        self.session._ensure_player = Mock()
        self.session._ensure_idle_timer = Mock()
        self.channel = Mock(id=456)
        self.vc = Mock(channel=self.channel)
        self.vc.is_connected.return_value = True
        self.vc.is_playing.return_value = False
        self.vc.disconnect = AsyncMock()
        self.vc.move_to = AsyncMock()
        self.channel.connect = AsyncMock(return_value=self.vc)

    def expire_activity(self):
        self.session.last_activity = time.monotonic() - bot_narrate.PLAYBACK_IDLE_DISCONNECT_SECS - 10

    async def idle_tick(self):
        with patch.object(bot_narrate.asyncio, "sleep", AsyncMock(
            side_effect=[None, asyncio.CancelledError]
        )):
            try:
                await self.session._idle_loop()
            except asyncio.CancelledError:
                pass

    async def test_first_join_after_session_sat_unused_stays_connected(self):
        self.expire_activity()
        await self.session.ensure_connected(self.channel)
        await self.idle_tick()
        self.vc.disconnect.assert_not_awaited()
        self.assertIs(self.session.voice_client, self.vc)

    async def test_rejoin_after_teardown_stays_connected_without_a_message(self):
        await self.session.ensure_connected(self.channel)
        await self.session.teardown()
        self.vc.disconnect.reset_mock()
        self.expire_activity()
        await self.session.ensure_connected(self.channel)
        await self.idle_tick()
        self.vc.disconnect.assert_not_awaited()

    async def test_enable_again_in_same_channel_refreshes_idle_timeout(self):
        await self.session.ensure_connected(self.channel)
        self.expire_activity()
        await self.session.ensure_connected(self.channel)
        await self.idle_tick()
        self.vc.disconnect.assert_not_awaited()
        self.channel.connect.assert_awaited_once()

    async def test_move_refreshes_idle_timeout(self):
        await self.session.ensure_connected(self.channel)
        self.expire_activity()
        destination = Mock(id=789)
        await self.session.ensure_connected(destination)
        await self.idle_tick()
        self.vc.move_to.assert_awaited_once_with(destination)
        self.vc.disconnect.assert_not_awaited()

    async def test_actual_inactivity_still_disconnects(self):
        await self.session.ensure_connected(self.channel)
        self.expire_activity()
        await self.idle_tick()
        self.vc.disconnect.assert_awaited_once_with(force=True)
        self.assertIsNone(self.session.voice_client)

    async def test_idle_check_during_move_does_not_disconnect(self):
        await self.session.ensure_connected(self.channel)
        self.expire_activity()
        # A move yields to the event loop, so the existing idle task can run.
        async def move_with_idle_check(channel):
            await self.idle_tick()

        self.vc.move_to.side_effect = move_with_idle_check
        await self.session.ensure_connected(Mock(id=789))
        self.vc.disconnect.assert_not_awaited()

    async def test_wall_clock_jump_does_not_disconnect(self):
        await self.session.ensure_connected(self.channel)
        with patch.object(bot_narrate.time, "time", return_value=time.time() + 86400):
            await self.idle_tick()
        self.vc.disconnect.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
