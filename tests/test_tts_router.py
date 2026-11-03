import unittest
from datetime import date
from unittest.mock import AsyncMock, Mock

import tts_providers
from tts_providers import (
    BudgetTracker, ProviderError, RateLimited, TTSRouter, extract_fish_id, gemini_cost, parse_voice_ref,
)

FISH_ID = "8ec9973300874562962bbcb300f82213"
OGG = b"OggS" + b"\x00" * 20
WAV_1S = b"RIFF" + b"\x00" * 40  # format sniffing only; durations are patched below


class VoiceRefTests(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_voice_ref(f"fish:{FISH_ID.upper()}"), ("fish", FISH_ID))
        self.assertEqual(parse_voice_ref("gemini:Kore"), ("gemini", "kore"))
        self.assertEqual(parse_voice_ref("en-US-Chirp3-HD-Kore"), ("google", "en-US-Chirp3-HD-Kore"))
        self.assertEqual(parse_voice_ref(None), ("google", tts_providers.DEFAULT_GOOGLE_VOICE))

    def test_extract_fish_id(self):
        self.assertEqual(extract_fish_id(f"https://fish.audio/m/{FISH_ID}/"), FISH_ID)
        self.assertEqual(extract_fish_id(FISH_ID.upper()), FISH_ID)
        self.assertIsNone(extract_fish_id("https://fish.audio/voice-library/"))

    def test_gemini_price_doubles_in_2027(self):
        now = gemini_cost("hi", None, 10, "gemini-3.8-flash-tts", today=date(2026, 12, 31))
        later = gemini_cost("hi", None, 10, "gemini-3.8-flash-tts", today=date(2027, 1, 1))
        self.assertAlmostEqual(later, now * 2, places=9)


class RouterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.google = Mock()
        self.google.synth = AsyncMock(return_value=OGG)
        self.fish = Mock(model="s2.1-pro")
        self.fish.synth = AsyncMock(return_value=OGG)
        self.gemini = Mock(model="gemini-3.8-flash-tts")
        self.gemini.synth = AsyncMock(return_value=WAV_1S)
        self.budget = BudgetTracker(lambda: None, monthly_cap_usd=10.0)
        self.router = TTSRouter(self.google, self.fish, self.gemini, self.budget)

    async def test_fish_voice_uses_fish_with_inline_style(self):
        res = await self.router.synth(f"fish:{FISH_ID}", "brb", style="excited", rate=1.2)
        self.assertEqual((res.provider, res.fmt, res.fallback_reason), ("fish", "ogg_opus", None))
        self.fish.synth.assert_awaited_once_with("[excited] be right back", FISH_ID, speed=1.2)
        self.google.synth.assert_not_awaited()
        self.assertGreater(await self.budget.spent(), 0)

    async def test_gemini_voice_sends_style_separately(self):
        res = await self.router.synth("gemini:kore", "ok [laugh]", style="dry")
        self.assertEqual((res.provider, res.fmt), ("gemini", "wav"))
        self.gemini.synth.assert_awaited_once_with("ok <laugh>", "kore", "dry")

    async def test_google_voice_strips_tags(self):
        res = await self.router.synth("en-US-Wavenet-D", "[whisper] hi [laugh]", style="sad")
        self.assertEqual((res.provider, res.fallback_reason), ("google", None))
        self.google.synth.assert_awaited_once()
        self.assertEqual(self.google.synth.await_args.args[0], "hi")

    async def test_budget_exhausted_falls_back_to_google(self):
        await self.budget.record("fish", 10.0)
        res = await self.router.synth(f"fish:{FISH_ID}", "hello")
        self.assertEqual((res.provider, res.fallback_reason), ("google", "budget"))
        self.fish.synth.assert_not_awaited()
        self.assertEqual(self.google.synth.await_args.kwargs["voice_name"], tts_providers.DEFAULT_GOOGLE_VOICE)

    async def test_rate_limit_falls_back(self):
        self.gemini.synth.side_effect = RateLimited("gemini", 429, "slow down")
        res = await self.router.synth("gemini:kore", "hello")
        self.assertEqual((res.provider, res.fallback_reason), ("google", "rate_limited"))

    async def test_out_of_credit_is_reported(self):
        self.fish.synth.side_effect = ProviderError("fish", 402, "insufficient balance")
        res = await self.router.synth(f"fish:{FISH_ID}", "hello")
        self.assertEqual(res.fallback_reason, "balance")

    async def test_unconfigured_provider_falls_back(self):
        router = TTSRouter(self.google, None, None, self.budget)
        res = await router.synth("gemini:kore", "hello")
        self.assertEqual((res.provider, res.fallback_reason), ("google", "not_configured"))

    async def test_repeat_message_is_cached(self):
        await self.router.synth(f"fish:{FISH_ID}", "same thing")
        await self.router.synth(f"fish:{FISH_ID}", "same thing")
        self.assertEqual(self.fish.synth.await_count, 1)

    async def test_designed_voice_sends_stored_sample_inline(self):
        loader = AsyncMock(return_value=(OGG, "sample transcript"))
        router = TTSRouter(self.google, self.fish, self.gemini, self.budget, reference_loader=loader)
        res = await router.synth("fish:ref:abc123", "hello there")
        self.assertEqual((res.provider, res.fallback_reason), ("fish", None))
        self.fish.synth.assert_awaited_once_with("hello there", None, speed=1.0, reference=(OGG, "sample transcript"))
        await router.synth("fish:ref:abc123", "second message")
        loader.assert_awaited_once_with("abc123")  # sample is loaded once, then kept in memory

    async def test_missing_designed_sample_falls_back(self):
        router = TTSRouter(self.google, self.fish, self.gemini, self.budget, reference_loader=AsyncMock(return_value=None))
        res = await router.synth("fish:ref:gone", "hello")
        self.assertEqual((res.provider, res.fallback_reason), ("google", "error"))

    def test_chunk_limits(self):
        self.assertEqual(self.router.chunk_limit("en-US-Wavenet-D"), 180)
        self.assertEqual(self.router.chunk_limit(f"fish:{FISH_ID}"), 400)


class RpmLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_gives_up_instead_of_waiting_too_long(self):
        lim = tts_providers.RpmLimiter(2)
        self.assertTrue(await lim.acquire(max_wait=0))
        self.assertTrue(await lim.acquire(max_wait=0))
        self.assertFalse(await lim.acquire(max_wait=1))


if __name__ == "__main__":
    unittest.main()
