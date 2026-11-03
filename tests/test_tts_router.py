import asyncio
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
        self.gemini.synth = AsyncMock(return_value=(WAV_1S, "gemini-3.8-flash-tts"))
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

    async def test_gemini_out_of_quota_falls_back_to_google_without_standin(self):
        self.gemini.synth.side_effect = RateLimited("gemini", 429, "slow down")
        res = await self.router.synth("gemini:kore", "hello")
        self.assertEqual((res.provider, res.fallback_reason), ("google", "gemini_limit"))

    async def test_gemini_out_of_quota_uses_fish_standin(self):
        self.gemini.synth.side_effect = RateLimited("gemini", 429, "all models at their limits")
        router = TTSRouter(self.google, self.fish, self.gemini, self.budget, fish_standin_voice=FISH_ID)
        res = await router.synth("gemini:kore", "hello", style="calm")
        self.assertEqual((res.provider, res.fallback_reason), ("fish", "gemini_limit"))
        self.fish.synth.assert_awaited_once_with("[calm] hello", FISH_ID, speed=1.0)
        self.google.synth.assert_not_awaited()
        self.assertGreater(res.cost_usd, 0)

    async def test_fish_rate_limit_falls_back_to_google(self):
        self.fish.synth.side_effect = RateLimited("fish", 429, "slow down")
        res = await self.router.synth(f"fish:{FISH_ID}", "hello")
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
        first = await self.router.synth(f"fish:{FISH_ID}", "same thing")
        again = await self.router.synth(f"fish:{FISH_ID}", "same thing")
        self.assertEqual(self.fish.synth.await_count, 1)
        self.assertGreater(first.cost_usd, 0)
        self.assertEqual(again.cost_usd, 0)  # cache hits are free

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

    def test_chunk_plans(self):
        self.assertEqual(self.router.chunk_plan("en-US-Wavenet-D"), (150, 900))
        self.assertEqual(self.router.chunk_plan(f"fish:{FISH_ID}"), (90, 600))


class FlakyDb:
    def __init__(self):
        self.down = True
        self.writes = []

    async def fetch(self, sql, *a):
        if self.down:
            raise ConnectionError("db down")
        return [{"provider": "fish", "est_usd": 9.0}]

    async def execute(self, sql, *a):
        if self.down:
            raise ConnectionError("db down")
        self.writes.append(a)


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_unreadable_db_fails_closed(self):
        db = FlakyDb()
        budget = BudgetTracker(lambda: db, monthly_cap_usd=10.0)
        self.assertTrue(await budget.exhausted())
        google = Mock(); google.synth = AsyncMock(return_value=OGG)
        fish = Mock(model="s2.1-pro"); fish.synth = AsyncMock(return_value=OGG)
        res = await TTSRouter(google, fish, None, budget).synth(f"fish:{FISH_ID}", "hello")
        self.assertEqual(res.fallback_reason, "budget")

    async def test_spend_during_outage_is_counted_and_written_later(self):
        db = FlakyDb()
        budget = BudgetTracker(lambda: db, monthly_cap_usd=10.0)
        await budget.record("fish", 0.5, chars=10)     # DB down: kept for later
        db.down = False
        budget._load_failed_at = None
        self.assertAlmostEqual(await budget.spent(), 9.5)   # 9.0 in DB + 0.5 unsaved
        await budget.record("fish", 0.25, chars=5)
        self.assertEqual(len(db.writes), 1)
        self.assertAlmostEqual(float(db.writes[0][-1]), 0.75)  # both increments in one write
        self.assertAlmostEqual(await budget.spent(), 9.75)


class CacheTests(unittest.TestCase):
    def test_byte_cap_evicts_oldest(self):
        c = tts_providers.LRUCache(100, max_bytes=10)
        c.put("a", b"12345")
        c.put("b", b"12345")
        c.put("c", b"12345")
        self.assertIsNone(c.get("a"))
        self.assertEqual(c.bytes, 10)
        c.put("huge", b"x" * 11)  # bigger than the whole cache: not cached
        self.assertIsNone(c.get("huge"))


class GeminiChainTests(unittest.IsolatedAsyncioTestCase):
    def make(self):
        g = tts_providers.GeminiTTSProvider("k", "m1", rpm=100, fallback_models=("m2", "m3"))
        g._synth_one = AsyncMock()
        return g

    async def test_daily_limit_moves_to_next_model_and_remembers(self):
        g = self.make()
        g._synth_one.side_effect = [RateLimited("gemini", 429, "limit: 100 requests per day. Please retry in 11h8m36s"),
                                    WAV_1S, WAV_1S]
        self.assertEqual(await g.synth("hi", "kore"), (WAV_1S, "m2"))
        self.assertGreater(g.blocked_for("m1"), 11 * 3600)
        self.assertEqual(await g.synth("hi", "kore"), (WAV_1S, "m2"))  # m1 skipped, not retried
        self.assertEqual([c.args[0] for c in g._synth_one.await_args_list], ["m1", "m2", "m2"])

    async def test_all_models_out_raises_rate_limited(self):
        g = self.make()
        g._synth_one.side_effect = RateLimited("gemini", 429, "Please retry in 2h")
        with self.assertRaises(RateLimited):
            await g.synth("hi", "kore")
        with self.assertRaises(RateLimited):  # now refused without any request
            await g.synth("hi", "kore")
        self.assertEqual(g._synth_one.await_count, 3)

    async def test_other_errors_try_next_model(self):
        g = self.make()
        g._synth_one.side_effect = [ProviderError("gemini", 400, "No matching speaker voice"), WAV_1S]
        self.assertEqual(await g.synth("hi", "storyteller-2"), (WAV_1S, "m2"))

    def test_retry_after_parsing(self):
        self.assertEqual(tts_providers.retry_after_secs("Please retry in 11h8m36s or upgrade"), 40116)
        self.assertEqual(tts_providers.retry_after_secs("Please retry in 5s."), 5)
        self.assertIsNone(tts_providers.retry_after_secs("no hint"))

    def test_raw_pcm_is_wrapped_as_wav(self):
        wav = tts_providers.pcm_to_wav(b"\x00\x00" * 24000)
        self.assertEqual(tts_providers.audio_format(wav), "wav")
        self.assertAlmostEqual(tts_providers.wav_duration(wav), 1.0)

    def test_older_models_are_priced(self):
        for m in tts_providers.DEFAULT_GEMINI_FALLBACK_MODELS:
            self.assertIn(m, tts_providers.GEMINI_USD_PER_M_TOKENS)


class RpmLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_gives_up_instead_of_waiting_too_long(self):
        lim = tts_providers.RpmLimiter(2)
        self.assertTrue(await lim.acquire(max_wait=0))
        self.assertTrue(await lim.acquire(max_wait=0))
        self.assertFalse(await lim.acquire(max_wait=1))

    async def test_concurrent_waiters_respect_deadline(self):
        lim = tts_providers.RpmLimiter(1)
        await lim.acquire(max_wait=0)
        t = asyncio.get_running_loop().time()
        results = await asyncio.gather(lim.acquire(max_wait=0.2), lim.acquire(max_wait=0.2))
        self.assertEqual(results, [False, False])
        self.assertLess(asyncio.get_running_loop().time() - t, 0.5)


if __name__ == "__main__":
    unittest.main()
