import unittest

from tts_text import chunk_for_speech, expand_slang, map_inline_tags, prepare, split_leading_style


class SplitLeadingStyleTests(unittest.TestCase):
    def test_leading_bracket_becomes_style(self):
        self.assertEqual(split_leading_style("[whispering] don't tell anyone"), ("whispering", "don't tell anyone"))

    def test_stacked_cues_are_joined(self):
        self.assertEqual(split_leading_style("[sad][very quiet]  oh no"), ("sad, very quiet", "oh no"))

    def test_leading_event_tag_stays_inline(self):
        self.assertEqual(split_leading_style("[laugh] okay that was good"), (None, "[laugh] okay that was good"))

    def test_no_style(self):
        self.assertEqual(split_leading_style("plain message [sigh] here"), (None, "plain message [sigh] here"))

    def test_style_only_message_leaves_nothing_to_say(self):
        self.assertEqual(split_leading_style("[excited]"), ("excited", ""))


class InlineTagTests(unittest.TestCase):
    def test_gemini_maps_events_and_drops_unknown(self):
        self.assertEqual(map_inline_tags("ha [laugh] ok [mysterious] bye [long pause] end", "gemini"),
                         "ha <laugh> ok bye <long pause> end")

    def test_fish_maps_events_and_keeps_unknown(self):
        self.assertEqual(map_inline_tags("ha [Laughs] ok [mysterious] [pause] end", "fish"),
                         "ha [laughing] ok [mysterious] [break] end")

    def test_google_strips_all(self):
        self.assertEqual(map_inline_tags("ha [laugh] ok [mysterious] end", "google"), "ha ok end")


class SlangTests(unittest.TestCase):
    def test_expands_standalone_abbreviations(self):
        self.assertEqual(expand_slang("brb, gg wp lmao"), "be right back, G G well played L M A O")

    def test_case_insensitive_and_word_bounded(self):
        self.assertEqual(expand_slang("IDK about the gguf file or brbs"), "I don't know about the gguf file or brbs")

    def test_leaves_contractions_alone(self):
        self.assertEqual(expand_slang("they'rn't fr"), "they'rn't for real")


class PrepareTests(unittest.TestCase):
    def test_gemini_keeps_style_separate(self):
        self.assertEqual(prepare("idk [sigh] fine", "gemini", "tired"), ("I don't know <sigh> fine", "tired"))

    def test_fish_puts_style_inline(self):
        self.assertEqual(prepare("idk [sigh] fine", "fish", "tired"), ("[tired] I don't know [sighing] fine", None))

    def test_google_ignores_style_and_slang(self):
        self.assertEqual(prepare("idk [sigh] fine", "google", "tired"), ("idk fine", None))


RANT = ("I swear this game hates me. I queued for forty minutes, finally got in, and the first thing that happens is "
        "my teammate disconnects. Then the other one starts arguing with the enemy team in all chat instead of playing. "
        "We lost in like eight minutes. Eight! I'm not even mad about losing, I'm mad that I waited forty minutes for that. "
        "Anyway I'm taking a break, someone ping me if you want to do something chill later.")


class ChunkForSpeechTests(unittest.TestCase):
    def test_short_text_is_one_chunk(self):
        self.assertEqual(chunk_for_speech("hey all", 90, 600), ["hey all"])
        self.assertEqual(chunk_for_speech("", 90, 600), [])

    def test_long_first_word_never_yields_an_empty_chunk(self):
        chunks = chunk_for_speech("a" * 200 + " then words", 90, 600)
        self.assertTrue(all(chunks))
        self.assertTrue(all(len(c) <= 600 for c in chunks))
        self.assertLessEqual(len(chunks[0]), 90)

    def test_keeps_every_word_in_order(self):
        chunks = chunk_for_speech(RANT, 90, 600)
        self.assertEqual(" ".join(chunks).split(), RANT.split())

    def test_small_first_chunk_then_growth_within_cap(self):
        chunks = chunk_for_speech(RANT, 90, 600)
        self.assertLessEqual(len(chunks[0]), 90)
        self.assertGreater(len(chunks[1]), len(chunks[0]))
        self.assertTrue(all(len(c) <= 600 for c in chunks))

    def test_breaks_land_on_sentences_or_clauses(self):
        for first, cap in ((90, 600), (150, 900), (220, 1200)):
            for c in chunk_for_speech(RANT, first, cap)[:-1]:
                self.assertRegex(c, r"[.!?,;:]$")

    def test_run_on_breaks_before_conjunctions(self):
        run_on = ("so basically we were halfway through the dungeon and sam pulls the entire room because he thought "
                  "the boss was dead and then everyone panics and jordan uses the only revive on the wrong person")
        chunks = chunk_for_speech(run_on, 90, 600)
        for c in chunks[1:]:
            self.assertIn(c.split()[0].lower(), {"and", "but", "so", "then", "because", "which", "when", "while", "until"})

    def test_tags_are_never_split(self):
        text = "okay so [very long dramatic pause] that happened, " * 6
        for c in chunk_for_speech(text, 40, 120):
            self.assertEqual(c.count("["), c.count("]"))

    def test_line_breaks_are_boundaries(self):
        self.assertEqual(chunk_for_speech("first line here\nsecond line here, which is a bit longer", 20, 600),
                         ["first line here", "second line here, which is a bit longer"])


if __name__ == "__main__":
    unittest.main()
