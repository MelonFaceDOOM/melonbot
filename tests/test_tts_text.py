import unittest

from tts_text import expand_slang, map_inline_tags, prepare, split_leading_style


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


if __name__ == "__main__":
    unittest.main()
