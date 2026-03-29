import unittest

from line_webhook import _merge_user_state, _parse_line_text


class LineWebhookParsingTests(unittest.TestCase):
    def test_free_form_single_line(self):
        parsed = _parse_line_text("1986/2/23 7:25 宮城")
        self.assertEqual(parsed.get("birth_date"), "1986-02-23")
        self.assertEqual(parsed.get("birth_time"), "07:25")
        self.assertEqual(parsed.get("prefecture"), "宮城")

    def test_labeled_multiline_without_colon(self):
        parsed = _parse_line_text("生年月日 1986-02-23\n出生時間 7:25\n都道府県 宮城")
        self.assertEqual(parsed.get("birth_date"), "1986-02-23")
        self.assertEqual(parsed.get("birth_time"), "07:25")
        self.assertEqual(parsed.get("prefecture"), "宮城")

    def test_session_merge_keeps_previous_fields(self):
        session = {"birth_date": "1986-02-23", "birth_time": "07:25"}
        parsed = _parse_line_text("宮城")
        merged = _merge_user_state(session, parsed)
        self.assertEqual(merged.get("birth_date"), "1986-02-23")
        self.assertEqual(merged.get("birth_time"), "07:25")
        self.assertEqual(merged.get("prefecture"), "宮城")


if __name__ == "__main__":
    unittest.main()
