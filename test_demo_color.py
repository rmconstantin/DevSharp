"""Semantics of the TTY-gated color helper, forcing the flag both ways."""
import re
import unittest

import demo_color

ANSI = re.compile(r'\x1b\[[0-9;]*m')


def strip_ansi(text):
    return ANSI.sub('', text)


class ColorDisabledTests(unittest.TestCase):
    def setUp(self):
        self._saved = demo_color.ENABLED
        demo_color.ENABLED = False

    def tearDown(self):
        demo_color.ENABLED = self._saved

    def test_wrappers_return_input_unchanged(self):
        for wrap in (demo_color.green, demo_color.yellow, demo_color.red,
                     demo_color.bright_yellow, demo_color.bold, demo_color.dim):
            self.assertEqual(wrap('payload'), 'payload')
            self.assertNotIn('\033[', wrap('payload'))

    def test_p99_returns_input_unchanged_and_width_preserved(self):
        field = f'{42.0:7.1f}'
        self.assertEqual(demo_color.p99(field, 42.0), field)
        self.assertEqual(len(demo_color.p99(field, 42.0)), len(field))
        self.assertEqual(demo_color.use_color(), False)


class ColorEnabledTests(unittest.TestCase):
    def setUp(self):
        self._saved = demo_color.ENABLED
        demo_color.ENABLED = True

    def tearDown(self):
        demo_color.ENABLED = self._saved

    def test_wrappers_bracket_with_escapes_and_preserve_display_text(self):
        for wrap in (demo_color.green, demo_color.yellow, demo_color.red,
                     demo_color.bright_yellow, demo_color.bold, demo_color.dim):
            result = wrap('payload')
            self.assertTrue(result.startswith('\033['))
            self.assertTrue(result.endswith('\033[0m'))
            self.assertEqual(strip_ansi(result), 'payload')

    def test_p99_wraps_and_strip_restores_padded_field(self):
        field = f'{42.0:7.1f}'
        result = demo_color.p99(field, 42.0)
        self.assertTrue(result.startswith('\033['))
        self.assertEqual(strip_ansi(result), field)


class ThresholdTests(unittest.TestCase):
    def test_p99_color_boundaries(self):
        self.assertIsNone(demo_color.p99_color(None))
        self.assertEqual(demo_color.p99_color(0.0), demo_color.GREEN)
        self.assertEqual(demo_color.p99_color(60), demo_color.GREEN)
        self.assertEqual(demo_color.p99_color(60.1), demo_color.YELLOW)
        self.assertEqual(demo_color.p99_color(120), demo_color.YELLOW)
        self.assertEqual(demo_color.p99_color(120.1), demo_color.RED)

    def test_p99_none_never_colored_even_when_enabled(self):
        saved = demo_color.ENABLED
        demo_color.ENABLED = True
        try:
            dash = '      -'
            self.assertEqual(demo_color.p99(dash, None), dash)
        finally:
            demo_color.ENABLED = saved


class GateExpressionTests(unittest.TestCase):
    """The enabled flag = stdout.isatty() and not NO_COLOR; recompute it."""
    def _compute(self, isatty, no_color_env):
        # Mirrors demo_color.ENABLED: sys.stdout.isatty() and not os.environ.get("NO_COLOR").
        return bool(isatty) and not no_color_env

    def test_tty_without_no_color_enables(self):
        # os.environ.get("NO_COLOR") returns None when unset.
        self.assertTrue(self._compute(True, None))

    def test_non_tty_disables(self):
        self.assertFalse(self._compute(False, None))
        self.assertFalse(self._compute(False, '1'))

    def test_no_color_set_disables_even_on_tty(self):
        self.assertFalse(self._compute(True, '1'))


if __name__ == '__main__':
    unittest.main()
