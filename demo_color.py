"""Dependency-free, TTY-gated semantic ANSI color for the terminal demos.

Color is enabled only when the real stdout is an interactive terminal and
NO_COLOR is unset. The enabled flag is computed ONCE at import from the real
``sys.stdout`` so it is independent of per-call stdout redirection (tests run
non-tty and therefore see plain text, byte-for-byte as before). Every wrapper
is a no-op that returns its input unchanged when color is disabled, so no
escape bytes ever leak into piped/redirected output or log files.
"""
import os
import sys

# Raw SGR codes.
RESET = '\033[0m'
BOLD = '\033[1m'
DIM = '\033[2m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
RED = '\033[31m'
BRIGHT_YELLOW = '\033[93m'

# p99 thresholds (ms): shared by both scripts.
P99_GREEN_MAX = 60
P99_YELLOW_MAX = 120

# Computed ONCE at import from the real stdout. NO_COLOR (any value) disables.
ENABLED = bool(sys.stdout.isatty()) and not os.environ.get("NO_COLOR")


def use_color():
    """Return whether color is enabled (decided once at import)."""
    return ENABLED


def paint(text, *codes):
    """Wrap ``text`` in the given SGR ``codes``; no-op when color disabled.

    When disabled (or given no codes) the input is returned unchanged, adding
    zero bytes so callers that padded to a fixed width keep that exact width.
    """
    if not ENABLED or not codes:
        return text
    return ''.join(codes) + text + RESET


def green(text):
    return paint(text, GREEN)


def yellow(text):
    return paint(text, YELLOW)


def red(text):
    return paint(text, RED)


def bright_yellow(text):
    return paint(text, BRIGHT_YELLOW)


def bold(text):
    return paint(text, BOLD)


def dim(text):
    return paint(text, DIM)


def p99_color(value):
    """Return the SGR code for a p99 value, or ``None`` for no color.

    ``None``/missing -> ``None`` (never color the dash); ``value <= 60`` ->
    green; ``60 < value <= 120`` -> yellow; ``value > 120`` -> red.
    """
    if value is None:
        return None
    if value <= P99_GREEN_MAX:
        return GREEN
    if value <= P99_YELLOW_MAX:
        return YELLOW
    return RED


def p99(text, value):
    """Wrap already-formatted, already-padded ``text`` with its p99 color.

    Padding is the caller's job: ``text`` must already be the fixed-width
    field. Returns ``text`` unchanged when the value has no color (dash/None)
    or when color is disabled, so field width is preserved.
    """
    code = p99_color(value)
    if code is None:
        return text
    return paint(text, code)
