#!/usr/bin/env python3
from __future__ import annotations

import sys

from nifty_span.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["span-groww-margin-parity", *sys.argv[1:]]))
