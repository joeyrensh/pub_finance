import sys
import time
import functools
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from finance import FINANCE_ROOT
from finance.utility.emcookie_generation import CookieGeneration

g = CookieGeneration()
g.generate_em_cookies()
