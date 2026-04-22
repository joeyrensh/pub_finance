from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.utility.get_proxy import ProxyManager

pm = ProxyManager()
pm.save_working_proxies_to_file()
