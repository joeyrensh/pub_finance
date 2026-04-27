from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.paths import FINANCE_ROOT
from finance.utility.get_proxy import ProxyManager

pm = ProxyManager(proxy_file_path=FINANCE_ROOT / "utility" / "test.txt")
pm.save_working_proxies_to_file(
    output_file=FINANCE_ROOT / "utility" / "test_output.txt"
)
