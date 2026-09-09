import multiprocessing

# 针对 Linux 下 Python 多进程启动机制的兼容适配
try:
    if multiprocessing.get_start_method(allow_none=True) is None:
        multiprocessing.set_start_method("fork")
except (RuntimeError, ValueError):
    pass

from waitress import serve
from finance.dashreport.app import app

if __name__ == "__main__":
    serve(
        app.server,
        host="0.0.0.0",
        port=80,
        threads=2,
        max_request_header_size=8192,
        max_request_body_size=10485760,
    )
