from waitress import serve
from finance.dashreport.app_int import app

serve(
    app.server,
    host="0.0.0.0",
    port=80,
    threads=1,
    max_request_header_size=8192,
    max_request_body_size=10485760,
)
