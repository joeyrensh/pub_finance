from waitress import serve
from app import app

serve(
    app.server,
    host="0.0.0.0",
    port=8050,
    threads=8,
    max_request_header_size=8192,
    max_request_body_size=2147483648,
)
