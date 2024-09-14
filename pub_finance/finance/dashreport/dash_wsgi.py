from waitress import serve
from app import app

serve(
    app.server,
    host="0.0.0.0",
    port=80,
    threads=4,
    max_request_header_size=8192,
    max_request_body_size=10485760,
)
