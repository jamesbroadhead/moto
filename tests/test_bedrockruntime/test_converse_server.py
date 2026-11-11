import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
import requests

from moto.server import ThreadedMotoServer


class Responder(BaseHTTPRequestHandler):
    requests: list = []

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        Responder.requests.append(request)
        text = request["messages"][-1]["content"][0]["text"].upper()
        body = json.dumps(
            {"output": {"message": {"role": "assistant", "content": [{"text": text}]}}}
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def test_server_mode_forwards_requests_to_the_configured_responder():
    responder = ThreadingHTTPServer(("127.0.0.1", 0), Responder)
    threading.Thread(target=responder.serve_forever, daemon=True).start()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    moto = f"http://{host}:{port}"
    try:
        config = requests.post(
            f"{moto}/moto-api/config",
            json={
                "bedrock-runtime": {
                    "converse_responder": f"http://127.0.0.1:{responder.server_port}"
                }
            },
        ).json()
        assert config["bedrock-runtime"]["converse_responder"].startswith("http://")
        client = boto3.client(
            "bedrock-runtime",
            region_name="eu-central-1",
            endpoint_url=moto,
            aws_access_key_id="ak",
            aws_secret_access_key="sk",
        )
        messages = [{"role": "user", "content": [{"text": "hello"}]}]
        system = [{"text": "Be loud."}]
        response = client.converse(modelId="m1", messages=messages, system=system)
        assert response["output"]["message"]["content"] == [{"text": "HELLO"}]
        assert response["stopReason"] == "end_turn"
        assert Responder.requests[-1] == {
            "modelId": "m1",
            "messages": messages,
            "system": system,
        }
        stream = client.converse_stream(modelId="m1", messages=messages)
        deltas = [
            e["contentBlockDelta"] for e in stream["stream"] if "contentBlockDelta" in e
        ]
        assert deltas == [{"contentBlockIndex": 0, "delta": {"text": "HELLO"}}]
    finally:
        requests.post(
            f"{moto}/moto-api/config",
            json={"bedrock-runtime": {"converse_responder": None}},
        )
        server.stop()
        responder.shutdown()
