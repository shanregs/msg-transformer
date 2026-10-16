"""
A stand-in for the Reference Data Service, for running and debugging the engine locally.

    python samples/refdata-stub.py            # listens on http://127.0.0.1:8089
    python samples/refdata-stub.py 9000       # another port (then set sftr.refdata.base-url)

The engine POSTs to /ref-data/sftr with a JSON body such as
    {"bmoLegalEntity": "LDNBB01", "counterpartyId": "CP1", "group_cd": "G1", "delegated": "N"}
and this stub answers with the shape the mapping rules read (find_sftr_party.values.lei and so on).

The counterpartyId picks the behaviour, so every path of the engine can be tried:

    FAIL4XX    answers HTTP 400  -> the message is rejected, reason "Reference data HTTP 400" (never retried)
    FAIL5XX    answers HTTP 503  -> retried, then rejected with "Reference data unavailable"
    SLOW       waits 8 seconds   -> longer than the 5 second read timeout, so it is retried then rejected
    NONATURE   no nature field   -> CP_NatureOfRepCp falls back to the default F
    anything else                -> LEI = 549300 + the counterpartyId in capitals, padded with 0 to 14 characters
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        raw = self.read_body() or b"{}"
        try:
            request = json.loads(raw or b"{}")
        except ValueError:
            request = {}
        cp = str(request.get("counterpartyId", "UNKNOWN"))

        print("POST %s %s" % (self.path, raw.decode("utf-8", "replace")), flush=True)

        if cp == "FAIL4XX":
            return self.reply(400, {"error": "bad request"})
        if cp == "FAIL5XX":
            return self.reply(503, {"error": "unavailable"})
        if cp == "SLOW":
            time.sleep(8)

        party = {"values": {"lei": "549300" + cp.upper().ljust(14, "0")[:14]}}
        sector = {"values": {"sector": "CDTI"}}
        if cp != "NONATURE":
            sector["values"]["natureOfReportingCounterparty"] = "C"
        self.reply(200, {"find_sftr_party": party, "find_sftr_party_sector": sector})

    def read_body(self):
        """The request body, whether it has a Content-Length or is sent in chunks (the engine's HTTP client may)."""
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            data = b""
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    self.rfile.readline()
                    return data
                data += self.rfile.read(size)
                self.rfile.readline()
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def reply(self, status, body):
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8089
    print("Reference data stub on http://127.0.0.1:%d/ref-data/sftr (Ctrl+C to stop)" % port, flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
