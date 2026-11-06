import socket
import threading
import json
import time
import os
import secrets
import base64
import hashlib
import re
import uuid
from pathlib import Path
from urllib.parse import urlparse

# The dashboard reads this live state, which the TCP messaging server updates.
state_lock = threading.RLock()
app_state = {
    "users": [],
    "messages": [],
    "files": {}
}
web_sessions = {}  # token -> username
web_sessions_lock = threading.RLock()
web_login_handler = None
web_send_handler = None
web_logout_handler = None
MAX_WEB_FILE_SIZE = 8 * 1024 * 1024
WEB_UPLOAD_DIR = Path(__file__).resolve().parent.parent / "web_uploads"
web_files = {}
web_files_lock = threading.RLock()


def configure_web_api(login_handler, send_handler, logout_handler=None):
    global web_login_handler, web_send_handler, web_logout_handler
    web_login_handler = login_handler
    web_send_handler = send_handler
    web_logout_handler = logout_handler


def _response(client_socket, status, payload, content_type="application/json; charset=utf-8"):
    if isinstance(payload, bytes):
        body = payload
    elif content_type.startswith("application/json"):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    else:
        body = str(payload).encode("utf-8")
    reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 409: "Conflict", 500: "Internal Server Error"}.get(status, "Error")
    headers = (
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Cache-Control: no-store\r\n"
        "Connection: close\r\n\r\n"
    )
    client_socket.sendall(headers.encode("ascii") + body)


def _read_http_request(client_socket):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        part = client_socket.recv(4096)
        if not part:
            raise ValueError("Incomplete HTTP request")
        data.extend(part)
        if len(data) > 65536:
            raise ValueError("HTTP headers too large")
    header_bytes, body = bytes(data).split(b"\r\n\r\n", 1)
    lines = header_bytes.decode("iso-8859-1").split("\r\n")
    parts = lines[0].split()
    if len(parts) < 2:
        raise ValueError("Malformed HTTP request line")
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()
    content_length = int(headers.get("content-length", "0"))
    if content_length > 12 * 1024 * 1024:
        raise ValueError("HTTP body too large")
    while len(body) < content_length:
        part = client_socket.recv(min(4096, content_length - len(body)))
        if not part:
            raise ValueError("Incomplete HTTP body")
        body += part
    return parts[0].upper(), parts[1], headers, body[:content_length]

def handle_http_request(client_socket, client_address):
    # Step 7: Network Resilience - Set timeout to prevent resource starvation
    client_socket.settimeout(5.0) 
    
    try:
        method, raw_path, headers, body = _read_http_request(client_socket)
        path = urlparse(raw_path).path
        
        # Routing raw HTTP
        if method == "GET" and path == '/':
            ui_path = os.path.join(os.path.dirname(__file__), '..', 'ui', 'index.html')
            with open(ui_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            encoded_html = html_content.encode('utf-8')
            response_headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(encoded_html)}\r\n"
                "Connection: close\r\n\r\n"
            )
            client_socket.sendall(response_headers.encode('utf-8') + encoded_html)
            
        elif method == "GET" and path == '/api/network_state':
            token = headers.get("authorization", "")
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            with web_sessions_lock:
                viewer = web_sessions.get(token)
            if token and not viewer:
                _response(client_socket, 401, {"error": "Session expired. Please sign in again."})
                return
            with state_lock:
                state = dict(app_state)
                state["messages"] = [
                    msg for msg in app_state["messages"]
                    if not msg.get("is_private") or viewer in {msg.get("sender"), msg.get("to")}
                ]
                json_str = json.dumps(state, ensure_ascii=False)
            encoded_json = json_str.encode('utf-8')
            response_headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json; charset=utf-8\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                f"Content-Length: {len(encoded_json)}\r\n"
                "Connection: close\r\n\r\n"
            )
            client_socket.sendall(response_headers.encode('utf-8') + encoded_json)
        elif method == "POST" and path == "/api/login":
            data = json.loads(body.decode("utf-8"))
            name = str(data.get("name", "")).strip()
            if not name or len(name) > 32:
                _response(client_socket, 400, {"error": "Enter a name between 1 and 32 characters."})
                return
            if web_login_handler is None:
                _response(client_socket, 500, {"error": "Web login is not configured."})
                return
            ok, result = web_login_handler(name, client_address)
            if not ok:
                _response(client_socket, 409, {"error": result})
                return
            token = secrets.token_urlsafe(32)
            with web_sessions_lock:
                web_sessions[token] = result
            _response(client_socket, 200, {"name": result, "token": token})
        elif method == "POST" and path == "/api/send":
            token = headers.get("authorization", "")
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            with web_sessions_lock:
                sender = web_sessions.get(token)
            if not sender:
                _response(client_socket, 401, {"error": "Log in before sending messages."})
                return
            data = json.loads(body.decode("utf-8"))
            kind = data.get("type")
            text = str(data.get("text", "")).strip()
            if not text or len(text) > 16000:
                _response(client_socket, 400, {"error": "Message must contain 1 to 16000 characters."})
                return
            if kind not in {"MSG_BROADCAST", "MSG_UNICAST"}:
                _response(client_socket, 400, {"error": "Unsupported message type."})
                return
            packet = {"type": kind, "text": text}
            if kind == "MSG_UNICAST":
                packet["to"] = str(data.get("to", "")).strip()
                if not packet["to"]:
                    _response(client_socket, 400, {"error": "Choose a recipient."})
                    return
            if web_send_handler is None:
                _response(client_socket, 500, {"error": "Web messaging is not configured."})
                return
            ok, error = web_send_handler(sender, packet)
            if not ok:
                _response(client_socket, 400, {"error": error})
                return
            _response(client_socket, 200, {"ok": True})
        elif method == "POST" and path == "/api/logout":
            token = headers.get("authorization", "")
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            with web_sessions_lock:
                username = web_sessions.pop(token, None)
            if not username:
                _response(client_socket, 401, {"error": "Session is already signed out."})
                return
            if web_logout_handler:
                web_logout_handler(username)
            _response(client_socket, 200, {"ok": True})
        elif method == "POST" and path == "/api/upload":
            token = headers.get("authorization", "")
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            with web_sessions_lock:
                sender = web_sessions.get(token)
            if not sender:
                _response(client_socket, 401, {"error": "Sign in before uploading files."})
                return
            data = json.loads(body.decode("utf-8"))
            filename = os.path.basename(str(data.get("name", "upload.bin")))
            recipient = str(data.get("to", "")).strip()
            encoded = data.get("data", "")
            if not recipient or not encoded:
                _response(client_socket, 400, {"error": "Choose a recipient and a file."})
                return
            try:
                raw = base64.b64decode(encoded, validate=True)
            except (ValueError, TypeError):
                _response(client_socket, 400, {"error": "Invalid upload data."})
                return
            if not raw or len(raw) > MAX_WEB_FILE_SIZE:
                _response(client_socket, 400, {"error": "Web uploads must be between 1 byte and 8 MiB."})
                return
            if web_send_handler is None:
                _response(client_socket, 500, {"error": "Web messaging is not configured."})
                return
            file_id = uuid.uuid4().hex
            digest = hashlib.sha256(raw).hexdigest()
            WEB_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            file_path = WEB_UPLOAD_DIR / file_id
            file_path.write_bytes(raw)
            with web_files_lock:
                web_files[file_id] = {
                    "path": file_path, "name": filename, "sender": sender,
                    "to": recipient, "size": len(raw), "sha256": digest,
                }
            ok, error = web_send_handler(sender, {
                "type": "WEB_FILE", "file_id": file_id, "name": filename,
                "to": recipient, "size": len(raw), "sha256": digest,
            })
            if not ok:
                with web_files_lock:
                    web_files.pop(file_id, None)
                file_path.unlink(missing_ok=True)
                _response(client_socket, 400, {"error": error})
                return
            _response(client_socket, 200, {"ok": True, "file_id": file_id})
        elif method == "GET" and path.startswith("/api/download/"):
            token = headers.get("authorization", "")
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            with web_sessions_lock:
                viewer = web_sessions.get(token)
            file_id = path.rsplit("/", 1)[-1]
            with web_files_lock:
                record = web_files.get(file_id)
            if not viewer or not record or viewer not in {record["sender"], record["to"]}:
                _response(client_socket, 404, {"error": "File not found."})
                return
            payload = record["path"].read_bytes()
            safe_name = re.sub(r"[^A-Za-z0-9._ -]", "_", record["name"])
            response_headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/octet-stream\r\n"
                f"Content-Length: {len(payload)}\r\n"
                f'Content-Disposition: attachment; filename="{safe_name}"\r\n'
                "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
            )
            client_socket.sendall(response_headers.encode("ascii") + payload)
        else:
            _response(client_socket, 404, {"error": "Not found"})

    except (socket.timeout, TimeoutError):
        # Step 7: Gracefully handle slow or dead clients without crashing
        print(f"[{client_address}] HTTP Request timed out.")
    except (ConnectionResetError, BrokenPipeError):
        # Step 7: Handle sudden client disconnects (e.g., closing browser tab abruptly)
        print(f"[{client_address}] Connection dropped abruptly by client.")
    except (ValueError, json.JSONDecodeError) as e:
        try:
            _response(client_socket, 400, {"error": str(e)})
        except OSError:
            pass
    except Exception as e:
        print(f"[{client_address}] Server exception: {e}")
    finally:
        client_socket.close()

def start_resilient_server(host='127.0.0.1', port=8081):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind((host, port))
        server.listen(20) # Scalability: handle 10-20 concurrent clients (Requirement 7)
        print(f"Resilient UI Server running at http://{host}:{port}/")
        
        while True:
            client_sock, addr = server.accept()
            # Step 4: Multi-threading to avoid blocking main thread
            thread = threading.Thread(target=handle_http_request, args=(client_sock, addr))
            thread.daemon = True
            thread.start()
            
    except KeyboardInterrupt:
        print("\nShutting down resilient server...")
    finally:
        server.close()

if __name__ == "__main__":
    start_resilient_server()
