import os
import socket
import threading
import time

from reliable import ReliableSocket
from ui_server import start_resilient_server, app_state

os.environ.setdefault("APP_SECRET", "my_secret_key_123")

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "9000"))
UDP_PORT = int(os.environ.get("UDP_PORT", str(PORT + 1)))

users = {}  # name -> {"connection": ReliableSocket, "address": (ip, port), "last_seen": timestamp}
lock = threading.RLock()

def sync_ui_users():
    """Helper to keep the UI active user list synchronized."""
    with lock:
        app_state["users"] = [f"{name} ({info['address'][0]})" for name, info in users.items()]

def remove_connection(connection, reason):
    with lock:
        stale = [name for name, user in users.items() if user["connection"] is connection]
        for name in stale:
            users.pop(name, None)
            print(f"Disconnected {name}: {reason}")
    sync_ui_users()

def send_error(connection, reason):
    try:
        connection.send({"type": "ERROR", "reason": reason})
    except (OSError, ConnectionError):
        pass

def handle_client(connection, address):
    authenticated_name = None
    try:
        while True:
            packet, _ = connection.recv()
            message_type = packet.get("type")

            if message_type == "AUTH_REQ":
                name = str(packet.get("name", "")).strip()
                if not name:
                    connection.send({"type": "AUTH_FAIL", "reason": "Name cannot be empty"})
                    continue
                with lock:
                    existing = users.get(name)
                    if existing and existing["connection"] is not connection:
                        connection.send({"type": "AUTH_FAIL", "reason": "Name is already in use"})
                        continue
                    users[name] = {
                        "connection": connection,
                        "address": address,
                        "last_seen": time.monotonic(),
                    }
                    authenticated_name = name
                connection.send({"type": "AUTH_OK", "name": name})
                print(f"User authenticated: {name} from {address}")
                
                # Update UI Dashboard state with newly connected user
                sync_ui_users()
                app_state["messages"].append({
                    "sender": "System",
                    "text": f"User '{name}' joined the session.",
                    "is_private": False
                })
                continue

            with lock:
                user = users.get(authenticated_name) if authenticated_name else None
                if not user or user["connection"] is not connection:
                    send_error(connection, "Please log in first")
                    continue
                user["last_seen"] = time.monotonic()

            if message_type == "USER_LIST":
                with lock:
                    names = sorted(users)
                connection.send({"type": "USER_LIST", "users": names})
                
            elif message_type == "MSG_BROADCAST":
                msg_text = str(packet.get("text", ""))
                outgoing = {
                    "type": "MSG",
                    "from": authenticated_name,
                    "text": msg_text,
                }
                app_state["messages"].append({
                    "sender": authenticated_name,
                    "text": msg_text,
                    "is_private": False
                })
                
                with lock:
                    recipients = [u["connection"] for n, u in users.items() if n != authenticated_name]
                for recipient in recipients:
                    try:
                        recipient.send(outgoing)
                    except (OSError, ConnectionError):
                        remove_connection(recipient, "send failure")
                        
            elif message_type == "MSG_UNICAST":
                recipient_name = str(packet.get("to", ""))
                msg_text = str(packet.get("text", ""))
                with lock:
                    recipient = users.get(recipient_name)
                if recipient:
                    try:
                        recipient["connection"].send({
                            "type": "MSG",
                            "from": authenticated_name,
                            "text": msg_text,
                        })
                        app_state["messages"].append({
                            "sender": authenticated_name,
                            "text": f"(Direct to {recipient_name}): {msg_text}",
                            "is_private": True
                        })
                    except (OSError, ConnectionError):
                        send_error(connection, "Recipient is unavailable")
                else:
                    send_error(connection, "Recipient is not online")
                    
            elif message_type == "BYE":
                connection.send({"type": "BYE_OK"})
                break
            elif message_type == "PING":
                connection.send({"type": "PONG"})
            else:
                send_error(connection, "Unknown message type")
    except (OSError, ConnectionError, ValueError) as exc:
        remove_connection(connection, str(exc))
    finally:
        remove_connection(connection, "client closed")
        connection.close()

def heartbeat_listener():
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp.bind((HOST, UDP_PORT))
    while True:
        try:
            data, address = udp.recvfrom(1024)
            if data == b"PING":
                udp.sendto(b"PONG", address)
        except OSError as exc:
            print(f"Heartbeat socket error: {exc}")

def cleanup_inactive_users():
    while True:
        time.sleep(5)
        now = time.monotonic()
        with lock:
            expired = [
                (name, user["connection"])
                for name, user in users.items()
                if now - user["last_seen"] > 30
            ]
            for name, _connection in expired:
                users.pop(name, None)
        if expired:
            sync_ui_users()
        for name, connection in expired:
            print(f"User timed out: {name}")
            connection.close()

def main():
    threading.Thread(target=heartbeat_listener, daemon=True).start()
    threading.Thread(target=cleanup_inactive_users, daemon=True).start()

    listener = ReliableSocket(bind=(HOST, PORT))
    listener.sock.listen()
    print(f"TCP server listening on {HOST}:{PORT}; UDP heartbeat on {UDP_PORT}")
    while True:
        raw_connection, address = listener.sock.accept()
        connection = ReliableSocket(sock=raw_connection, on_giveup=lambda _a, _p: None)
        connection.peer = address
        connection._start_receiver()
        threading.Thread(
            target=handle_client,
            args=(connection, address),
            daemon=True,
        ).start()

if __name__ == "__main__":
    ui_thread = threading.Thread(target=start_resilient_server, kwargs={'port': 8081}, daemon=True)
    ui_thread.start()
    main()