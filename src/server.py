"""TCP messaging/file relay, UDP heartbeat, and live HTTP dashboard."""

import os
import socket
import threading
import time

from reliable import ReliableSocket
from router import MessageRouter
from ui_server import app_state, configure_web_api, start_resilient_server, state_lock, web_sessions, web_sessions_lock
from users import UserRegistry
from udp_filetransfer import UDPFileUploadReceiver

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "9000"))
UDP_PORT = int(os.environ.get("UDP_PORT", str(PORT + 1)))
UI_PORT = int(os.environ.get("UI_PORT", "8081"))

registry = UserRegistry()
web_user_names = set()
web_user_names_lock = threading.RLock()


def publish_message(message):
    with state_lock:
        app_state["messages"].append(message)
        del app_state["messages"][:-500]


def publish_file_progress(filename, progress):
    with state_lock:
        app_state["files"][filename] = max(0, min(100, int(progress)))


router = MessageRouter(registry, on_message=publish_message, on_file_progress=publish_file_progress)
udp_file_receiver = UDPFileUploadReceiver(registry)


def sync_ui_users():
    tcp_users = registry.ui_snapshot()
    with web_user_names_lock:
        browser_users = [f"{name} (web)" for name in sorted(web_user_names)]
    with state_lock:
        app_state["users"] = sorted(set(tcp_users + browser_users))


def web_login(name, address):
    if registry.get(name):
        return False, "That name is already connected from a terminal client."
    with web_sessions_lock:
        if name in web_sessions.values():
            return False, "That name is already signed in on the web."
    with web_user_names_lock:
        web_user_names.add(name)
    sync_ui_users()
    print(f"Web user authenticated: {name} from {address}", flush=True)
    publish_message({"sender": "System", "text": f"{name} joined from the web.", "is_private": False})
    return True, name


def web_logout(name):
    with web_user_names_lock:
        web_user_names.discard(name)
    sync_ui_users()
    print(f"Web user signed out: {name}", flush=True)
    publish_message({"sender": "System", "text": f"{name} left the web chat.", "is_private": False})


def web_send(sender, packet):
    kind = packet["type"]
    if kind == "WEB_FILE":
        recipient = packet.get("to", "")
        with web_user_names_lock:
            online = recipient in web_user_names
        if not online:
            return False, "Web file delivery requires the recipient to be signed in to the web chat."
        publish_message({
            "sender": sender, "to": recipient, "text": packet["name"],
            "is_private": True, "file_id": packet["file_id"],
            "file_size": packet["size"], "sha256": packet["sha256"],
        })
        return True, None
    text = packet["text"]
    if kind == "MSG_BROADCAST":
        publish_message({"sender": sender, "text": text, "is_private": False})
        for name, user in registry.recipients(exclude=sender):
            try:
                user.send({"type": "MSG", "from": sender, "text": text})
            except (OSError, ConnectionError):
                pass
        return True, None

    recipient = packet.get("to", "")
    with web_user_names_lock:
        web_recipient_online = recipient in web_user_names
    tcp_recipient = registry.get(recipient)
    if not web_recipient_online and not tcp_recipient:
        return False, "Recipient is not online."
    if tcp_recipient:
        tcp_recipient["connection"].send({"type": "MSG", "from": sender, "text": text})
    publish_message({"sender": sender, "to": recipient, "text": text, "is_private": True})
    return True, None


configure_web_api(web_login, web_send, web_logout)


def handle_client(connection, address):
    authenticated_name = None
    try:
        while True:
            packet, _ = connection.recv()
            if packet.get("type") == "AUTH_REQ":
                name = str(packet.get("name", "")).strip()
                with web_user_names_lock:
                    used_on_web = name in web_user_names
                if used_on_web:
                    connection.send({"type": "AUTH_FAIL", "reason": "Name is already in use on the web."})
                    continue
                accepted, result = registry.add(name, connection, address)
                if not accepted:
                    connection.send({"type": "AUTH_FAIL", "reason": result})
                    continue
                authenticated_name = result
                connection.send({"type": "AUTH_OK", "name": authenticated_name})
                sync_ui_users()
                publish_message({
                    "sender": "System",
                    "text": f"{authenticated_name} joined the session.",
                    "is_private": False,
                })
                print(f"Authenticated {authenticated_name} from {address}")
                continue

            if not authenticated_name or not registry.touch(authenticated_name, connection):
                connection.send({"type": "ERROR", "reason": "Please log in first"})
                continue

            if packet.get("type") == "BYE":
                connection.send({"type": "BYE_OK"})
                break
            router.route(authenticated_name, connection, packet)
    except (OSError, ConnectionError, ValueError) as exc:
        print(f"Client {address} disconnected: {exc}")
    finally:
        removed = registry.remove_connection(connection)
        if removed:
            sync_ui_users()
            for name in removed:
                publish_message({"sender": "System", "text": f"{name} left the session.", "is_private": False})
        connection.close()


def heartbeat_listener():
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp.bind((HOST, UDP_PORT))
    print(f"UDP heartbeat listening on {HOST}:{UDP_PORT}")
    while True:
        try:
            data, address = udp.recvfrom(4096)
            if data == b"PING":
                udp.sendto(b"PONG", address)
            else:
                udp_file_receiver.handle(data, address, udp)
        except OSError as exc:
            print(f"Heartbeat socket error: {exc}")


def cleanup_inactive_users():
    while True:
        time.sleep(5)
        expired = registry.expire(30)
        if expired:
            sync_ui_users()
        for name, connection in expired:
            print(f"User timed out: {name}")
            connection.close()


def main():
    threading.Thread(target=heartbeat_listener, daemon=True).start()
    threading.Thread(target=cleanup_inactive_users, daemon=True).start()
    threading.Thread(
        target=start_resilient_server,
        kwargs={"host": os.environ.get("UI_HOST", "0.0.0.0"), "port": UI_PORT},
        daemon=True,
    ).start()

    listener = ReliableSocket(bind=(HOST, PORT))
    listener.sock.listen(20)
    print(f"TCP backend listening on {HOST}:{PORT}")
    print(f"Open the live dashboard at http://127.0.0.1:{UI_PORT}/")
    try:
        while True:
            raw_connection, address = listener.sock.accept()
            connection = ReliableSocket(sock=raw_connection)
            connection.peer = address
            connection._start_receiver()
            threading.Thread(target=handle_client, args=(connection, address), daemon=True).start()
    except KeyboardInterrupt:
        print("Server is shutting down.")
        listener.sock.close()


if __name__ == "__main__":
    main()
