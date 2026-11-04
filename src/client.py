
import os
import socket
import sys
import threading
import time

from reliable import ReliableSocket

SERVER_HOST = os.environ.get("SERVER", "127.0.0.1")
PORT = int(os.environ.get("PORT", "9000"))
UDP_PORT = int(os.environ.get("UDP_PORT", str(PORT + 1)))
SERVER = (SERVER_HOST, PORT)


def report_giveup(_address, packet):
    print(f"\n[!] No acknowledgement for message: {packet.get('type')}", flush=True)


def receiver(connection, stop_event):
    while not stop_event.is_set():
        try:
            packet, _ = connection.recv()
        except (OSError, ConnectionError, ValueError) as exc:
            if not stop_event.is_set():
                print(f"\n[!] Connection lost: {exc}", flush=True)
                stop_event.set()
            return
        if packet.get("type") == "PONG":
            continue
        print(f"\n<< {packet}", flush=True)
        if packet.get("type") in {"AUTH_FAIL", "BYE_OK"}:
            if packet.get("type") == "BYE_OK":
                stop_event.set()


def heartbeat(stop_event, connection):
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(2)
    while not stop_event.wait(10):
        try:
            # Keep the authenticated TCP session active; UDP is used for heartbeat discovery.
            connection.send({"type": "PING"}, reliable=False)
            udp.sendto(b"PING", (SERVER_HOST, UDP_PORT))
            udp.recvfrom(128)
        except socket.timeout:
            print("\n[!] Heartbeat timed out", flush=True)
        except OSError as exc:
            print(f"\n[!] Heartbeat error: {exc}", flush=True)
    udp.close()


def main():
    name = input("Your name: ").strip()
    if not name:
        print("Name cannot be empty.")
        return

    stop_event = threading.Event()
    connection = ReliableSocket.connect(SERVER, on_giveup=report_giveup)
    threading.Thread(target=receiver, args=(connection, stop_event), daemon=True).start()
    threading.Thread(target=heartbeat, args=(stop_event, connection), daemon=True).start()
    connection.send({"type": "AUTH_REQ", "name": name})

    print("Commands: /all <message> | /to <name> <message> | /list | /quit")
    try:
        while not stop_event.is_set():
            try:
                line = input()
            except EOFError:
                line = "/quit"

            if line.startswith("/all "):
                connection.send({"type": "MSG_BROADCAST", "text": line[5:]})
            elif line.startswith("/to "):
                parts = line.split(" ", 2)
                if len(parts) == 3 and parts[2].strip():
                    connection.send({"type": "MSG_UNICAST", "to": parts[1], "text": parts[2]})
                else:
                    print("Usage: /to <name> <message>")
            elif line == "/list":
                connection.send({"type": "USER_LIST"})
            elif line == "/quit":
                connection.send({"type": "BYE"})
                time.sleep(0.2)
                stop_event.set()
                break
            elif line:
                print("Unknown command. Use /all, /to, /list, or /quit.")
    except (OSError, ConnectionError) as exc:
        print(f"Connection error: {exc}")
    finally:
        stop_event.set()
        connection.close()


if __name__ == "__main__":
    main()
