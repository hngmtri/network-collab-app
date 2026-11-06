# Network Collaboration App — Backend Demo

This runnable demo starts the TCP messaging backend, UDP heartbeat listener, and HTTP dashboard together. The dashboard shows live logins, messages, and file transfer progress.

## Install

From this project folder in PowerShell:

```powershell
py -m pip install -r .\requirements.txt
```

## Start the backend and dashboard

In PowerShell window 1, from this project folder:

```powershell
$env:APP_SECRET = "replace-this-with-a-long-random-secret"
py .\src\server.py
```

Keep this window open. The dashboard is at <http://127.0.0.1:8081>. The HTTP dashboard listens on all network interfaces by default, so a teammate on the same LAN can open `http://SERVER_LAN_IP:8081` (replace with the server computer's IPv4 address from `ipconfig`). Allow inbound TCP ports 8081 (web chat), 9000 (terminal clients), and UDP 9001 (heartbeat and file transfer) through the server computer's firewall.

On the dashboard, each teammate signs in with a display name. Names must be unique among web and terminal users. Choose `Everyone` to broadcast or a person from the recipient menu for a private message. Web users can message other web users and connected terminal clients.

To attach a file in the web chat, sign in, choose another web user from the recipient menu, press `+`, and select a file. The web upload limit is 8 MiB. The recipient downloads it from the attachment bubble. Larger files and transfers to terminal-only users should use `/file <recipient> <path>` in `src/client.py`.

## Connect clients

In PowerShell window 2, use the same secret and start one client:

```powershell
$env:APP_SECRET = "replace-this-with-a-long-random-secret"
py .\src\client.py
```

Enter a username. Open another PowerShell window and repeat to connect a second client. Use `/all hello` for broadcast, `/to username hello` for a private message, `/list` for active users, and `/quit` to disconnect. The dashboard updates as users join and messages are sent.

To populate the dashboard quickly with two live sample users and messages, open a third PowerShell window and run:

```powershell
$env:APP_SECRET = "replace-this-with-a-long-random-secret"
py .\demo_clients.py
```

Leave that process running while viewing the dashboard; press Ctrl+C to disconnect the demo users.

File transfer is available from a client with `/file <recipient> <path>`. The client sends encrypted UDP packets in 400-byte chunks. The server ACKs each packet, safely handles retransmissions, reassembles the file, and checks SHA-256 before relaying it to the recipient. Files received by a client are written to `received_files` under the current working directory. Both computers must use the same `APP_SECRET` and allow inbound UDP port 9001.

## Ports

- TCP messaging: `9000`
- UDP heartbeat and reliable file packets: `9001`
- HTTP dashboard: `8081`

If a port is occupied, set `PORT`, `UDP_PORT`, or `UI_PORT` before starting the server. Terminal clients use `PORT` and `UDP_PORT`; set `SERVER` to the server machine's IP when connecting across machines.

The dashboard uses plain HTTP and is intended for a trusted local network demo. Do not expose it to the public internet; messages and session tokens are not protected by HTTPS.

This is a local demo key setup. Use a unique shared secret for a real deployment, and do not commit it to source control.

To run the included checks from the project root:

```powershell
py -m unittest discover -s .\tests -v
```
