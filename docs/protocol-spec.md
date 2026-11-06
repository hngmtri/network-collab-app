# Application packet protocol

All TCP application packets are JSON objects inside length-prefixed frames. `ReliableSocket` encrypts each frame with Fernet and retries unacknowledged messages. Chat and terminal file relay packets therefore travel over encrypted TCP.

## Reliable UDP file upload

Terminal clients send the file upload packets below to the server's UDP port (default `9001`). Each JSON packet is encrypted as a Fernet token before it is sent. The shared `APP_SECRET` must match on the server and clients. The server reassembles the file, verifies its byte count and SHA-256 digest, then relays the verified file to the recipient over the existing encrypted TCP connection.

| Packet | Fields | Purpose |
| --- | --- | --- |
| `UDP_FILE_START` | `transfer_id`, `sender`, `to`, `name`, `size`, `sha256` | Opens a transfer and announces the expected file metadata. |
| `UDP_FILE_ACK` | `transfer_id`, `ack: START` | Confirms the server accepted the transfer metadata. |
| `UDP_FILE_CHUNK` | `transfer_id`, `index`, base64 `data` | Sends one ordered chunk; raw data is at most 400 bytes. |
| `UDP_FILE_ACK` | `transfer_id`, `ack: CHUNK`, `index` | Confirms that numbered chunk was written once. A repeated chunk is ACKed again without duplicate bytes. |
| `UDP_FILE_END` | `transfer_id` | Says the sender has transmitted all chunks. |
| `UDP_FILE_ACK` | `transfer_id`, `ack: COMPLETE` | Confirms successful reassembly, size and SHA-256 verification, and relay to the recipient. |
| `UDP_FILE_ERROR` | `transfer_id`, `reason` | Reports a rejected transfer or integrity failure. |

The sender waits for the matching ACK after every packet. If it times out, it resends that exact encrypted datagram up to 12 times. The server only writes the next expected chunk index, so a duplicate caused by a lost ACK cannot corrupt or duplicate the output. The receiver checks the final file size and SHA-256 before accepting it.

Example transfer sequence:

```text
Client A        Server                         Client B
   |-- encrypted UDP_FILE_START -->|                |
   |<-- encrypted ACK START -------|                |
   |-- encrypted UDP_FILE_CHUNK 0->|                |
   |<-- encrypted ACK CHUNK 0 -----|                |
   |-- encrypted UDP_FILE_CHUNK 1->|                |
   |<-- encrypted ACK CHUNK 1 -----|                |
   |-- encrypted UDP_FILE_END ---->|                |
   |                               | reassemble +   |
   |                               | SHA-256 check  |
   |                               |-- encrypted TCP file packets -->|
   |<-- encrypted ACK COMPLETE ---|                | verify & save
```

The separate UDP `PING`/`PONG` heartbeat remains a lightweight liveness check; file packet names begin with `UDP_FILE_` and use the same UDP listener.
