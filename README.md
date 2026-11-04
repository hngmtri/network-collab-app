# UI & Resilience Implementation 

This folder contains the complete deliverables for the UI & Resilience Engineer. It fulfills Steps 7, 9, and 10 of the grading rubric.

## 1. Running the Server (For Grading)
1. Open a terminal and navigate to this directory.
2. Run `python ui_server.py`
3. If you get a `[WinError 10013] PermissionError`, open `ui_server.py`, scroll to the bottom, and change `port=8081` to another number like `5050`.
4. Open a browser and navigate to `http://127.0.0.1:8081` (or your new port).

## 2. Key Compliance
- **No High-Level Frameworks:** Uses ONLY raw `socket` and `threading` libraries.
- **Client-Side JS Only:** The UI polls data natively without WebSockets.
- **Resilience:** The server handles `socket.timeout` and `ConnectionResetError` gracefully. Test this by closing browser tabs abruptly.

---

## 3. INSTRUCTIONS FOR TEAMMATES: How to Merge Your Backend Code
To connect your raw TCP/UDP networking logic to this interface, you do NOT need to write any HTML or JavaScript. You only need to interact with the `app_state` dictionary inside `ui_server.py`.

### Step A: Remove the Simulator
In `ui_server.py`, delete the `simulate_incoming_data()` function and remove this line inside `start_resilient_server`:
`threading.Thread(target=simulate_incoming_data, daemon=True).start()`

### Step B: Run Your Network Threads Alongside the UI
Import your custom backend networking logic directly into `ui_server.py` and start your listener loops in separate threads (just like the UI server loop).

### Step C: Feed the `app_state` Dictionary
Whenever your networking script processes new data, append it directly to the `app_state` variable. The UI will automatically pull and display it.

**Example for Active Users (Step 3):**
```python
# When a new user logs in via your TCP/UDP handshake:
app_state["users"].append("Dave (192.168.1.15)")
```

**Example for Messaging (Step 5):**
```python
# When your routing logic receives a custom MSG packet over UDP:
app_state["messages"].append({
    "sender": "Dave", 
    "text": "Hello network!", 
    "is_private": False
})
```

**Example for File Transfers (Step 6):**
```python
# When your file chunking logic processes a chunk:
app_state["files"]["large_video.mp4"] = 45  # Updates progress bar to 45%
```