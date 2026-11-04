import socket
import threading
import json
import time

# Step 7 & 9 Complete Implementation:
# - Serves HTTP using ONLY raw sockets (Django/Flask prohibited).
# - Handles broken pipes, timeouts, and concurrent requests safely.

# Dummy state to demonstrate the UI (Teammates will replace this with live UDP data)
app_state = {
    "users": ["Alice (192.168.1.10)", "Bob (192.168.1.11)"],
    "messages": [
        {"sender": "System", "text": "Secure socket server initialized.", "is_private": False}
    ],
    "files": {"architecture_diagram.pdf": 10, "protocol_spec.docx": 100}
}

def simulate_incoming_data():
    """Simulates background data arriving from teammates' backend logic."""
    time.sleep(5)
    app_state["messages"].append({"sender": "Alice", "text": "Are the custom headers ready?", "is_private": False})
    app_state["files"]["architecture_diagram.pdf"] = 65
    time.sleep(5)
    app_state["users"].append("Charlie (192.168.1.12)")
    app_state["messages"].append({"sender": "Bob", "text": "Sent the raw bytes over UDP.", "is_private": True})
    app_state["files"]["architecture_diagram.pdf"] = 100

def handle_http_request(client_socket, client_address):
    # Step 7: Network Resilience - Set timeout to prevent resource starvation
    client_socket.settimeout(5.0) 
    
    try:
        request_data = client_socket.recv(4096).decode('utf-8')
        if not request_data:
            return
            
        lines = request_data.split('\r\n')
        if not lines or len(lines[0].split()) < 2:
            return
            
        method, path = lines[0].split()[0:2]
        
        # Routing raw HTTP
        if path == '/':
            with open('index.html', 'r', encoding='utf-8') as f:
                html_content = f.read()
            response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html\r\n"
                f"Content-Length: {len(html_content)}\r\n"
                "Connection: close\r\n\r\n"
                f"{html_content}"
            )
            client_socket.sendall(response.encode('utf-8'))
            
        elif path == '/api/network_state':
            json_data = json.dumps(app_state)
            response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                f"Content-Length: {len(json_data)}\r\n"
                "Connection: close\r\n\r\n"
                f"{json_data}"
            )
            client_socket.sendall(response.encode('utf-8'))
        else:
            # 404 Not Found
            client_socket.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")

    except socket.timeout:
        # Step 7: Gracefully handle slow or dead clients without crashing
        print(f"[{client_address}] HTTP Request timed out.")
    except (ConnectionResetError, BrokenPipeError):
        # Step 7: Handle sudden client disconnects (e.g., closing browser tab abruptly)
        print(f"[{client_address}] Connection dropped abruptly by client.")
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
        
        # Start background simulator for demo purposes
        threading.Thread(target=simulate_incoming_data, daemon=True).start()
        
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