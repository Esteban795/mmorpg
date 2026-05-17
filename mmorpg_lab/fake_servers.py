import socket
import json
import time
import threading

UDP_IP = "127.0.0.1"
UDP_PORT = 8000 # The Orchestrator's Listener Port

def simulate_server(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"Fake Server {port} booted and sending heartbeats...")
    
    try:
        while True:
            # Exact match to your Rust ServerInfo struct!
            payload = {
                "ip": "127.0.0.1",
                "port": port,
                "zone": "forêt_sombre",
                "status": "available",
                "num_players": 10,
                "capacity": 100,
                "lat": 48.4,
                "lon": -71.0,
                "cpu_usage": 45.0,
                "mem_usage": 512
            }
            
            json_string = json.dumps(payload)
            sock.sendto(json_string.encode('utf-8'), (UDP_IP, UDP_PORT))
            
            # Send a heartbeat every 3 seconds
            time.sleep(3)
    except KeyboardInterrupt:
        print(f"\n Fake Server {port} crashed (KeyboardInterrupt).")

# Simulate 3 active servers on ports 8001, 8002, 8003
if __name__ == "__main__":
    print("Starting MMO Chaos Test...")
    ports = [8001, 8002, 8003]
    threads = []
    
    for p in ports:
        t = threading.Thread(target=simulate_server, args=(p,))
        t.daemon = True
        t.start()
        threads.append(t)
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nTest ended.")