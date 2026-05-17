import socket

# Crée un socket UDP
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", 8000))

print("Faux Orchestrateur en écoute sur le port UDP 8000...")
while True:
    data, addr = sock.recvfrom(1024)
    print(f"Reçu de {addr}: {data.decode('utf-8')}")