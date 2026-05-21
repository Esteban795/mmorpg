# Rust MMORPG Architecture

A modular, scalable, and authoritative MMORPG architecture built in Rust. 

This project demonstrates a modern microservices approach to multiplayer game development, featuring a Gatekeeper for authentication, an Orchestrator for auto-scaling, and Dedicated Game Servers running on Bevy.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [How to Test the Cluster](#how-to-test-the-cluster)
3. [Connecting Clients](#connecting-clients)
4. [Architecture & Implementation](#architecture--implementation)
    - [Client](#1-client-implementation)
    - [Dedicated Game Server](#2-dedicated-game-server-implementation)
    - [Gatekeeper](#3-gatekeeper-implementation)
    - [Orchestrator](#4-orchestrator-implementation)

---

## Prerequisites

Before building or running the project, ensure your development environment meets the following requirements:

* **Rust Toolchain:** You must have Rust installed (version 1.80+ recommended). Install it via [rustup.rs](https://rustup.rs/).
* **Redis Database:** The Orchestrator and Gatekeeper rely heavily on Redis for state management and matchmaking.
  1. **Install Docker (If not already installed):**
     * **Windows / macOS:** Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/). Ensure the Docker Desktop application is running.
     * **Linux (Ubuntu/Debian):** Run the following commands in your terminal to install and start Docker:
       ```bash
       sudo apt update
       sudo apt install -y docker.io
       sudo systemctl start docker
       sudo systemctl enable docker
       # Optional: allow running docker without sudo:
       sudo usermod -aG docker $USER
       ```
       *(Note: If you add your user to the docker group, log out and back in for changes to take effect).*

  2. **Launch Redis via Docker:** Once Docker is running, spin up a local Redis instance instantly with:
     ```bash
     docker run --name mmorpg-redis -p 6379:6379 -d redis
     ```
* **OS-Specific Dependencies (For Bevy):** Because the game client uses the Bevy engine (v0.18) for rendering, you may need specific system libraries depending on your OS.
  * **Linux:** You will need ALSA and udev. E.g., on Ubuntu:
    ```
    sudo apt install g++ pkg-config libx11-dev libasound2-dev libudev-dev
    ```
  * **Windows/macOS:** Generally works out of the box with the standard Rust toolchain.

## How to Test the Cluster

**Prerequisite:** Ensure you have the Redis database running locally (e.g., via Docker:). 
```
docker run --name mmorpg-redis -p 6379:6379 -d redis
```


Start by compiling the entire workspace to avoid file lock contentions later:
```bash
cargo build
```

1. Pre-build the Dedicated Server

```bash
cargo build -p dedicated_server
```

2. Launch the Orchestrator
Open a new terminal in the mmorpg_lab directory and run:

```bash
cargo run -p orchestrator
```

Note: The orchestrator will immediately detect that 0 servers are available and will automatically spawn 3 dedicated servers in the background. It will then receive their UDP heartbeats and register them in Redis.

## Connecting Clients

With your Redis database and Orchestrator running, you can now simulate player traffic.

1. Launch the Gatekeeper
Open a new terminal and run the API gateway:

```bash
cargo run -p gatekeeper
```

2. Launch Game Clients
Open one or multiple new terminals to launch the clients:

```bash
cargo run -p client
```

- In the game window, enter any Username you like.
- The default test Password is 1234.
- Click "Connect".

Note: For testing purposes, the Orchestrator currently limits server capacity to 3 players. Once 3 players join the same server, the Orchestrator will mark it as full and automatically spawn a new server to accommodate the 4th player.

## Architecture & Implementation

### 1. Client Implementation

The game client is built using the Bevy 0.18 engine and acts as a "dumb terminal". It handles rendering and user inputs, but strictly relies on the server for game logic and entity positioning.

#### Technical Choices & Features:

**Two-Step Connection Protocol:**
  1. HTTP/REST: The client starts in an AppState::LoginMenu and uses an HTTP request to authenticate with the Gatekeeper. It receives a dynamic IP and Port assignment in return.
  2. QUIC (bevy_quinnet): Upon successful login, the client transitions to AppState::InGame and seamlessly opens a secure, multiplexed QUIC tunnel to the assigned Dedicated Server.

**Egui UI Integration:** 
Uses bevy_egui to render a lightweight, non-blocking login interface. Async tasks (IoTaskPool) are used for HTTP requests to prevent freezing the main game thread.

**State-Driven Rendering:** 
The client manages a HashMap linking Network IDs to Bevy Entities.

**Snapshot-based AOI (Area of Interest):** 
The client's 2D world is entirely driven by the server's AOISnapshots. If a player ID is missing from the current frame's snapshot, the client automatically uses despawn() to destroy the entity and its children (like the floating username text), saving memory and rendering resources. For each player ID in the current snapshot, the system either spawns a new entity (if not already visible) or updates the existing entity's position


### 2. Dedicated Game Server Implementation

The Dedicated Server is the authoritative source of truth for the game. It handles physics, movement, and player broadcasting.

#### Technical Choices & Features:

**Headless Bevy (20Hz):** 
Uses ScheduleRunnerPlugin to run the engine without graphics or audio components. The server ticks at a fixed rate of 20 Updates per second.

**Dual Network Stack:**
  Client <-> Server (QUIC): Uses bevy_quinnet 0.20 to accept encrypted player connections. It handles JOIN handshakes, registers session IDs, and replies with WELCOME messages.
  Server -> Orchestrator (UDP): Uses a standard non-blocking std::net::UdpSocket to broadcast a JSON ServerInfo Heartbeat every 5 seconds.
  
**Authoritative Movement:** 
Clients only send input vectors (e.g., W, A, S, D). The server calculates the actual movement speed, applies boundary clamping (map limits), and updates the internal registry.

**Area of Interest (AOI) Optimization:** 
Instead of an $O(N²)$ broadcast where every player receives data about every other player, the server calculates distances. It sends a custom snapshot to each client containing only the state of players within a specific pixel radius (e.g., 400px).

### 3. Gatekeeper Implementation

The gatekeeper is a REST API made in Rust, which follows a simple architecture using `axum` and its way of doing things. It uses redis under the hood to communicate with the orchestrator to gather informations about game servers, mostly if they are available and where they are located on the globe. The locations of those servers are then used to determine the lowest latency that we could offer to our client, and this metric is (for now) the only one that decides which game server we want to redirect our player on.


#### Technical Choices & Features:
The REST API is listening on `GATEKEEPER_ADDR_PORT` environment variable. It defaults to `127.0.0.1:8080`.

+ REST API : two very simple endpoints : 
  + `/login` which takes a username and a password. Currently accepts any username with password '1234' and returns the ip, port and zone of the game server that is closest to the player. For now, it checks the redis database every time but it should cache the available servers to reduce latency in the future.
  + `/health` which takes no parameters and only returns a OK. It's basically a ping to check if the API is up and available. 
+ Redis : uses the `redis` crate to open multiplexed connections to prevent blocking any incoming transactions

### 4. Orchestrator Implementation

The orchestrator acts as the central control server for the game cluster. It is responsible for monitoring the health of all running game servers and automatically scaling the server pool to ensure there is always enough capacity for incoming players, maintaining a buffer of 'hot' servers to handle sudden login spikes.

#### Technical Choices & Features:

**Asynchronous Runtime (Tokio):** Built entirely on the Tokio async runtime, allowing it to concurrently manage the UDP listener and the continuous server-scaling loop without blocking operations. It utilizes multiplexed Redis connections to handle database interactions.

**UDP Heartbeat Listener:** Runs a background task listening on `0.0.0.0:8000` (configurable via `ORCH_PORT`). It continuously receives JSON `ServerInfo` payloads sent by the Dedicated Servers and updates their current state in the Redis database.

**TTL-Based deletion:** Instead of relying on complex disconnect logic, theorchestrator stores server data in Redis with a strict 15-second Time-To-Live (TTL) handled directly by Redis. As servers send heartbeats, the TTL is refreshed. If a Dedicated Server crashes or hangs, its Redis key naturally expires, automatically removing it from the Gatekeeper's routing pool.

**Proactive Auto-Scaling:** A scaler task ticks every 5 seconds to guarantee a minimum pool of available servers (currently configured to 3). It scans Redis for servers marked as "available" and calculates a projected count with servers already being started by the orchestrator. It actively tracks "pending spawns" with a 20-second timeout to prevent the system from over-spawning servers while binaries are still booting.

**Dynamic Port Allocation & Process Spawning:** When the cluster needs more capacity, the orchestrator safely tests UDP socket bindings (between ports 8001 and 9000) to find a free port. Once secured, it uses `tokio::process::Command` to seamlessly spawn new child processes of the headless Bevy server, injecting parameters like `DS_PORT` and `DS_ZONE` via environment variables.


