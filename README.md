# Rust MMORPG Architecture

A modular, scalable, and authoritative MMORPG architecture built in Rust. 

This project demonstrates a modern microservices approach to multiplayer game development, featuring a Gatekeeper for authentication, an Orchestrator for auto-scaling, and Dedicated Game Servers running on Bevy.

## Table of Contents
1. [How to Test the Cluster](#how-to-test-the-cluster)
2. [Connecting Clients](#connecting-clients)
3. [Architecture & Implementation](#architecture--implementation)
    - [Client](#1-client-implementation)
    - [Dedicated Game Server](#2-dedicated-game-server-implementation)
    - [Gatekeeper](#3-gatekeeper-implementation)
    - [Orchestrator](#4-orchestrator-implementation)

---

## How to Test the Cluster

**Prerequisite:** Ensure you have the Redis database running locally (e.g., via Docker: `docker run --name mmorpg-redis -p 6379:6379 -d redis`).

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
The REST API is listening on `DEFAULT_GATEKEEPER_ADDR_PORT` environment variable. It defaults to `127.0.0.1:8080`.

+ REST API : two very simple endpoints : 
  + `/login` which takes a username and a password. Currently accepts any username with password '1234' and returns the ip, port and zone of the game server that is closest to the player. For now, it checks the redis database every time but it should cache the available servers to reduce latency in the future.
  + `/health` which takes no parameters and only returns a OK. It's basically a ping to check if the API is up and available. 
+ Redis : uses the `redis` crate to open multiplexed connections to prevent blocking any incoming transactions

### 4. Orchestrator Implementation

