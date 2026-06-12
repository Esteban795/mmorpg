Première implémention de l'orchestrator :
- Reçoit et parse les heartbeats (ServerInfo pour le moment, à voir pour changer avec exactement les infos nécessaires) et met à jour Redis avec une expiration de 15s.
- Si on a moins que HOT_SERVERS_MIN serveurs disponibles, on en spawn* des nouveaux.

**A noter** : update aussi le Redis pour se conformer au .md

*Pour l'instant on fait rien à part print dans le terminal.

# Pour tester :

## Docker & Redis
Il faut avoir docker d'installé et lancer le docker redis avec la commande suivante : 
```
docker run --name mmorpg-redis -p 6379:6379 -d redis
```
Cela devrait lancer un docker redis sur la machine avec pour nom "mmorpg-redis"

## Heartbeats

### Monitor Redis

Pour checker ce qui se passe dans la DB redis, il suffit de :
Rentrer dans la CLI : 
```
docker exec -it mmorpg-redis redis-cli
```

Puis taper MONITOR

```
127.0.0.1:6379> MONITOR
OK
```
De là, on peut avoir les infos pour chaque ajout / get dans la DB.

### Envoyer et écouter les heartbeats

Pour envoyer des heartbeats de test, j'ai créé un petit script python à la racine du dépot. Suffit de faire (depuis mmorpg_lab)
```
python3 fake_servers.py
```

Enfin, pour écouter les heartbeats on lance l'orchestrator :
```
cargo run -p orchestrator
```
ça devrait donner quelque chose comme : 

```
Starting MMORPG Orchestrator...
Scaler started. Minimum available servers required: 3
Listener started on UDP 8000...
Cluster Status: 0/3 available servers.
Need 3 more servers. Spawning...
Booting Bevy server on port 8001
Booting Bevy server on port 8002
Booting Bevy server on port 8003
Heartbeat updated for server:127.0.0.1:8003
Heartbeat updated for server:127.0.0.1:8001
Heartbeat updated for server:127.0.0.1:8002
Heartbeat updated for server:127.0.0.1:8003
Heartbeat updated for server:127.0.0.1:8002
Heartbeat updated for server:127.0.0.1:8001
Cluster Status: 3/3 available servers.
```
tant que les heartbeats continuent. Puis, au bout de 15 secondes sans heartbeat (ctrl+c le programme python), on repasse à : 
```
Cluster Status: 0/3 available servers.
Need 3 more servers. Spawning...
Booting Bevy server on port 8013
Booting Bevy server on port 8014
Booting Bevy server on port 8015
```


# A faire encore : 

- Spawn les dedicated game server
- Rendre le port configurable (ORCH_PORT)
- Eventuellement faire un heartbeat plus "propre" 
