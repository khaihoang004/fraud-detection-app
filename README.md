# Fraud Detection App

## 1. Prerequisites

Make sure the following are installed on your machine:

- **Docker** ≥ 20.x  
- **Docker Compose** ≥ v2  
- Operating System: Windows / Linux / macOS  
- Recommended RAM: **≥ 8GB** (Cassandra is memory-intensive)

Verify installation:

```bash
docker --version
docker compose version
```

## 2. Build and Start the System
### 2.1. Build Docker Images
```bash
docker compose build
```

### 2.2. Start All Services
```bash
docker compose up -d
```
This command starts:
- Redis (Streams)
- RedisInsight (UI)
- Cassandra
- Cassandra initialization service
- Producer
- Consumer
- Monitor

## 3. Access Services
🔹 RedisInsight
- URL: http://localhost:5540
- Redis Host: redis
- Port: 6379

🔹 Cassandra (CQL Shell)
```
docker exec -it cassandra cqlsh
```

🔹 Dashboard
- URL: http://localhost:5000

## 4. Check Status and Logs
### 4.1 Check Running Containers
```bash
docker compose ps
```

### 4.2 View Logs
```
docker compose logs -f
```

Or view logs per service:
```
docker compose logs -f producer
docker compose logs -f consumer
docker compose logs -f monitor
```

