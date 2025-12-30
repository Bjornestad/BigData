# Quick Start Script for Power Grid Monitor with Database

## Fixed Issues ✅

1. **TimescaleDB Image Tag**
   - ❌ Was: `timescale/timescaledb-ha:pg16-latest` (doesn't exist)
   - ✅ Now: `timescale/timescaledb:latest-pg16` (official tag)

2. **Init Script Path**
   - ❌ Was: Mounting YAML file as SQL
   - ✅ Now: Created proper `init-db/01-init-schema.sql`

3. **Database Schema**
   - ✅ Added `UNIQUE(time, type)` constraint for ON CONFLICT

4. **Docker Compose Version**
   - ✅ Removed obsolete `version: '3.8'`

## Start the System

```bash
cd C:\Users\caspe\Desktop\fera\power-grid-monitor

# Start all services
docker-compose -f docker-compose-with-db.yml up --build

# Wait ~60 seconds for everything to initialize
# Open browser: http://localhost:3000
```

## What Will Start

1. **Zookeeper** (Port 2181) - Kafka coordination
2. **Kafka** (Port 9092) - Message broker
3. **TimescaleDB** (Port 5432) - Time-series database
4. **Database Writer** (Port 8081) - Kafka → DB writer
5. **Backend** (Port 8080) - WebSocket + HTTP API
6. **Frontend** (Port 3000) - React UI
7. **Test Producer** - Generates fake data

## Check Status

### All containers running:
```bash
docker ps
```

Should show 7 containers running.

### Database writer health:
```bash
curl http://localhost:8081/health
```

Should return JSON with stats.

### Backend health:
```bash
curl http://localhost:8080/health
```

Should show `"databaseEnabled": true`.

### Database content:
```bash
docker exec -it timescaledb psql -U postgres -d power_grid -c "SELECT COUNT(*) FROM measurements;"
```

Should show increasing count after ~10-20 seconds.

## Frontend Usage

1. Open http://localhost:3000
2. You'll see 5 buttons: **LIVE, 1 HOUR, 24 HOURS, 7 DAYS, 30 DAYS**
3. Click **LIVE** - Shows real-time data (WebSocket)
4. Wait 2-3 minutes for data to accumulate in database
5. Click **1 HOUR** - Queries database for historical data
6. All buttons should work!

## Troubleshooting

### Issue: "manifest for timescale/timescaledb-ha:pg16-latest not found"
**Status:** ✅ FIXED - Updated to correct image tag

### Issue: TimescaleDB won't start
```bash
docker logs timescaledb
```
Check for SQL errors. If needed, reset:
```bash
docker-compose -f docker-compose-with-db.yml down -v
docker-compose -f docker-compose-with-db.yml up --build
```

### Issue: Database writer not writing
```bash
docker logs database-writer --tail=50
```
Should see "✓ Flushed X records" after 10-20 seconds.

### Issue: Frontend shows "Failed to load historical data"
Wait 2-3 minutes for data to accumulate, then try again.

## Stop Everything

```bash
docker-compose -f docker-compose-with-db.yml down
```

## Reset Everything (Including Data)

```bash
docker-compose -f docker-compose-with-db.yml down -v
docker-compose -f docker-compose-with-db.yml up --build
```

## Next Steps

Once working locally:
1. Deploy to Kubernetes (see DATABASE_DEPLOYMENT_GUIDE.md)
2. Update backend: `cp backend/server-with-db.js backend/server.js`
3. Update frontend: `cp frontend/src/App-with-db.js frontend/src/App.js`
4. Build and push Docker images to your registry
5. Deploy to production cluster

---

**Ready to go!** Just run:
```bash
docker-compose -f docker-compose-with-db.yml up --build
```
