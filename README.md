# Real-Time Financial Transaction Pipeline

> **Kafka + PostgreSQL + Redis event-driven processing**

## Architecture

```
PostgreSQL → Producer → Kafka → Clearance → Settlement → Redis
```

## Features

- 🚀 Real-time processing
- 🎯 Risk-based validation  
- 💰 Balance management
- 📊 Audit trail
- ❌ Rejection handling
- 🖥️ Web UIs

## Components

**Producer**: Enriches transactions with customer data
**Clearance**: Validates balances and risk scores
**Settlement**: Updates balances and stores results

## Quick Start

```bash
# Setup
git clone <repo> && cd kafka-financial-poc
pip install -r requirements.txt
cd docker && docker-compose up -d

# Run (3 terminals)
python src/db_producer.py
python src/clearance_consumer.py  
python src/settlement_consumer.py
```

**UIs**: PostgreSQL (8080), Redis (8081), Kafka (8082)

## Testing

```bash
# Add transaction
docker exec -it docker-postgres-1 psql -U user -d financial_db -c "
INSERT INTO transactions (transaction_id, amount, from_account, to_account, transaction_type, description) 
VALUES ('TEST001', 1500.00, 'ACC001', 'ACC002', 'TRANSFER', 'Test transaction');
"
```

**Flow**: INSERT → Enrich → Validate → Settle → Store

## Rules

**Risk**: Amount >$10K (+30), >$5K (+15), HIGH risk (+40), MEDIUM (+20)
**Clearance**: Score >70 (manual), 40-70 (conditional), <40 (auto)

**Redis**: `settlement:{id}` / `rejected:{id}`

## Monitoring

```bash
# Services
docker-compose ps

# Database
SELECT * FROM transactions ORDER BY created_at DESC LIMIT 10;

# Redis
docker exec -it docker-redis-1 redis-cli KEYS "settlement:*"
```

## License
MIT