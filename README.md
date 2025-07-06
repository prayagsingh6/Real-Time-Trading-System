# Kafka Financial Transaction Pipeline

A real-time financial transaction processing system built with Apache Kafka, PostgreSQL, and Redis. This system demonstrates event-driven architecture for clearance, settlement, and audit trail management in financial services.

## Architecture Overview

```
Database → Kafka Producer → Clearance Consumer → Settlement Consumer → Redis
    ↓              ↓                ↓                    ↓              ↓
PostgreSQL    financial-      cleared-           Balance Updates    Settlement
              transactions   transactions        & Audit Log        Storage
```

## Features

- **Real-time Transaction Processing**: Event-driven pipeline with sub-second latency
- **Risk-based Clearance**: Automated validation with configurable risk scoring
- **Balance Management**: Real-time balance checks and updates with before/after tracking
- **Audit Trail**: Complete transaction lifecycle logging
- **Rejection Handling**: Separate storage and processing for failed transactions
- **Web UIs**: PostgreSQL, Redis, and Kafka monitoring interfaces

## Components

### 1. Database Producer (`src/db_producer.py`)
- Monitors PostgreSQL for new transactions
- Enriches data with customer information and risk scores
- Publishes to `financial-transactions` Kafka topic

### 2. Clearance Consumer (`src/clearance_consumer.py`)
- Validates transactions (balance checks, risk assessment)
- Implements business rules and compliance checks
- Publishes cleared transactions to `cleared-transactions` topic

### 3. Settlement Consumer (`src/settlement_consumer.py`)
- Processes cleared transactions for settlement
- Updates account balances atomically
- Stores results in Redis for fast access
- Handles rejected transactions separately

## Database Schema

### Tables
- **customers**: Customer information and risk profiles
- **accounts**: Account details with real-time balances
- **transactions**: Transaction records with status tracking
- **transaction_audit**: Complete audit trail of status changes

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.8+
- Git

### 1. Clone and Setup
```bash
git clone <repository-url>
cd kafka-financial-poc
pip install -r requirements.txt
```

### 2. Start Infrastructure
```bash
cd docker
docker-compose up -d
```

Wait 60 seconds for all services to initialize.

### 3. Run Pipeline Components
Open 3 separate terminals:

**Terminal 1 - Database Producer:**
```bash
python src/db_producer.py
```

**Terminal 2 - Clearance Consumer:**
```bash
python src/clearance_consumer.py
```

**Terminal 3 - Settlement Consumer:**
```bash
python src/settlement_consumer.py
```

### 4. Access Web Interfaces
- **PostgreSQL Admin**: http://localhost:8080 (admin@admin.com / admin)
- **Redis Commander**: http://localhost:8081
- **Kafka UI**: http://localhost:8082

## Testing the Pipeline

### Add Test Transaction
```bash
docker exec -it docker-postgres-1 psql -U user -d financial_db -c "
INSERT INTO transactions (transaction_id, amount, from_account, to_account, transaction_type, description) 
VALUES ('TEST001', 1500.00, 'ACC001', 'ACC002', 'TRANSFER', 'Test transaction');
"
```

### Monitor Results
1. **Terminal Output**: Watch real-time processing in all 3 terminals
2. **PostgreSQL**: Check transaction status and audit trail
3. **Redis**: View settlement data with balance tracking
4. **Kafka**: Monitor message flow through topics

## Data Flow Examples

### Successful Transaction
```
1. INSERT → transactions table
2. DB Producer → Enriches with customer data, calculates risk score
3. Clearance → Validates balance, approves (status: 'cleared')
4. Settlement → Updates balances, stores in Redis (status: 'settled')
5. Audit → Logs: pending → cleared → settled
```

### Rejected Transaction
```
1. INSERT → transactions table (insufficient funds)
2. DB Producer → Enriches with customer data
3. Clearance → Rejects due to insufficient balance (status: 'rejected')
4. Settlement → Stores rejection details in Redis
5. Audit → Logs: pending → rejected
```

## Configuration

### Risk Scoring Rules
- Amount > $10,000: +30 points
- Amount > $5,000: +15 points
- Customer risk level HIGH: +40 points
- Customer risk level MEDIUM: +20 points

### Clearance Rules
- Risk score > 70: Manual review required
- Risk score 40-70: Cleared with conditions
- Risk score < 40: Auto-cleared
- Amount > $100,000: Manual review required

## Redis Storage Structure

### Settled Transactions
```
Key: settlement:{transaction_id}
Fields: transaction_id, status, amount, from_account, to_account,
        from_balance_before, from_balance_after, to_balance_before, 
        to_balance_after, settlement_time, customer_info...
```

### Rejected Transactions
```
Key: rejected:{transaction_id}
Fields: transaction_id, status, rejection_reason, amount, 
        available_balance, risk_score, rejection_time...
```

## Monitoring and Troubleshooting

### Check Service Status
```bash
docker-compose ps
```

### View Logs
```bash
docker-compose logs kafka
docker-compose logs postgres
docker-compose logs redis
```

### Database Queries
```sql
-- Check transaction status
SELECT transaction_id, status, amount, created_at FROM transactions ORDER BY created_at DESC LIMIT 10;

-- View audit trail
SELECT * FROM transaction_audit ORDER BY changed_at DESC LIMIT 10;

-- Check account balances
SELECT account_number, balance FROM accounts;
```

### Redis Queries
```bash
# List all settlement keys
docker exec -it docker-redis-1 redis-cli KEYS "settlement:*"

# List all rejected transactions
docker exec -it docker-redis-1 redis-cli KEYS "rejected:*"

# View specific transaction
docker exec -it docker-redis-1 redis-cli HGETALL settlement:TXN001
```

## Performance Considerations

- **Kafka Partitioning**: Single partition for demo; scale with multiple partitions
- **Database Connections**: Connection pooling configured for concurrent access
- **Redis Memory**: Monitor memory usage for large transaction volumes
- **Transaction Atomicity**: Database transactions ensure data consistency

## Security Features

- **Input Validation**: Amount and account validation
- **Risk Assessment**: Multi-factor risk scoring
- **Audit Trail**: Complete transaction lifecycle tracking
- **Balance Verification**: Real-time balance checks before processing

## Extending the System

### Adding New Validation Rules
1. Modify `clearance_consumer.py` validation logic
2. Update risk scoring in `db_producer.py`
3. Add new audit fields as needed

### Adding New Data Sources
1. Create new producer following `db_producer.py` pattern
2. Publish to existing or new Kafka topics
3. Update consumers to handle new message formats

### Scaling Considerations
- Add Kafka partitions for parallel processing
- Implement consumer groups for load distribution
- Use Redis clustering for high availability
- Consider database read replicas for reporting

## License

MIT License - See LICENSE file for details.