import json
import redis
from kafka import KafkaConsumer
from datetime import datetime
from sqlalchemy import create_engine, text

class SettlementConsumer:
    """
    Processes cleared transactions for real-time settlement and balance updates.
    
    This consumer handles the final stage of transaction processing by updating
    account balances, storing settlement results in Redis, and managing rejected
    transactions. It maintains complete audit trails and balance tracking.
    """
    
    def __init__(self, kafka_servers, topic, db_url):
        """
        Initialize the settlement consumer with Kafka, Redis, and database connections.
        
        Args:
            kafka_servers (list): List of Kafka bootstrap servers
            topic (str): Kafka topic to consume from (cleared-transactions)
            db_url (str): PostgreSQL connection string for balance updates
        """
        self.engine = create_engine(db_url)
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='settlement-group'
        )
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
    
    def process_transactions(self):
        """
        Main processing loop for transaction settlement.
        
        Processes different transaction statuses:
        - 'cleared': Executes settlement with balance updates
        - 'rejected'/'requires_manual_review': Stores rejection details
        
        Results are stored in Redis with separate key prefixes:
        - settlement:{txn_id} for successful settlements
        - rejected:{txn_id} for failed transactions
        """
        for message in self.consumer:
            transaction = message.value
            txn_id = transaction.get('transaction_id', transaction['id'])
            status = transaction.get('status')
            
            if status == 'cleared':
                # Process successful settlement
                settlement_result = self.settle_transaction(transaction)
                key = f"settlement:{txn_id}"
                redis_data = {k: str(v) for k, v in settlement_result.items()}
                self.redis_client.hset(key, mapping=redis_data)
                print(f"Settled transaction {txn_id}: {settlement_result['status']}")
                
            elif status in ['rejected', 'requires_manual_review']:
                # Process rejected/failed transactions
                rejection_result = self.process_rejection(transaction)
                key = f"rejected:{txn_id}"
                redis_data = {k: str(v) for k, v in rejection_result.items()}
                self.redis_client.hset(key, mapping=redis_data)
                print(f"Rejected transaction {txn_id}: {rejection_result['rejection_reason']}")
                
            else:
                print(f"Unknown transaction status {txn_id}: {status}")
    
    def settle_transaction(self, transaction):
        """
        Execute settlement processing for a cleared transaction.
        
        Settlement process:
        1. Prepare settlement data with all enriched information
        2. Update account balances atomically
        3. Track before/after balances for audit purposes
        4. Store complete settlement record
        
        Args:
            transaction (dict): Cleared transaction data from Kafka
            
        Returns:
            dict: Complete settlement data including balance changes
        """
        # Store all enriched data from the pipeline
        settlement_data = {
            'transaction_id': transaction.get('transaction_id', transaction['id']),
            'status': 'settled',
            'settlement_time': datetime.now().isoformat(),
            'amount': transaction['amount'],
            'from_account': transaction['from_account'],
            'to_account': transaction['to_account'],
            'transaction_type': transaction.get('transaction_type', 'TRANSFER'),
            'description': transaction.get('description', ''),
            'risk_score': transaction.get('risk_score', 0)
        }
        
        # Add customer information if available
        if 'from_customer' in transaction:
            settlement_data.update({
                'from_customer_name': transaction['from_customer'].get('name', ''),
                'from_customer_risk': transaction['from_customer'].get('risk_level', ''),
                'from_account_type': transaction['from_customer'].get('account_type', ''),
                'from_account_balance': transaction['from_customer'].get('balance', 0)
            })
            
        if 'to_customer' in transaction:
            settlement_data.update({
                'to_customer_name': transaction['to_customer'].get('name', ''),
                'to_customer_risk': transaction['to_customer'].get('risk_level', ''),
                'to_account_type': transaction['to_customer'].get('account_type', ''),
                'to_account_balance': transaction['to_customer'].get('balance', 0)
            })
            
        # Update account balances if settlement is successful
        if settlement_data['status'] == 'settled':
            balance_update = self.update_account_balances(transaction)
            settlement_data['balance_updated'] = str(balance_update['success'])
            settlement_data['balance_update_notes'] = str(balance_update['notes'])
            
            # Add balance tracking if successful
            if balance_update['success']:
                settlement_data.update({
                    'from_balance_before': str(balance_update.get('from_balance_before', 0)),
                    'from_balance_after': str(balance_update.get('from_balance_after', 0)),
                    'to_balance_before': str(balance_update.get('to_balance_before', 0)),
                    'to_balance_after': str(balance_update.get('to_balance_after', 0))
                })
            
        return settlement_data
    
    def update_account_balances(self, transaction):
        """
        Atomically update account balances and track changes.
        
        This method:
        1. Captures balances before the transaction
        2. Debits the source account
        3. Credits the destination account
        4. Updates transaction status to 'settled'
        5. Logs the status change to audit trail
        6. Returns before/after balance information
        
        All operations are performed within a database transaction to ensure
        atomicity and consistency.
        
        Args:
            transaction (dict): Transaction data with amount and account details
            
        Returns:
            dict: Result with success status, notes, and balance tracking:
                - success (bool): Whether the update succeeded
                - notes (str): Description of the operation
                - from_balance_before/after (float): Source account balances
                - to_balance_before/after (float): Destination account balances
        """
        try:
            with self.engine.connect() as conn:
                # Start transaction first
                trans = conn.begin()
                
                try:
                    amount = float(transaction['amount'])
                    from_account = transaction['from_account']
                    to_account = transaction['to_account']
                    
                    # Get balances before transaction (within the same transaction)
                    from_balance_before = conn.execute(text(
                        "SELECT balance FROM accounts WHERE account_number = :account"
                    ), {"account": from_account}).fetchone().balance
                    
                    to_balance_before = conn.execute(text(
                        "SELECT balance FROM accounts WHERE account_number = :account"
                    ), {"account": to_account}).fetchone().balance
                    
                    # Debit from source account
                    conn.execute(text(
                        "UPDATE accounts SET balance = balance - :amount WHERE account_number = :account"
                    ), {"amount": amount, "account": from_account})
                    
                    # Credit to destination account
                    conn.execute(text(
                        "UPDATE accounts SET balance = balance + :amount WHERE account_number = :account"
                    ), {"amount": amount, "account": to_account})
                    
                    # Update transaction status in database
                    conn.execute(text(
                        "UPDATE transactions SET status = 'settled' WHERE transaction_id = :txn_id"
                    ), {"txn_id": transaction.get('transaction_id', transaction['id'])})
                    
                    # Log status change to audit table
                    conn.execute(text(
                        "INSERT INTO transaction_audit (transaction_id, old_status, new_status, changed_by) VALUES (:txn_id, :old_status, :new_status, :changed_by)"
                    ), {
                        "txn_id": transaction.get('transaction_id', transaction['id']),
                        "old_status": 'cleared',
                        "new_status": 'settled',
                        "changed_by": 'settlement_system'
                    })
                    
                    trans.commit()
                    
                    # Calculate after balances
                    from_balance_after = float(from_balance_before) - amount
                    to_balance_after = float(to_balance_before) + amount
                    
                    return {
                        'success': True, 
                        'notes': f'Transferred {amount} from {from_account} to {to_account}',
                        'from_balance_before': float(from_balance_before),
                        'from_balance_after': from_balance_after,
                        'to_balance_before': float(to_balance_before),
                        'to_balance_after': to_balance_after
                    }
                    
                except Exception as e:
                    trans.rollback()
                    return {'success': False, 'notes': f'Balance update failed: {str(e)}'}
                    
        except Exception as e:
            return {'success': False, 'notes': f'Database connection failed: {str(e)}'}
    
    def process_rejection(self, transaction):
        """
        Process and store details for rejected or failed transactions.
        
        Creates a comprehensive rejection record including:
        - Rejection reason and timestamp
        - Customer and account information
        - Available balance at time of rejection
        - Risk score that contributed to rejection
        
        Also updates the transaction status in the database and logs
        the status change to the audit trail.
        
        Args:
            transaction (dict): Rejected transaction data from clearance
            
        Returns:
            dict: Complete rejection record for Redis storage
        """
        rejection_data = {
            'transaction_id': transaction.get('transaction_id', transaction['id']),
            'status': 'rejected',
            'rejection_time': datetime.now().isoformat(),
            'rejection_reason': transaction.get('clearance_notes', 'Unknown rejection reason'),
            'amount': transaction['amount'],
            'from_account': transaction['from_account'],
            'to_account': transaction['to_account'],
            'transaction_type': transaction.get('transaction_type', 'TRANSFER'),
            'description': transaction.get('description', ''),
            'risk_score': transaction.get('risk_score', 0),
            'available_balance': transaction.get('available_balance', 0)
        }
        
        # Add customer information if available
        if 'from_customer' in transaction:
            rejection_data.update({
                'from_customer_name': transaction['from_customer'].get('name', ''),
                'from_customer_risk': transaction['from_customer'].get('risk_level', '')
            })
            
        # Update transaction status in database
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(
                        "UPDATE transactions SET status = :status WHERE transaction_id = :txn_id"
                    ), {
                        "status": transaction.get('status', 'rejected'),
                        "txn_id": transaction.get('transaction_id', transaction['id'])
                    })
                    
                    # Log rejection to audit table
                    conn.execute(text(
                        "INSERT INTO transaction_audit (transaction_id, old_status, new_status, changed_by) VALUES (:txn_id, :old_status, :new_status, :changed_by)"
                    ), {
                        "txn_id": transaction.get('transaction_id', transaction['id']),
                        "old_status": 'cleared',
                        "new_status": transaction.get('status', 'rejected'),
                        "changed_by": 'settlement_system'
                    })
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    raise e
        except Exception as e:
            print(f"Failed to update rejected transaction in DB: {e}")
            
        return rejection_data

if __name__ == "__main__":
    """
    Main entry point for the settlement consumer.
    
    Initializes the consumer with Kafka topic and database connection,
    then starts the continuous processing loop for transaction settlement.
    """
    consumer = SettlementConsumer(
        kafka_servers=['localhost:9092'],
        topic='cleared-transactions',
        db_url="postgresql://user:pass@localhost:5432/financial_db"
    )
    consumer.process_transactions()