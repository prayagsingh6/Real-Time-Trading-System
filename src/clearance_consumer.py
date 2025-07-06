import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from sqlalchemy import create_engine, text

class ClearanceConsumer:
    """
    Processes transactions for clearance validation and compliance checks.
    
    This consumer validates transactions by checking account balances, applying
    risk-based business rules, and determining clearance status. It publishes
    cleared transactions to the next stage and logs all status changes.
    """
    
    def __init__(self, kafka_servers, input_topic, output_topic, db_url):
        """
        Initialize the clearance consumer with Kafka and database connections.
        
        Args:
            kafka_servers (list): List of Kafka bootstrap servers
            input_topic (str): Kafka topic to consume from (financial-transactions)
            output_topic (str): Kafka topic to publish to (cleared-transactions)
            db_url (str): PostgreSQL connection string for balance checks
        """
        self.engine = create_engine(db_url)
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='clearance-group'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.output_topic = output_topic
    
    def process_clearance(self):
        """
        Main processing loop for transaction clearance validation.
        
        Continuously consumes messages from the input topic, validates each
        transaction through business rules and balance checks, then publishes
        the result to the output topic with updated status.
        """
        for message in self.consumer:
            transaction = message.value
            
            # Clearance validation
            cleared_transaction = self.validate_and_clear(transaction)
            
            # Send to settlement topic
            self.producer.send(self.output_topic, cleared_transaction)
            txn_id = transaction.get('transaction_id', transaction['id'])
            print(f"Cleared transaction {txn_id}")
    
    def validate_and_clear(self, transaction):
        """
        Validate transaction against business rules and balance requirements.
        
        Validation process:
        1. Check account balance sufficiency
        2. Validate transaction amount (positive, within limits)
        3. Apply risk-based clearance rules
        4. Log status changes to audit trail
        
        Clearance rules:
        - Risk score > 70: Manual review required
        - Risk score 40-70: Cleared with conditions
        - Risk score < 40: Auto-cleared
        - Amount > $100,000: Manual review required
        
        Args:
            transaction (dict): Transaction data from Kafka message
            
        Returns:
            dict: Transaction with updated status and clearance information
        """
        # Check account balances first
        balance_check = self.check_account_balance(transaction)
        if not balance_check['valid']:
            transaction['status'] = 'rejected'
            transaction['clearance_notes'] = balance_check['reason']
            transaction['clearance_time'] = datetime.now().isoformat()
            return transaction
        
        # Enhanced clearance validation using enriched data
        risk_score = transaction.get('risk_score', 0)
        amount = transaction.get('amount', 0)
        
        # Sanity checks
        if amount <= 0:
            transaction['status'] = 'rejected'
            transaction['clearance_notes'] = 'Invalid amount'
        elif amount > 100000:  # Large transaction limit
            transaction['status'] = 'requires_manual_review'
            transaction['clearance_notes'] = 'Amount exceeds daily limit'
        elif risk_score > 70:
            transaction['status'] = 'requires_manual_review'
            transaction['clearance_notes'] = 'High risk - manual review required'
        elif risk_score > 40:
            transaction['status'] = 'cleared_with_conditions'
            transaction['clearance_notes'] = 'Medium risk - additional monitoring'
        else:
            transaction['status'] = 'cleared'
            transaction['clearance_notes'] = 'All validations passed'
            
        transaction['clearance_time'] = datetime.now().isoformat()
        transaction['cleared_by'] = 'automated_system'
        transaction['available_balance'] = balance_check['balance']
        
        # Log status change to audit table
        self.log_status_change(
            transaction.get('transaction_id', transaction['id']),
            'pending',
            transaction['status'],
            'clearance_system'
        )
        
        return transaction
    
    def check_account_balance(self, transaction):
        """
        Verify that the source account has sufficient balance for the transaction.
        
        Args:
            transaction (dict): Transaction data containing from_account and amount
            
        Returns:
            dict: Validation result with keys:
                - valid (bool): Whether balance is sufficient
                - reason (str): Explanation of the result
                - balance (float): Current account balance
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT balance FROM accounts WHERE account_number = :account"
                ), {"account": transaction['from_account']})
                
                row = result.fetchone()
                if not row:
                    return {'valid': False, 'reason': 'From account not found', 'balance': 0}
                
                balance = float(row.balance)
                amount = float(transaction['amount'])
                
                if balance < amount:
                    return {
                        'valid': False, 
                        'reason': f'Insufficient funds. Available: {balance}, Required: {amount}',
                        'balance': balance
                    }
                
                return {'valid': True, 'reason': 'Sufficient balance', 'balance': balance}
                
        except Exception as e:
            return {'valid': False, 'reason': f'Balance check failed: {str(e)}', 'balance': 0}
    
    def log_status_change(self, transaction_id, old_status, new_status, changed_by):
        """
        Record transaction status changes in the audit trail.
        
        Args:
            transaction_id (str): Unique transaction identifier
            old_status (str): Previous transaction status
            new_status (str): New transaction status
            changed_by (str): System component making the change
        """
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(
                        "INSERT INTO transaction_audit (transaction_id, old_status, new_status, changed_by) VALUES (:txn_id, :old_status, :new_status, :changed_by)"
                    ), {
                        "txn_id": transaction_id,
                        "old_status": old_status,
                        "new_status": new_status,
                        "changed_by": changed_by
                    })
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    print(f"Failed to log audit entry: {e}")
        except Exception as e:
            print(f"Audit logging failed: {e}")

if __name__ == "__main__":
    """
    Main entry point for the clearance consumer.
    
    Initializes the consumer with Kafka topics and database connection,
    then starts the continuous processing loop for transaction validation.
    """
    consumer = ClearanceConsumer(
        kafka_servers=['localhost:9092'],
        input_topic='financial-transactions',
        output_topic='cleared-transactions',
        db_url="postgresql://user:pass@localhost:5432/financial_db"
    )
    consumer.process_clearance()