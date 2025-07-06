import json
import time
from kafka import KafkaProducer
from sqlalchemy import create_engine, text
from datetime import datetime

class DatabaseProducer:
    """
    Monitors PostgreSQL database for new transactions and publishes enriched data to Kafka.
    
    This producer performs data transformation by joining transaction data with customer
    and account information, calculates risk scores, and publishes enriched messages
    to the financial-transactions Kafka topic.
    """
    
    def __init__(self, db_url, kafka_servers, topic):
        """
        Initialize the database producer with connection parameters.
        
        Args:
            db_url (str): PostgreSQL connection string
            kafka_servers (list): List of Kafka bootstrap servers
            topic (str): Kafka topic name for publishing transactions
        """
        self.engine = create_engine(db_url)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        # Start from the beginning to pick up existing data
        self.last_id = self.get_last_processed_id()
    
    def listen_for_changes(self):
        """
        Continuously poll the database for new transactions and publish to Kafka.
        
        This method:
        1. Queries for new transactions with enriched customer/account data
        2. Calculates risk scores based on transaction and customer attributes
        3. Publishes enriched transaction data to Kafka topic
        4. Maintains cursor position to avoid reprocessing transactions
        
        The polling runs indefinitely with 1-second intervals.
        """
        while True:
            try:
                with self.engine.connect() as conn:
                    # Enhanced query with joins for data transformation
                    result = conn.execute(text("""
                        SELECT 
                            t.id, t.transaction_id, t.amount, t.from_account, t.to_account,
                            t.transaction_type, t.description, t.created_at, t.status,
                            fc.name as from_customer_name, fc.risk_level as from_risk_level,
                            tc.name as to_customer_name, tc.risk_level as to_risk_level,
                            fa.account_type as from_account_type, fa.balance as from_balance,
                            ta.account_type as to_account_type, ta.balance as to_balance
                        FROM transactions t
                        LEFT JOIN accounts fa ON t.from_account = fa.account_number
                        LEFT JOIN accounts ta ON t.to_account = ta.account_number
                        LEFT JOIN customers fc ON fa.customer_id = fc.customer_id
                        LEFT JOIN customers tc ON ta.customer_id = tc.customer_id
                        WHERE t.id > :last_id 
                        ORDER BY t.id
                    """), {"last_id": self.last_id})
                    
                    for row in result:
                        # Handle missing transaction_id for old records
                        txn_id = row.transaction_id or f"TXN_{row.id:06d}"
                        
                        # Enriched transaction data with transformation
                        transaction_data = {
                            'id': row.id,
                            'transaction_id': txn_id,
                            'amount': float(row.amount),
                            'from_account': row.from_account,
                            'to_account': row.to_account,
                            'transaction_type': row.transaction_type,
                            'description': row.description,
                            'timestamp': row.created_at.isoformat(),
                            'status': row.status,
                            'from_customer': {
                                'name': row.from_customer_name,
                                'risk_level': row.from_risk_level,
                                'account_type': row.from_account_type,
                                'balance': float(row.from_balance) if row.from_balance else 0
                            },
                            'to_customer': {
                                'name': row.to_customer_name,
                                'risk_level': row.to_risk_level,
                                'account_type': row.to_account_type,
                                'balance': float(row.to_balance) if row.to_balance else 0
                            },
                            'risk_score': self.calculate_risk_score(row)
                        }
                        
                        self.producer.send(self.topic, transaction_data)
                        self.last_id = row.id
                        print(f"Sent enriched transaction {txn_id} to Kafka")
                
                time.sleep(1)  # Poll every second
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)
    
    def calculate_risk_score(self, row):
        """
        Calculate transaction risk score based on amount and customer risk levels.
        
        Risk scoring rules:
        - Amount > $10,000: +30 points
        - Amount > $5,000: +15 points  
        - Customer risk level HIGH: +40 points
        - Customer risk level MEDIUM: +20 points
        
        Args:
            row: Database row containing transaction and customer data
            
        Returns:
            int: Risk score (0-100, capped at 100)
        """
        score = 0
        
        # High amount transactions
        if row.amount > 10000:
            score += 30
        elif row.amount > 5000:
            score += 15
            
        # Risk level of customers
        if row.from_risk_level == 'HIGH' or row.to_risk_level == 'HIGH':
            score += 40
        elif row.from_risk_level == 'MEDIUM' or row.to_risk_level == 'MEDIUM':
            score += 20
            
        return min(score, 100)  # Cap at 100
    
    def get_last_processed_id(self):
        """
        Get the last processed transaction ID to determine starting point.
        
        For this implementation, always returns 0 to process all existing
        transactions from the beginning.
        
        Returns:
            int: Transaction ID to start processing from (0 = from beginning)
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) as count FROM transactions"))
                count = result.fetchone().count
                print(f"Found {count} existing transactions in database")
                return 0  # Start from beginning to process all data
        except Exception as e:
            print(f"Error getting last ID: {e}")
            return 0

if __name__ == "__main__":
    """
    Main entry point for the database producer.
    
    Initializes the producer with database and Kafka connection parameters,
    then starts the continuous polling loop for new transactions.
    """
    producer = DatabaseProducer(
        db_url="postgresql://user:pass@localhost:5432/financial_db",
        kafka_servers=['localhost:9092'],
        topic='financial-transactions'
    )
    producer.listen_for_changes()