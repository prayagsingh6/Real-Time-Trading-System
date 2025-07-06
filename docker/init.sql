-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    risk_level VARCHAR(20) DEFAULT 'LOW',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Accounts table
CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    account_number VARCHAR(50) UNIQUE NOT NULL,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    account_type VARCHAR(20) NOT NULL,
    balance DECIMAL(15,2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table (enhanced)
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    from_account VARCHAR(50) REFERENCES accounts(account_number),
    to_account VARCHAR(50) REFERENCES accounts(account_number),
    transaction_type VARCHAR(20) DEFAULT 'TRANSFER',
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending'
);

-- Transaction audit log
CREATE TABLE IF NOT EXISTS transaction_audit (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) REFERENCES transactions(transaction_id),
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    changed_by VARCHAR(50),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample customers
INSERT INTO customers (customer_id, name, email, risk_level) VALUES
('CUST001', 'John Doe', 'john@example.com', 'LOW'),
('CUST002', 'Jane Smith', 'jane@example.com', 'MEDIUM'),
('CUST003', 'Bob Johnson', 'bob@example.com', 'HIGH'),
('CUST004', 'Alice Brown', 'alice@example.com', 'LOW');

-- Insert sample accounts
INSERT INTO accounts (account_number, customer_id, account_type, balance) VALUES
('ACC001', 'CUST001', 'CHECKING', 50000.00),
('ACC002', 'CUST002', 'SAVINGS', 25000.00),
('ACC003', 'CUST003', 'CHECKING', 75000.00),
('ACC004', 'CUST004', 'SAVINGS', 30000.00),
('ACC005', 'CUST001', 'SAVINGS', 100000.00),
('ACC006', 'CUST002', 'CHECKING', 15000.00);

-- Insert sample transactions
INSERT INTO transactions (transaction_id, amount, from_account, to_account, transaction_type, description) VALUES
('TXN001', 1000.00, 'ACC001', 'ACC002', 'TRANSFER', 'Monthly payment'),
('TXN002', 2500.50, 'ACC003', 'ACC004', 'TRANSFER', 'Investment transfer'),
('TXN003', 750.25, 'ACC005', 'ACC006', 'TRANSFER', 'Savings withdrawal');