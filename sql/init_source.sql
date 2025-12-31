-- Source Database Initialization
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    registration_date TIMESTAMP,
    customer_segment VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    order_date TIMESTAMP,
    status VARCHAR(50),
    payment_method VARCHAR(50),
    subtotal DECIMAL(12,2),
    tax DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2) DEFAULT 0,
    total_price DECIMAL(12,2)
);

-- Insert sample customers
INSERT INTO customers (customer_id, first_name, last_name, email, city, state, country, postal_code, registration_date, customer_segment) VALUES
('CUST001', 'John', 'Smith', 'john.smith@email.com', 'New York', 'NY', 'USA', '10001', '2023-06-15', 'Premium'),
('CUST002', 'Emily', 'Johnson', 'emily.j@email.com', 'Los Angeles', 'CA', 'USA', '90001', '2023-07-20', 'Regular'),
('CUST003', 'Michael', 'Williams', 'm.williams@email.com', 'Chicago', 'IL', 'USA', '60601', '2023-08-10', 'Premium'),
('CUST004', 'Sarah', 'Brown', 'sarah.brown@email.com', 'Houston', 'TX', 'USA', '77001', '2023-09-05', 'Regular'),
('CUST005', 'David', 'Jones', 'david.jones@email.com', 'Phoenix', 'AZ', 'USA', '85001', '2023-10-12', 'New')
ON CONFLICT (customer_id) DO NOTHING;

-- Insert sample products
INSERT INTO products (product_id, name, category, subcategory, brand, price, cost, stock_quantity) VALUES
('PROD001', 'Wireless Bluetooth Headphones', 'Electronics', 'Audio', 'SoundMax', 79.99, 45.00, 150),
('PROD002', 'Organic Green Tea', 'Food & Beverages', 'Tea', 'NatureBrew', 12.99, 6.50, 500),
('PROD003', 'Running Shoes Pro', 'Sports & Outdoors', 'Footwear', 'SpeedRunner', 129.99, 75.00, 200),
('PROD004', 'Stainless Steel Water Bottle', 'Home & Kitchen', 'Drinkware', 'EcoLife', 24.99, 12.00, 300),
('PROD005', 'Smart Watch Series 5', 'Electronics', 'Wearables', 'TechTime', 299.99, 180.00, 100)
ON CONFLICT (product_id) DO NOTHING;

-- Insert sample orders
INSERT INTO orders (order_id, customer_id, order_date, status, payment_method, subtotal, tax, shipping_cost, total_amount) VALUES
('ORD-001', 'CUST001', '2024-01-15 10:30:00', 'completed', 'credit_card', 129.97, 10.40, 5.99, 146.36),
('ORD-002', 'CUST003', '2024-01-16 14:45:00', 'completed', 'paypal', 269.99, 21.60, 0, 291.59),
('ORD-003', 'CUST002', '2024-01-17 09:15:00', 'shipped', 'credit_card', 169.98, 13.60, 7.99, 191.57),
('ORD-004', 'CUST005', '2024-01-18 16:00:00', 'processing', 'debit_card', 76.95, 6.16, 5.99, 89.10),
('ORD-005', 'CUST004', '2024-01-19 11:30:00', 'completed', 'credit_card', 109.97, 8.80, 0, 118.77)
ON CONFLICT (order_id) DO NOTHING;

-- Insert sample order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount, total_price) VALUES
('ORD-001', 'PROD001', 1, 79.99, 0, 79.99),
('ORD-001', 'PROD004', 2, 24.99, 0, 49.98),
('ORD-002', 'PROD005', 1, 299.99, 30.00, 269.99),
('ORD-003', 'PROD003', 1, 129.99, 0, 129.99),
('ORD-003', 'PROD004', 1, 24.99, 0, 24.99),
('ORD-004', 'PROD002', 3, 12.99, 0, 38.97),
('ORD-004', 'PROD004', 1, 24.99, 0, 24.99),
('ORD-005', 'PROD001', 1, 79.99, 0, 79.99),
('ORD-005', 'PROD002', 2, 12.99, 0, 25.98)
ON CONFLICT DO NOTHING;
