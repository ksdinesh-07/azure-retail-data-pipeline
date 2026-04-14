-- MySQL Sample Data for Retail Project
CREATE DATABASE IF NOT EXISTS onprem_retail_db;
USE onprem_retail_db;

-- Customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    customer_segment VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    registration_date DATE
);

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, customer_segment, registration_date) VALUES
('John', 'Smith', 'john.smith@email.com', 'Premium', '2024-01-15'),
('Emma', 'Johnson', 'emma.j@email.com', 'Gold', '2024-01-20'),
('Michael', 'Brown', 'michael.brown@email.com', 'Regular', '2024-02-01');

-- Orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Insert sample orders
INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES
(1, '2024-01-15', 1387.99, 'Delivered'),
(2, '2024-01-20', 36.79, 'Delivered'),
(3, '2024-02-01', 274.99, 'Shipped');

-- Products table
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2),
    cost DECIMAL(10,2)
);

-- Insert sample products
INSERT INTO products (product_name, category, unit_price, cost) VALUES
('Laptop Pro 15"', 'Electronics', 1299.99, 800.00),
('Wireless Mouse', 'Electronics', 29.99, 15.00),
('Desk Chair', 'Furniture', 249.99, 120.00);
