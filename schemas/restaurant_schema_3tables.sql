-- Restaurant Database Schema (3 Tables)
-- This schema represents a restaurant management system

-- Table 1: Customers
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loyalty_points INTEGER DEFAULT 0,
    is_vip BOOLEAN DEFAULT FALSE,
    address_street VARCHAR(100),
    address_city VARCHAR(50),
    address_state VARCHAR(30),
    address_zip VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Menu Items
CREATE TABLE menu_items (
    item_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50) NOT NULL, -- appetizer, main_course, dessert, beverage
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    is_vegetarian BOOLEAN DEFAULT FALSE,
    is_vegan BOOLEAN DEFAULT FALSE,
    is_gluten_free BOOLEAN DEFAULT FALSE,
    is_spicy BOOLEAN DEFAULT FALSE,
    preparation_time_minutes INTEGER,
    calories INTEGER,
    allergens TEXT, -- comma-separated list of allergens
    is_available BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Orders
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE SET NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending', -- pending, confirmed, preparing, ready, delivered, cancelled
    total_amount DECIMAL(10,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    tip_amount DECIMAL(10,2) DEFAULT 0,
    delivery_address_street VARCHAR(100),
    delivery_address_city VARCHAR(50),
    delivery_address_state VARCHAR(30),
    delivery_address_zip VARCHAR(10),
    delivery_instructions TEXT,
    payment_method VARCHAR(20), -- cash, credit_card, debit_card, digital_wallet
    payment_status VARCHAR(20) DEFAULT 'pending', -- pending, paid, failed, refunded
    estimated_delivery_time TIMESTAMP,
    actual_delivery_time TIMESTAMP,
    special_requests TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_phone ON customers(phone);
CREATE INDEX idx_customers_registration_date ON customers(registration_date);

CREATE INDEX idx_menu_items_category ON menu_items(category);
CREATE INDEX idx_menu_items_price ON menu_items(price);
CREATE INDEX idx_menu_items_available ON menu_items(is_available);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_payment_status ON orders(payment_status);

-- Add some sample data for testing
INSERT INTO customers (first_name, last_name, email, phone, date_of_birth, loyalty_points, is_vip, address_street, address_city, address_state, address_zip) VALUES
('John', 'Smith', 'john.smith@email.com', '555-0101', '1985-03-15', 150, FALSE, '123 Main St', 'New York', 'NY', '10001'),
('Sarah', 'Johnson', 'sarah.johnson@email.com', '555-0102', '1990-07-22', 300, TRUE, '456 Oak Ave', 'Los Angeles', 'CA', '90210'),
('Michael', 'Brown', 'michael.brown@email.com', '555-0103', '1988-11-08', 75, FALSE, '789 Pine Rd', 'Chicago', 'IL', '60601'),
('Emily', 'Davis', 'emily.davis@email.com', '555-0104', '1992-05-14', 200, FALSE, '321 Elm St', 'Houston', 'TX', '77001'),
('David', 'Wilson', 'david.wilson@email.com', '555-0105', '1987-09-30', 500, TRUE, '654 Maple Dr', 'Phoenix', 'AZ', '85001');

INSERT INTO menu_items (name, description, category, price, cost, is_vegetarian, is_vegan, is_gluten_free, is_spicy, preparation_time_minutes, calories, allergens) VALUES
('Caesar Salad', 'Fresh romaine lettuce with parmesan cheese and croutons', 'appetizer', 12.99, 4.50, TRUE, FALSE, FALSE, FALSE, 10, 320, 'dairy,gluten'),
('Grilled Chicken Breast', 'Seasoned chicken breast with herbs and spices', 'main_course', 18.99, 8.75, FALSE, FALSE, TRUE, FALSE, 25, 450, 'none'),
('Chocolate Lava Cake', 'Warm chocolate cake with molten center and vanilla ice cream', 'dessert', 8.99, 3.25, TRUE, FALSE, FALSE, FALSE, 15, 580, 'dairy,eggs,gluten'),
('Margherita Pizza', 'Fresh mozzarella, tomato sauce, and basil on thin crust', 'main_course', 16.99, 6.50, TRUE, FALSE, FALSE, FALSE, 20, 420, 'dairy,gluten'),
('Spicy Buffalo Wings', 'Crispy chicken wings with buffalo sauce and celery', 'appetizer', 14.99, 5.25, FALSE, FALSE, TRUE, TRUE, 18, 380, 'none'),
('Veggie Burger', 'Plant-based patty with lettuce, tomato, and vegan mayo', 'main_course', 15.99, 6.00, TRUE, TRUE, FALSE, FALSE, 12, 350, 'gluten'),
('Tiramisu', 'Classic Italian dessert with coffee and mascarpone', 'dessert', 9.99, 3.75, TRUE, FALSE, FALSE, FALSE, 5, 420, 'dairy,eggs,gluten'),
('Fresh Lemonade', 'House-made lemonade with fresh lemons', 'beverage', 4.99, 1.50, TRUE, TRUE, TRUE, FALSE, 3, 120, 'none');

INSERT INTO orders (customer_id, status, total_amount, tax_amount, tip_amount, delivery_address_street, delivery_address_city, delivery_address_state, delivery_address_zip, payment_method, payment_status, special_requests) VALUES
(1, 'delivered', 45.97, 3.68, 7.00, '123 Main St', 'New York', 'NY', '10001', 'credit_card', 'paid', 'Extra napkins please'),
(2, 'preparing', 32.98, 2.64, 5.00, '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'digital_wallet', 'paid', 'No onions'),
(3, 'ready', 28.99, 2.32, 4.50, '789 Pine Rd', 'Chicago', 'IL', '60601', 'debit_card', 'paid', NULL),
(4, 'pending', 22.99, 1.84, 3.50, '321 Elm St', 'Houston', 'TX', '77001', 'credit_card', 'pending', 'Well done'),
(5, 'cancelled', 18.99, 1.52, 0.00, '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'credit_card', 'refunded', NULL);




