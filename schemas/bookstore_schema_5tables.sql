-- Bookstore Database Schema (5 Tables)
-- This schema represents a comprehensive bookstore management system

-- Table 1: Authors
CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    middle_name VARCHAR(50),
    date_of_birth DATE,
    date_of_death DATE,
    nationality VARCHAR(50),
    biography TEXT,
    website VARCHAR(200),
    email VARCHAR(100),
    awards TEXT, -- comma-separated list of awards
    genres TEXT, -- comma-separated list of genres they write in
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Publishers
CREATE TABLE publishers (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    founded_year INTEGER,
    headquarters_city VARCHAR(50),
    headquarters_country VARCHAR(50),
    website VARCHAR(200),
    contact_email VARCHAR(100),
    contact_phone VARCHAR(20),
    description TEXT,
    specialties TEXT, -- comma-separated list of publishing specialties
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Books
CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    isbn VARCHAR(20) UNIQUE NOT NULL,
    title VARCHAR(200) NOT NULL,
    subtitle VARCHAR(200),
    author_id INTEGER REFERENCES authors(author_id) ON DELETE SET NULL,
    publisher_id INTEGER REFERENCES publishers(publisher_id) ON DELETE SET NULL,
    publication_date DATE,
    edition VARCHAR(20),
    language VARCHAR(20) DEFAULT 'English',
    page_count INTEGER,
    format VARCHAR(20), -- hardcover, paperback, ebook, audiobook
    genre VARCHAR(50),
    subgenre VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    description TEXT,
    table_of_contents TEXT,
    target_audience VARCHAR(50), -- children, young_adult, adult, academic
    reading_level VARCHAR(20), -- beginner, intermediate, advanced
    is_bestseller BOOLEAN DEFAULT FALSE,
    is_featured BOOLEAN DEFAULT FALSE,
    stock_quantity INTEGER DEFAULT 0,
    reorder_threshold INTEGER DEFAULT 10,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: Customers
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    membership_type VARCHAR(20) DEFAULT 'regular', -- regular, premium, vip
    membership_expiry_date DATE,
    loyalty_points INTEGER DEFAULT 0,
    preferred_genres TEXT, -- comma-separated list of preferred genres
    reading_preferences TEXT, -- format preferences, etc.
    address_street VARCHAR(100),
    address_city VARCHAR(50),
    address_state VARCHAR(30),
    address_zip VARCHAR(10),
    address_country VARCHAR(50) DEFAULT 'USA',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 5: Orders
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE SET NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending', -- pending, confirmed, processing, shipped, delivered, cancelled, returned
    total_amount DECIMAL(10,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    shipping_amount DECIMAL(10,2) DEFAULT 0,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    payment_method VARCHAR(20), -- cash, credit_card, debit_card, digital_wallet, store_credit
    payment_status VARCHAR(20) DEFAULT 'pending', -- pending, paid, failed, refunded, partially_refunded
    shipping_address_street VARCHAR(100),
    shipping_address_city VARCHAR(50),
    shipping_address_state VARCHAR(30),
    shipping_address_zip VARCHAR(10),
    shipping_address_country VARCHAR(50) DEFAULT 'USA',
    shipping_method VARCHAR(30), -- standard, express, overnight, pickup
    tracking_number VARCHAR(50),
    estimated_delivery_date DATE,
    actual_delivery_date DATE,
    gift_wrap BOOLEAN DEFAULT FALSE,
    gift_message TEXT,
    special_instructions TEXT,
    loyalty_points_earned INTEGER DEFAULT 0,
    loyalty_points_used INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_authors_name ON authors(last_name, first_name);
CREATE INDEX idx_authors_nationality ON authors(nationality);
CREATE INDEX idx_authors_active ON authors(is_active);

CREATE INDEX idx_publishers_name ON publishers(name);
CREATE INDEX idx_publishers_active ON publishers(is_active);

CREATE INDEX idx_books_isbn ON books(isbn);
CREATE INDEX idx_books_title ON books(title);
CREATE INDEX idx_books_author_id ON books(author_id);
CREATE INDEX idx_books_publisher_id ON books(publisher_id);
CREATE INDEX idx_books_genre ON books(genre);
CREATE INDEX idx_books_price ON books(price);
CREATE INDEX idx_books_stock ON books(stock_quantity);
CREATE INDEX idx_books_active ON books(is_active);

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_phone ON customers(phone);
CREATE INDEX idx_customers_membership ON customers(membership_type);
CREATE INDEX idx_customers_registration_date ON customers(registration_date);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_payment_status ON orders(payment_status);

-- Add some sample data for testing
INSERT INTO authors (first_name, last_name, middle_name, date_of_birth, nationality, biography, genres, awards) VALUES
('J.K.', 'Rowling', NULL, '1965-07-31', 'British', 'Author of the Harry Potter series', 'fantasy,young_adult', 'Hugo Award,National Book Award'),
('Stephen', 'King', NULL, '1947-09-21', 'American', 'Master of horror and suspense fiction', 'horror,thriller,mystery', 'Bram Stoker Award,World Fantasy Award'),
('Agatha', 'Christie', NULL, '1890-09-15', 'British', 'Queen of mystery novels', 'mystery,crime', 'Edgar Award,Grand Master Award'),
('Toni', 'Morrison', NULL, '1931-02-18', 'American', 'Nobel Prize-winning novelist', 'literary_fiction,historical_fiction', 'Nobel Prize in Literature,Pulitzer Prize'),
('Haruki', 'Murakami', NULL, '1949-01-12', 'Japanese', 'Contemporary Japanese writer', 'literary_fiction,magical_realism', 'Franz Kafka Prize,Jerusalem Prize');

INSERT INTO publishers (name, founded_year, headquarters_city, headquarters_country, website, specialties) VALUES
('Penguin Random House', 2013, 'New York', 'USA', 'penguinrandomhouse.com', 'fiction,non-fiction,children_books'),
('HarperCollins', 1989, 'New York', 'USA', 'harpercollins.com', 'fiction,non-fiction,educational'),
('Macmillan Publishers', 1843, 'London', 'UK', 'macmillan.com', 'academic,professional,children_books'),
('Simon & Schuster', 1924, 'New York', 'USA', 'simonandschuster.com', 'fiction,non-fiction,biography'),
('Hachette Book Group', 2006, 'New York', 'USA', 'hachettebookgroup.com', 'fiction,non-fiction,graphic_novels');

INSERT INTO books (isbn, title, subtitle, author_id, publisher_id, publication_date, edition, language, page_count, format, genre, price, cost, description, target_audience, stock_quantity, is_bestseller) VALUES
('978-0-439-02348-1', 'Harry Potter and the Philosopher''s Stone', 'The Boy Who Lived', 1, 1, '1997-06-26', '1st', 'English', 223, 'hardcover', 'fantasy', 19.99, 8.50, 'The first book in the Harry Potter series', 'young_adult', 45, TRUE),
('978-0-671-02700-1', 'The Shining', 'A Novel', 2, 2, '1977-01-28', '1st', 'English', 447, 'paperback', 'horror', 16.99, 6.75, 'A psychological horror novel about isolation and madness', 'adult', 32, TRUE),
('978-0-06-207348-8', 'Murder on the Orient Express', NULL, 3, 2, '1934-01-01', 'Reprint', 'English', 288, 'paperback', 'mystery', 14.99, 5.25, 'Hercule Poirot investigates a murder on a train', 'adult', 28, TRUE),
('978-1-4000-3341-0', 'Beloved', 'A Novel', 4, 1, '1987-09-02', '1st', 'English', 324, 'hardcover', 'literary_fiction', 18.99, 7.50, 'A powerful novel about slavery and its aftermath', 'adult', 15, TRUE),
('978-0-307-26530-2', 'Norwegian Wood', NULL, 5, 3, '1987-09-04', '1st', 'English', 296, 'paperback', 'literary_fiction', 15.99, 6.00, 'A coming-of-age story set in 1960s Tokyo', 'adult', 22, FALSE),
('978-0-439-02349-8', 'Harry Potter and the Chamber of Secrets', 'The Heir of Slytherin', 1, 1, '1998-07-02', '1st', 'English', 251, 'hardcover', 'fantasy', 19.99, 8.50, 'The second book in the Harry Potter series', 'young_adult', 38, TRUE),
('978-0-671-02701-8', 'Carrie', 'A Novel', 2, 2, '1974-04-05', '1st', 'English', 199, 'paperback', 'horror', 15.99, 6.25, 'A telekinetic teenager seeks revenge', 'adult', 25, FALSE),
('978-0-06-207349-5', 'Death on the Nile', NULL, 3, 2, '1937-11-01', 'Reprint', 'English', 288, 'paperback', 'mystery', 14.99, 5.25, 'Hercule Poirot solves a murder on a Nile cruise', 'adult', 20, FALSE);

INSERT INTO customers (first_name, last_name, email, phone, date_of_birth, membership_type, loyalty_points, preferred_genres, address_street, address_city, address_state, address_zip) VALUES
('Alice', 'Johnson', 'alice.johnson@email.com', '555-1001', '1985-03-15', 'premium', 250, 'fantasy,mystery', '123 Book Lane', 'Boston', 'MA', '02101'),
('Bob', 'Smith', 'bob.smith@email.com', '555-1002', '1990-07-22', 'regular', 120, 'horror,thriller', '456 Reading Rd', 'Seattle', 'WA', '98101'),
('Carol', 'Williams', 'carol.williams@email.com', '555-1003', '1988-11-08', 'vip', 450, 'literary_fiction,historical_fiction', '789 Novel Ave', 'Austin', 'TX', '73301'),
('David', 'Brown', 'david.brown@email.com', '555-1004', '1992-05-14', 'regular', 85, 'science_fiction,fantasy', '321 Story St', 'Denver', 'CO', '80201'),
('Eva', 'Davis', 'eva.davis@email.com', '555-1005', '1987-09-30', 'premium', 300, 'mystery,crime', '654 Chapter Dr', 'Portland', 'OR', '97201');

INSERT INTO orders (customer_id, status, total_amount, tax_amount, shipping_amount, payment_method, payment_status, shipping_address_street, shipping_address_city, shipping_address_state, shipping_address_zip, shipping_method, loyalty_points_earned, loyalty_points_used) VALUES
(1, 'delivered', 54.97, 4.40, 5.99, 'credit_card', 'paid', '123 Book Lane', 'Boston', 'MA', '02101', 'standard', 55, 0),
(2, 'shipped', 32.98, 2.64, 0.00, 'digital_wallet', 'paid', '456 Reading Rd', 'Seattle', 'WA', '98101', 'express', 33, 10),
(3, 'processing', 48.98, 3.92, 7.99, 'credit_card', 'paid', '789 Novel Ave', 'Austin', 'TX', '73301', 'overnight', 49, 25),
(4, 'pending', 19.99, 1.60, 0.00, 'store_credit', 'pending', '321 Story St', 'Denver', 'CO', '80201', 'pickup', 20, 0),
(5, 'cancelled', 15.99, 1.28, 0.00, 'credit_card', 'refunded', '654 Chapter Dr', 'Portland', 'OR', '97201', 'standard', 0, 0);




