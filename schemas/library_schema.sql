-- Library Management System DDL Schema
-- Demonstrates complex relationships and various data types

CREATE TABLE authors (
    author_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    birth_date DATE,
    death_date DATE,
    nationality VARCHAR(50),
    biography TEXT,
    website VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE publishers (
    publisher_id INTEGER PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    address TEXT,
    phone VARCHAR(20),
    email VARCHAR(255),
    website VARCHAR(255),
    founded_year INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE books (
    book_id INTEGER PRIMARY KEY,
    isbn VARCHAR(20) UNIQUE NOT NULL,
    title VARCHAR(300) NOT NULL,
    subtitle VARCHAR(300),
    publication_year INTEGER,
    pages INTEGER,
    language VARCHAR(50) DEFAULT 'English',
    description TEXT,
    cover_image_url VARCHAR(500),
    price DECIMAL(8,2),
    publisher_id INTEGER REFERENCES publishers(publisher_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book_authors (
    book_id INTEGER REFERENCES books(book_id),
    author_id INTEGER REFERENCES authors(author_id),
    role VARCHAR(50) DEFAULT 'author',
    PRIMARY KEY (book_id, author_id)
);

CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    parent_genre_id INTEGER REFERENCES genres(genre_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book_genres (
    book_id INTEGER REFERENCES books(book_id),
    genre_id INTEGER REFERENCES genres(genre_id),
    PRIMARY KEY (book_id, genre_id)
);

CREATE TABLE members (
    member_id INTEGER PRIMARY KEY,
    library_card_number VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    date_of_birth DATE,
    membership_type VARCHAR(20) DEFAULT 'standard',
    membership_start_date DATE DEFAULT CURRENT_DATE,
    membership_end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    total_books_borrowed INTEGER DEFAULT 0,
    current_fines DECIMAL(8,2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE library_staff (
    staff_id INTEGER PRIMARY KEY,
    employee_id VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    position VARCHAR(100),
    department VARCHAR(100),
    hire_date DATE DEFAULT CURRENT_DATE,
    salary DECIMAL(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book_copies (
    copy_id INTEGER PRIMARY KEY,
    book_id INTEGER REFERENCES books(book_id),
    barcode VARCHAR(50) UNIQUE NOT NULL,
    acquisition_date DATE DEFAULT CURRENT_DATE,
    condition VARCHAR(20) DEFAULT 'good',
    location VARCHAR(100),
    is_available BOOLEAN DEFAULT TRUE,
    last_maintenance_date DATE,
    notes TEXT
);

CREATE TABLE loans (
    loan_id INTEGER PRIMARY KEY,
    member_id INTEGER REFERENCES members(member_id),
    copy_id INTEGER REFERENCES book_copies(copy_id),
    loan_date DATE DEFAULT CURRENT_DATE,
    due_date DATE NOT NULL,
    return_date DATE,
    fine_amount DECIMAL(8,2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'active',
    processed_by INTEGER REFERENCES library_staff(staff_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE reservations (
    reservation_id INTEGER PRIMARY KEY,
    member_id INTEGER REFERENCES members(member_id),
    book_id INTEGER REFERENCES books(book_id),
    reservation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    priority INTEGER DEFAULT 1,
    status VARCHAR(20) DEFAULT 'pending',
    fulfilled_date TIMESTAMP,
    expires_at TIMESTAMP,
    notes TEXT
);

