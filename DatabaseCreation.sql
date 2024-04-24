-- DDL
CREATE TABLE Customers (
    id SERIAL PRIMARY KEY,
    type VARCHAR(7),
    gender VARCHAR(6),
    UNIQUE (type, gender)
);

CREATE TABLE Products (
    id SERIAL PRIMARY KEY,
    line VARCHAR(255),
    price DECIMAL(10, 2),
    UNIQUE (line, price)
);

CREATE TABLE StoreBranches (
    id SERIAL PRIMARY KEY,
    code CHAR(1) UNIQUE,
    city VARCHAR(255)
);

CREATE TABLE Times (
    id SERIAL PRIMARY KEY,
    date DATE,
    time TIME,
    day VARCHAR(9),
    month VARCHAR(9),
    part_of_day VARCHAR(9)
    UNIQUE (date, time, day, month, part_of_day)
);

CREATE TABLE Transactions (
    invoice_id VARCHAR(11) PRIMARY KEY,
    branch_id SERIAL,
    customer_id INT,
    time_id INT,
    payment VARCHAR(11),
    cost_of_goods_sold DECIMAL(10, 2),
    gross_margin_percentage DECIMAL(5, 2),
    gross_income DECIMAL(10, 2),
    customer_rating DECIMAL(3, 1),
    FOREIGN KEY (branch_id) REFERENCES StoreBranches(id),
    FOREIGN KEY (customer_id) REFERENCES Customers(id),
    FOREIGN KEY (time_id) REFERENCES Times(id)
);

CREATE TABLE Sales (
    id SERIAL PRIMARY KEY,
    transaction_invoice_id VARCHAR(11),
    quantity INT,
    total DECIMAL(10, 2),
    value_added_tax DECIMAL(10, 2),
    FOREIGN KEY (transaction_invoice_id) REFERENCES Transactions(invoice_id)
);

-- DML
CREATE TABLE Staging_Transactions (
    invoice_id VARCHAR(20),
    branch CHAR(1),
    city VARCHAR(255),
    customer_type VARCHAR(7),
    gender VARCHAR(6),
    product_line VARCHAR(255),
    unit_price DECIMAL(10, 2),
    quantity INT,
    vat DECIMAL(10, 2),
    total DECIMAL(10, 2),
    dtme TIMESTAMP,
    tme TIME,
    payment_method VARCHAR(11),
    cogs DECIMAL(10, 2),
    gross_margin_pct DECIMAL(5, 2),
    gross_income DECIMAL(10, 2),
    rating DECIMAL(3, 1),
    time_of_day VARCHAR(9),
    day_name VARCHAR(9),
    month_name VARCHAR(9)
);

-- Next line is run in the terminal to be able to access csv file from my desktop
\copy Staging_Transactions FROM '/Users/nicolegiron/Desktop/School/stage 2/WalmartSQL repository..csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');

/*
-- I know that this is the SQL query for import data but I don't know how to import from the server instead of the local user
COPY Staging_Transactions FROM 'WalmartSQL_repository.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');

*/

INSERT INTO StoreBranches (code, city)
SELECT DISTINCT branch, city FROM Staging_Transactions
ON CONFLICT (code) DO NOTHING;

INSERT INTO Customers (type, gender)
SELECT DISTINCT customer_type, gender FROM Staging_Transactions
ON CONFLICT (type, gender) DO NOTHING; 

INSERT INTO Products (line, price)
SELECT DISTINCT product_line, unit_price FROM Staging_Transactions
ON CONFLICT (line, price) DO NOTHING;

INSERT INTO Times (date, time, day, month, part_of_day)
SELECT DISTINCT dtme::date, tme, day_name, month_name, time_of_day FROM Staging_Transactions
ON CONFLICT (date, time, day, month, part_of_day) DO NOTHING;

INSERT INTO Transactions (invoice_id, branch_id, customer_id, time_id, payment, cost_of_goods_sold, gross_margin_percentage, gross_income, customer_rating)
SELECT s.invoice_id, 
       b.id AS branch_id,
       c.id AS customer_id,
       t.id AS time_id,
       s.payment_method,
       s.cogs,
       s.gross_margin_pct,
       s.gross_income,
       s.rating
FROM Staging_Transactions s
JOIN StoreBranches b ON s.branch = b.code
JOIN Customers c ON s.customer_type = c.type AND s.gender = c.gender
JOIN Times t ON s.dtme::date = t.date AND s.tme = t.time AND s.day_name = t.day AND s.month_name = t.month;

INSERT INTO Sales (transaction_invoice_id, quantity, total, value_added_tax)
SELECT
    invoice_id,
    quantity,
    total,
    vat
FROM Staging_Transactions;

DROP TABLE Staging_Transactions;

SELECT tablename
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
