CREATE TABLE IF NOT EXISTS branches (
  id SERIAL,
  Branch VARCHAR(1) UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Supercenters (
  id SERIAL,
  Branch_id INT REFERENCES branches (id),
  City VARCHAR(50) UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Customer_types (
  id SERIAL,
  type VARCHAR(10) UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Customers (
  id SERIAL,
  Type_id INT REFERENCES Customer_types (id),
  Gender VARCHAR(10),
  Rating FLOAT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Product_lines (
  id SERIAL,
  Line VARCHAR(50) UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Products (
  id SERIAL,
  line_id INT REFERENCES Product_lines (id),
  Unit_price FLOAT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Invoices (
  Invoice_id VARCHAR(11) UNIQUE,
  Supercenter_id INT REFERENCES Supercenters (id),
  Customer_id INT REFERENCES Customers (id),
  Product_id INT REFERENCES Products (id),
  Quantity INT,
  Tax FLOAT,
  Total FLOAT,
  Datetime TIMESTAMP,
  Payment VARCHAR(20),
  cogs FLOAT,
  margin_percentage FLOAT,
  income FLOAT,
  PRIMARY KEY (Invoice_id)
);
