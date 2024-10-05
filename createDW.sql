-- drop database  `ELECTRONICA-DW`;


-- create database `ELECTRONICA-DW`;


use `ELECTRONICA-DW`;

drop table if exists sales;
drop table if exists customers;
drop table if exists dates;
drop table if exists suppliers;
drop table if exists stores;
drop table if exists products;

-- Creating the 'products' table
create table products (
    productID INT PRIMARY KEY,
    name VARCHAR(255),
    price FLOAT
);

-- Creating the 'stores' table
create table stores (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(255)
);

-- Creating the 'suppliers' table
create table suppliers (
    supplierName VARCHAR(255) PRIMARY KEY
);

-- Creating the 'dates' table
create table dates (
    dateID VARCHAR(255) PRIMARY KEY,
    year INT,
    month INT,
    week INT,
    day INT,
    dayName VARCHAR(255),
    quarter INT
);

-- Creating the 'customers' table
create table customers (
    customerID INT PRIMARY KEY,
    customerName VARCHAR(255),
    gender VARCHAR(255)
);

-- Creating the 'sales' table with foreign key constraints
create table sales (
    productID INT,
    customerID INT,
    dateID VARCHAR(255),
    storeID INT,
    supplierName VARCHAR(255),
    quantity INT,
    FOREIGN KEY (productID) REFERENCES products(productID),
    FOREIGN KEY (customerID) REFERENCES customers(customerID),
    FOREIGN KEY (dateID) REFERENCES dates(dateID),
    FOREIGN KEY (storeID) REFERENCES stores(storeID),
    FOREIGN KEY (supplierName) REFERENCES suppliers(supplierName)
);


select *from sales;
select *from products;