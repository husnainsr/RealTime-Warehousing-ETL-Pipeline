
use `ELECTRONICA-DW`;


-- Query 1 

-- Present total sales of all products supplied by each supplier with respect to quarter and
-- month using drill down concept.
select sales.supplierName,dates.quarter ,dates.month ,SUM(sales.quantity * products.price) as TotalSales from sales
inner join dates on dates.dateID = sales.dateID
inner join products on products.productID = sales.productID
group by sales.supplierName, dates.quarter ,dates.month
order by sales.supplierName;


-- Query 2
-- Find total sales of product with respect to month using feature of rollup on month and
-- feature of dicing on supplier with name "DJI" and Year as "2019". You will use the
-- grouping sets feature to achieve rollup. Your output should be sequentially ordered
-- according to product and month.

select products.name, dates.year, dates.month , SUM(products.price*sales.quantity) as TotalSales from sales
inner join dates on dates.dateID = sales.dateID
inner join products on products.productID = sales.productID
where sales.supplierName = "DJI" and dates.year = 2019
group by products.name , dates.year ,dates.month
order by products.name, dates.month;

-- query 3
-- Find the 5 most popular products sold over the weekends.   
select products.name,  SUM(sales.quantity) as QuantitySold from sales
inner join dates on dates.dateID = sales.DateID
inner join products on products.productID = sales.productID
where dates.dayName = "SATURDAY" or dates.dayName = "SUNDAY"
group by products.name
order by QuantitySold DESC LIMIT 5;

-- query 4
-- Present the quarterly sales of each product for 2019 along with its total yearly sales.
-- Note: each quarter sale must be a column and yearly sale as well. Order result according
-- to product
Select products.productID , products.name,
SUM(case when dates.quarter = 1 then (sales.quantity*products.price) else 0 end) as Q1,
SUM(case when dates.quarter = 2 then (sales.quantity*products.price) else 0 end) as Q2,
SUM(case when dates.quarter = 3 then (sales.quantity*products.price) else 0 end) as Q3,
SUM(case when dates.quarter = 4 then (sales.quantity*products.price) else 0 end) as Q4,
SUM(products.price*sales.quantity) as YearlySales
from sales
inner join dates on dates.dateID = sales.dateID
inner join products on products.productID = sales.productID
group by products.productID,products.name
order by products.productID;


-- Query 5
-- different ids are assigned to same suppliers
use masterdata;
select supplierID, supplierName from master_data
where supplierName = "Apple Inc.";

-- different suppliers are assigned to same ids
select supplierID,supplierName from master_data
where supplierID =19 ;


-- Query 6 
-- Create a materialised view with the name “STOREANALYSIS_MV” that presents the
-- product-wise sales analysis for each store.
-- drop view STOREANALYSIS_VIEW;
-- step 1 create simple view
create view  STOREANALYSIS_VIEW as
select stores.storeID, products.productID, sum(products.price * sales.quantity) as storeTotal
from sales
inner join products on products.productID = sales.productID
inner join stores on stores.storeID = sales.storeID
group by stores.storeID, products.productID;
-- step 2 create table to store data
create table STOREANALYSIS_MV as
select *from  STOREANALYSIS_VIEW;
-- step 3 refreshing the data
-- truncate table STOREANALYSIS_MV;
-- insert into STOREANALYSIS_MV select * from STOREANALYSIS_VIEW;

select *from STOREANALYSIS_MV;


-- query 7
-- Use the concept of Slicing calculate the total sales for the store “Tech Haven”and product
-- combination over the months.

select dates.month,products.name , SUM(sales.quantity*products.price) as TotalSales
from sales
inner join dates on dates.dateID= sales.dateID
inner join products on products.productID = sales.productID
inner join stores on stores.storeID = sales.storeID
where stores.storeName = "Tech Haven" 
group by dates.month,products.name
order by dates.month, products.name;

-- Query 8
-- Create a materialized view named "SUPPLIER_PERFORMANCE_MV" that presents the
-- monthly performance of each supplier.
create view SUPPLIER_PERFORMANCE_VIEW as
select suppliers.supplierName, dates.month , SUM(sales.quantity*products.price) as TotalSales
from sales
inner join dates on dates.dateID = sales.dateID
inner join suppliers on suppliers.supplierName = sales.supplierName
inner join products on products.productID = sales.productID
group by suppliers.supplierName, dates.month
order by suppliers.supplierName;
-- step two create table for materlized view

create table SUPPLIER_PERFORMANCE_MV as
select * from SUPPLIER_PERFORMANCE_VIEW ;

-- truncate table SUPPLIER_PERFORMANCE_MV;
-- insert into SUPPLIER_PERFORMANCE_MV select * from SUPPLIER_PERFORMANCE_VIEW;
select *from SUPPLIER_PERFORMANCE_MV;

-- Query 9
-- Identify the top 5 customers with the highest total sales in 2019, considering the number
-- of unique products they purchased.

select customers.customerName,count(Distinct products.productID) as UniqueProducts ,sum(sales.quantity*products.price) as TotalSale
from sales
inner join products on products.productID = sales.productID
inner join dates on dates.dateID=sales.dateID
inner join customers on customers.customerID = sales.customerID
where dates.year = 2019
group by customers.customerName
order by TotalSale DESC, UniqueProducts DESC LIMIT 5;


-- query 10

-- Create a materialized view named "CUSTOMER_STORE_SALES_MV" that presents the
-- monthly sales analysis for each store and then customers wise.

drop view CUSTOMER_STORE_SALES_VIEW;

create view CUSTOMER_STORE_SALES_VIEW as 
select stores.storeName,customers.customerName, dates.month ,sum(products.price * sales.quantity) as TotalSales from
sales
inner join stores on stores.storeID = sales.storeID
inner join customers on customers.customerID = sales.customerID
inner join dates on dates.dateID = sales.dateID
inner join products on products.productID = sales.productID
group by stores.storeName,customers.customerName, dates.month
order by storeName,dates.month , TotalSales Desc;

create table CUSTOMER_STORE_SALES_MV as
select * from CUSTOMER_STORE_SALES_VIEW ;

select *from CUSTOMER_STORE_SALES_MV;	




