-- 1.How many customers are added each year

SELECT
  DATE_TRUNC('year', create_date)::DATE AS create_year,
  COUNT(customer_key) AS total_customer
FROM gold.dim_customers
GROUP BY DATE_TRUNC('year', create_date)
ORDER BY create_year;

-- 2.calculate the total sales per month and the running total of sales over time

SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
  ROUND(AVG(avg_price) OVER (ORDER BY order_date), 2) AS moving_average_price
FROM (
  SELECT
    DATE_TRUNC('month', order_date)::DATE AS order_date,
    SUM(sales_amount) AS total_sales,
    AVG(price) AS avg_price
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATE_TRUNC('month', order_date)
  ORDER BY DATE_TRUNC('month', order_date)
);

-- 3.Analyze the yearly performance of products by comparing their sales to both 
-- the average sales performance of the product and the previous year sales

WITH yearly_product_sales as ( 
SELECT 
       EXTRACT(YEAR FROM f.order_date) AS order_year,
       p.product_name,
       SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
  ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY order_year , p.product_name
)
select 
order_year,
product_name,
current_sales,
round(AVG(current_sales) over (partition by product_name),0) as avg_sales,
current_sales - round(Avg(current_sales) over (partition by product_name), 0) as diff_avg,
case when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'above avg'
     when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'below avg'
	 else 'avg'
END avg_change,


lag(current_sales) over (partition by product_name order by order_year) pyr_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) diff_py,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year)  > 0 then 'increase'
     when current_sales - lag(current_sales) over (partition by product_name order by order_year)  < 0 then 'decrease'
	 else 'no change'
END py_change	 	 
from yearly_product_sales
order by product_name , order_year

-- 4.Which categories contribute the most to overall sales?

WITH category_sales AS (
  SELECT 
    p.category,
    SUM(f.sales_amount) AS total_sales
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
  GROUP BY p.category
)
SELECT 
  category,
  total_sales,
  SUM(total_sales) OVER () AS overall_sales,
  ROUND((total_sales::numeric / SUM(total_sales) OVER ()::numeric) * 100, 2)::text || '%' AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;

-- 5.Segment products into cast ranges and count how many products fall into each segment.

with product_segments as (
select 
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
     when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
     else 'above 1000'

end cost_range 
from gold.dim_products)

select
cost_range,
count (product_key) as total_products
from product_segments
group by cost_range
order by total_products desc;

/* 6. group customers into three segments based on their spending behaviour:

  1- VIP : customer with atleast 12 months of history and spends more than 5000
  2- Regular : customer with 12 months of history but their spending is less than 5000
  3 -New : customer with a lifespan lesss than 12 months
  
  And find the total number of customer by each group */  

with customer_spending as(
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(f.order_date) as first_order,
max(f.order_date) as last_order,
EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 +
EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan

from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key

group by c.customer_key)

select 
customer_segment,
count (customer_key) as total_customers
from
(
  select customer_key,
   case when lifespan >= 12 and total_spending > 5000 then 'VIP'
     when lifespan >= 12 and total_spending <= 5000 then 'Regular'
	 else 'new'
end customer_segment	 
from customer_spending)

group by customer_segment
order by total_customers desc



/*
  7.Customer report--
-----------------------------------------------------------------------
 Purpose - This report consolidates key customer metrics and behaviour

 -------------------------------------------------------------------------------
 1. Gathers essenstial field such as names, ages, and transaction details.
 
 2. Segments customer into categories(VIP, Regular, new) and age groups.
 
 3. Aggregate customer-level-metrics:
    - total sales 
	- total orders 
	- total quantity purchase
	- total products
	- lifespan(in months)

 4. calculate Valuable KPIs
  - recency (months since last order)
  - average order value
  - averagy monthly spend

  */



CREATE OR REPLACE VIEW gold.customer_summary_report AS

WITH base_query AS ( 
    -- Base table - Retrieves core columns from tables
    SELECT 
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.birthdate)) AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
),

customer_aggregation AS (
    -- Customer-level aggregations
    SELECT 
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 +
        EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
    FROM base_query
    GROUP BY 
        customer_key,
        customer_number,
        customer_name,
        age
)

SELECT 
    customer_key,
    customer_number,
    customer_name,
    age,

    -- Age bucket
    CASE 
        WHEN age < 20 THEN 'Under 20' 
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39' 
        WHEN age BETWEEN 40 AND 49 THEN '40-49' 
        ELSE '50 and above'
    END AS age_group,

    -- Customer segment
    CASE 
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,

    last_order_date,

    -- Recency in months
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, last_order_date)) * 12 +
    EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_order_date)) AS recency,

    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,

    -- Average order value
    CASE 
        WHEN total_sales = 0 THEN 0
        ELSE total_sales / total_orders
    END AS average_order_value,

    -- Average monthly spend
    ROUND(
        CASE 
            WHEN lifespan = 0 THEN total_sales
            ELSE total_sales / lifespan
        END,
    0) AS avg_monthly_spend

FROM customer_aggregation;



/*
  8.Product report--
-----------------------------------------------------------------------
 Purpose - This report consolidates key product metrics and behaviour

 -------------------------------------------------------------------------------
 1. Gathers essenstial field such as product name, category,sub-category and cost.
 
 2. Segments products by revenue to identify high-performers , mid-range or low-performers.
 
 3. Aggregate product-level-metrics:
    - total sales 
	- total orders 
	- total quantity sold
	- total customers (unique)
	- lifespan(in months)

 4. calculate Valuable KPIs
  - recency (months since last sale)
  - average order revenue(AOR)
  - averagy monthly revenue

  */


CREATE OR REPLACE VIEW gold.product_summary_report AS

WITH base_query AS ( 
    -- Base table - Retrieves core column from fact_sales and dim_products
    SELECT 
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
),

product_aggregation AS (
    -- Product Aggregations: Summarizes key metrics at the product level
    SELECT 
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        MAX(order_date) AS last_sale_date,
        EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 +
        EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sales_amount) / NULLIF(SUM(quantity), 0), 1) AS average_selling_price
    FROM base_query
    GROUP BY 
        product_key,
        product_name,
        category,
        subcategory,
        cost
)

-- Combine all product results into final output
SELECT 
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,

    EXTRACT(YEAR FROM AGE(CURRENT_DATE, last_sale_date)) * 12 +
    EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_sale_date)) AS recency,

    CASE 
        WHEN total_sales > 50000 THEN 'High-Performer' 
        WHEN total_sales >= 10000 THEN 'Mid-range' 
        ELSE 'Low-Performer'
    END AS product_segment,

    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    average_selling_price,

    -- Average Order Revenue (AOR)
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS average_order_revenue,

    -- Average Monthly Revenue
    ROUND(
        CASE 
            WHEN lifespan = 0 THEN total_sales
            ELSE total_sales / lifespan
        END,
    0) AS avg_monthly_revenue

FROM product_aggregation;

SELECT * FROM gold.product_summary_report;


 