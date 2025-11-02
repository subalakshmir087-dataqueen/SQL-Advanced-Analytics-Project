/*total sales by day wise*/
select
 datetrunc(month,order_date)as order_date,
 count(distinct customer_key)as total_customers,
 sum(quantity)as total_quantity,
 sum(sales_amount)as total_sales
 from gold.fact_sales
 where order_date is not null
 group by datetrunc(month,order_date)
 order by datetrunc(month,order_date)
 /*=========================================================*/
 
 /*total sales by year wise*/
select
 year(order_date),
 count(distinct customer_key)as total_customers,
 sum(quantity)as total_quantity,
 sum(sales_amount)as total_sales
 from gold.fact_sales
 where year(order_date) is not null
 group by year(order_date)
 order by year(order_date)
 /*=========================================================*/

 /*total sales by month wise*/
 select
 month(order_date) as month,
 count(distinct customer_key)as total_customers,
 sum(quantity)as total_quantity,
 sum(sales_amount)as total_sales
 from gold.fact_sales
 where month(order_date) is not null
 group by month(order_date)
 order by month(order_date)
 /*=========================================================*/

 /*cumulative analysis
 Aggregate the data progressively over time*/
 with cumulative_sales as(
 select
     datetrunc(month,order_date)as order_date,
     sum(sales_amount)as total_sales,
     avg(price)as avg_price
 from gold.fact_sales
 where datetrunc(month,order_date)is not null
 group by datetrunc(month,order_date)
 )
 select
   order_date,
   total_sales,
   sum(total_sales)over(partition by year(order_date) order by order_date)as running_total,
   avg(avg_price)over(partition by year(order_date) order by order_date)as moving_avg
 from cumulative_sales
 /*=========================================================*/

/*performance analysis
comparing the current value to a target value
helps measure success and compare performance*/
with performance_sales as (
select
   year(f.order_date) as order_year,
   p.product_name,
   sum(f.sales_amount)as total_sales
   from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where f.order_date is not null
group by year(f.order_date),p.product_name
)
select
  order_year,
  product_name,
  total_sales,
  avg(total_sales)over(partition by product_name)as avg_sales,
  (total_sales-avg(total_sales)over(partition by product_name))as avg_sales_performance,
  case when (total_sales-avg(total_sales)over(partition by product_name))>0 then 'Above average'
       when (total_sales-avg(total_sales)over(partition by product_name))<0 then 'Below average'
       else 'Average'
 end as avg_bucket,
  lag(total_sales)over(partition by product_name order by order_year )as previous_sale,
  (total_sales-lag(total_sales)over(partition by product_name order by order_year ))as previous_sale_performance,
  case when  (total_sales-lag(total_sales)over(partition by product_name order by order_year ))>0 then 'Increased'
       when (total_sales-lag(total_sales)over(partition by product_name order by order_year ))<0 then 'Decreased'
       else 'Null'
end as previous_sales_bucket
from performance_sales 
 /*=========================================================*/

/*part to whole analysis
Analyze how an individual part is performing compared to the overall,
allowing us to understand which category has the highest impact on the business*/

with category_sales as(
select
    p.category,
    sum(f.sales_amount)as total_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where year(f.order_date) is not null
group by p.category
)
select
 category,
 total_sales,
 sum(total_sales)over()as overall,
concat(round((cast(total_sales as float)/sum(total_sales)over())*100,2),'%')as per_total
from category_sales
order by per_total desc
 /*=========================================================*/

/*data segmnetation 
group the data based on a specific range
helps understand the correlation between two measures*/
 with product_segment as (
 select
   product_key,
   product_name,
   cost,
   case when cost<100 then 'Below 100'
        when cost between 100 and 500 then '100-500'
        when cost between 500 and 1000 then '500-1000'
   else 'Above 1000'
   end as cost_range
   from gold.dim_products)
   select
    cost_range,
    count(product_key)as total_products
   from product_segment
   group by cost_range
   order by total_products desc
 /*=========================================================*/

/* group customers by their spending behavior:
----VIP:at least 12 months of history and spending more than 5000.
----Regular:at least 12 months of history but spending 5000 or less
----New:lifespan less than 12 months*/
with customer_segmentation as (
select
    c.customer_key,
    sum(f.sales_amount)as total_sales,
    min(f.order_date)as first_order,
    max(f.order_date)as last_order,
    datediff(month,min(f.order_date), max(f.order_date))as lifespan,
    case when datediff(month,min(f.order_date), max(f.order_date))>=12 and  sum(f.sales_amount)>=5000 then 'VIP'
    when  datediff(month,min(f.order_date), max(f.order_date))>=12 and  sum(f.sales_amount)<5000 then 'Regular'
    else 'New'
    end as customers_segment
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key
group by  c.customer_key
)
select
  customers_segment,
  count(*)as total_customers,
  round(avg(total_sales),2)as avg_sales_segment
  from customer_segmentation
group by customers_segment
order by total_customers desc 
 /*=========================================================*/

/*
==================================================================
purpose:
     -This report consolidates key customer metrics and behaviors
Highlights
   1.Gathers essentials fields such a names,ages,and transaction details.
   2.segments customers into categories(VIP,Regular,New)and age group.
   3.Aggregates customer-level metrics
       --- total orders
       --- total sales
       --- total quantity purchased
       ---total_products
       ---lifespan(in months)
   4.calculate valuable KPIs
       ---recency(months since last order)
       ---average order value
       ---average monthly spend
===================================================================
*/
/*-----------------------------------------------------------------
1)Base Query:Retrives core columns from tables
-----------------------------------------------------------------*/
create view gold.report_customers as
with base as(
select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.Firstname,' ',c.Lastname)as customer_name,
datediff(year,c.Birthday,getdate())as age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key
where order_date is not null
)
,customer_segmentation as(
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number)as total_orders,
sum(sales_amount)as total_sales,
sum(quantity)as total_quantity,
count(distinct product_key)as total_product,
max(order_date)as last_order_date,
datediff(month,min(order_date),max(order_date))as lifespan
from base
group by customer_key,
customer_number,
customer_name,age
)
select
  customer_key,
  customer_number,
  customer_name,
  age,
  case when age<20 then 'Under 20'
       when age between 20 and 29 then '20-29'
       when age between 30 and 39 then '30-39'
       when age between 40 and 49 then '40-49'
       else 'Above 50'
 end as age_group,

  case when lifespan>=12 and total_sales>5000 then 'VIP'
       when lifespan>=12 and total_sales<=5000 then 'Regular'
       else 'New'
  end as customer_segment,
   last_order_date,
   datediff(month, last_order_date,(select max(order_date) from gold.fact_sales))as recency,
    case when datediff(month, last_order_date,(select max(order_date) from gold.fact_sales))>=3 then 'Churned'
      else 'Active'
    end  customer_status,
  total_sales,
  total_quantity,
  total_product,
  lifespan,
  ---compuate average order value(AVO)
  case when total_orders=0 then 0
        else total_sales/total_orders
  end as avg_order_value,
  ---compuate average monthly spend
  case when lifespan=0 then total_sales
       else total_sales/lifespan
 end as avg_monthly_spend
 from customer_segmentation
/*=========================================================*/
