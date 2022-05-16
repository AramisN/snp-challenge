-- 1.	What is the average order price per customer?
	select dc.cust_id,round(avg(cast(payment_amount as float))::numeric, 2) as avg_price
	from fact_allorder fa 
	inner join dim_customer dc 
	on dc.cust_id = fa.cust_id 
	group by dc.cust_id 
	order by dc.cust_id 

-- 2.	How many ‘Vacuum cleaners’ were ordered in New York? Ans who bought most of them in this city? 
	select n4.cust_name from (select n3.cust_name,dp.product_name,count(n3.order_id) as order_count from dim_product dp 
	inner join (select *
	from fact_allorder fa 
	inner join dim_customer dc 
	on dc.cust_id = fa.cust_id ) n3
	on n3.product_id = dp.product_id 
	where dp.product_name ='Vacuum cleaner'
	group by n3.cust_name,dp.product_name
	order by order_count desc) n4 
	limit 1

-- 3.	What product is the most popular in each city?

	select n8.cust_city,n8.product_name from (
	select n7.*,rank() over (
	partition by cust_city order by count_order desc
	) from 
	(select n5.cust_city ,dp.product_name, count(n5.order_id) as count_order from (select *
	from fact_allorder fa 
	inner join dim_customer dc 
	on dc.cust_id = fa.cust_id ) n5
	inner join dim_product dp 
	on n5.product_id = dp.product_id 
	group by n5.cust_city ,dp.product_name 
	order by count(n5.order_id) desc) n7
	) n8 
	where rank = 1
	
	