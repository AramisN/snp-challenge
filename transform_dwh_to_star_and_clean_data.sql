--Clean data, tranfrom data, create tables filled with data from temp tables
------Creating main FACT table
create table public.fact_allorder as select * from  (select tfo.orderline_id,tfo.order_id,tfo.product_id,cast(trim(leading 'CUST-' from tfoi.cust_id) as INTEGER) as cust_id,tfo.payment_amount ,tfoi.order_procs_time from temp_fact_order_info tfoi 
right join temp_fact_orderline tfo 
on tfoi.order_id =tfo.order_id 
order by tfoi.order_procs_time,tfo.orderline_id asc) n2

------Creating main dim_customer table
create table public.dim_customer as (select cast(trim(leading 'CUST-' from cust_id) as INTEGER) as cust_id,n1.cust_name, n1.cust_address, n1.cust_city ,n1.cust_procs_time from (select distinct on(tdc.cust_id) tdc.cust_id,tdc.cust_name, tdc.cust_address, tdc.cust_city ,tdc.cust_procs_time 
from temp_dim_customer tdc 
order by tdc.cust_id,tdc.cust_procs_time desc )  n1)

------Creating main dim_product table
create table public.dim_product as (select distinct on(tdp.product_id) tdp.product_id,tdp.product_name ,tdp.stock_count, tdp.n_a, tdp.product_procs_time 
from temp_dim_product tdp 
order by tdp.product_id,tdp.product_procs_time desc )



--Droping temp tables
drop table temp_dim_customer ;
drop table temp_dim_product ;
drop table temp_fact_order_info ;
drop table temp_fact_orderline ;

-- Next step: making relationships 