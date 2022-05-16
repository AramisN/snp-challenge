-- public.dim_customer definition

-- Drop table

-- DROP TABLE public.dim_customer;

CREATE TABLE public.temp_dim_customer (
	cust_id varchar(500) NULL,
	cust_name varchar(500) NULL,
	cust_address varchar(500) NULL,
	cust_city varchar(500) NULL,
	cust_procs_time varchar(500) NULL
);


-- public.fact_orderline definition

-- Drop table

-- DROP TABLE public.fact_orderline;

CREATE TABLE public.temp_fact_orderline (
	orderline_id varchar(500) NULL,
	order_id varchar(500) NULL,
	product_id varchar(500) NULL,
	na_na varchar(500) NULL,
	payment_amount varchar(500) NULL
);


-- public.dim_product definition

-- Drop table

-- DROP TABLE public.dim_product;

CREATE TABLE public.temp_dim_product (
	product_id varchar(500) NULL,
	product_name varchar(500) NULL,
	n_a varchar(500) NULL,
	product_procs_time varchar(500) null,
	stock_count varchar(500) NULL
);

-- public.fact_order_info definition

-- Drop table

-- DROP TABLE public.fact_order_info;

CREATE TABLE public.temp_fact_order_info (
	order_id varchar(500) NULL,
	cust_id varchar(500) NULL,
	order_procs_time varchar(500) NULL
);
INSERT INTO public.temp_dim_customer (cust_id,cust_name,cust_address,cust_city,cust_procs_time) VALUES
	 ('CUST-001','Jonesy','E 187th Street','New York','54:18.9'),
	 ('CUST-001','Jones','E 187th Street','New York','54:19.0'),
	 ('CUST-001','Jones','W 82nd Street','New York','54:19.1'),
	 ('CUST-002','Doe','Poland St NW','Atlanta','54:19.2'),
	 ('CUST-003','Smith','Sotelo Ave','San Francisco','54:19.3'),
	 ('CUST-003','Smith','Main Street','Baltimore','54:19.3'),
	 ('CUST-004','Parker','Race street','Denver','54:19.4');
	 
	
INSERT INTO public.temp_dim_product (product_id,product_name,n_a,product_procs_time,stock_count) VALUES
	 ('1','Vacuum cleaner','1','2020-02-13 10:54:19.448470','1'),
	 ('2','Cleaner bags','2','2020-02-13 10:54:19.506872','201'),
	 ('3','Oven mittens','3','2020-02-13 10:54:19.565332','380'),
	 ('1','Vacuum cleaner','4','2020-02-16 10:54:19.623850','1'),
	 ('1','Vacuum cleaner','1','2020-02-18 10:54:19.695248','1');
	 

INSERT INTO public.temp_fact_order_info (order_id,cust_id,order_procs_time) VALUES
	 ('6','CUST-004','2020-02-18 10:54:18.002713'),
	 ('5','CUST-001','2020-02-17 10:54:18.044875'),
	 ('4','CUST-002','2020-02-17 10:54:18.094475'),
	 ('3','CUST-003','2020-02-16 10:54:18.152956'),
	 ('2','CUST-001','2020-02-14 10:54:18.244955'),
	 ('1','CUST-001','2020-02-13 10:54:18.320031');

INSERT INTO public.temp_fact_orderline (orderline_id,order_id,product_id,na_na,payment_amount) VALUES
	 ('','1','1','3','49.99'),
	 ('2','1','2','1','24.99'),
	 ('3','2','2','1','24.99'),
	 ('4','2','3','2','19.99'),
	 ('5','3','1','1','49.99'),
	 ('6','4','1','3','47.99'),
	 ('7','4','3','2','24.5'),
	 ('8','5','1','1','49.99'),
	 ('9','5','2','3','24.99'),
	 ('10','6','1','1','48.99');

	

	