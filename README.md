# Question 1: 
We have a toy OLTP database named **oltp\_db** which is presented as a set of CSV files. In **oltp\_db**, there are four tables including order\_line, order\_info, customer and product. Our team is in charge of creating a data warehouse in order to answer some business-related questions. 

First, you should transfer data from **oltp\_db** to a data warehouse with the Star Schema in a staging environment. In the next steps, you should answer some business questions with efficient **SQL** queries on the data warehouse database.



Given the CSV files, you are expected to implement the following steps:

1. Load CSV files in a Postgresql Database named oltp\_db.
![alt text](https://github.com/AramisN/snp-challenge/blob/main/11111.png)
![alt text](https://github.com/AramisN/snp-challenge/blob/main/22222.png)







1. Create a Postgresql database named dwh\_db with stars schema using SQL queries.

+**Answer**: to make oltp\_data become star, we can just combine two facts and make it one fact table (view, if we use same database, table if we have dwh\_db). 

**create** **view** fact\_allorder  **as** 

(

**select** fo.orderline\_id ,fo.order\_id ,fo.product\_id ,foi.cust\_id ,fo.payment\_amount ,**to\_timestamp**(foi.order\_procs\_time,'YYYY-MM-DD HH12:MI:SS') **as** order\_date  **from** public.fact\_orderline fo 

**left** **join** public.fact\_order\_info foi 

**on** fo.order\_id = foi.order\_id 

)


1. Transfer data from oltp\_db to dwh\_db using SQL queries.

+**Answer**: we can do it using psql dump or just exporting tables with SQL format, tansfer it into temp tables and after doing transformation and data cleaning we can create the tables we want and change it to the names we desire. If the size of tables is big, doing it with a processing engines like spark will be preferred (bonus: I will upload an ETL job that will get these data from S3 and create a cube and it will be far easier than using SQL)

Sql files:

migration\_oltp\_to\_dwh.sql

transform\_dwh\_to\_star\_and\_clean\_data.sql

![alt text](https://github.com/AramisN/snp-challenge/blob/main/333333.png)


**Notes on data**: the dataset provided has many issues:

1. Datamodel is not provided, so It will be a guessing game + finding correlation using fact tables
1. Seems like the transaction software is failing to update old records and just inserts data(happened for customer table and products like vacuum cleaner)
1. Product table has a 4th column that is not clear what it is, it can be category or brand or anything and this can make the deduplication of multiple rows with “vacuum cleaner” in it very tricky! 
1. In orderline table, one row has no ID, this row can be manually inserted as “1” in ID or because it is unclear, be ignored from transactions. Luckily because theres no table using orderline\_ID as foreign key, we can ignore it.
1. Customer table has minute:seconds.miliseconds data at the end, which is unclear but I believe its possible to guess the date and hour by order\_info (if the customer gets added into customer table during order submit, two process times can be almost the same)


Given a data warehouse you should answer the following question:

1. What is the average order price per customer?

+**Answer**: 

cust\_id |   avg\_price         

-------+------------------+

`      `1|             32.49|

`      `2|             36.25|

`      `3|             49.99|

`      `4|             48.99|

query provided in ‘reports.sql’ file

1. How many ‘Vacuum cleaners’ were ordered in New York? Ans who bought most of them in this city? 

**Answer**: ‘Vacuum cleaners’ as mentioned in quote, doesn’t exist. But ‘Vacuum cleaner’ was mostly bought by ‘Jones’

query provided in ‘reports.sql’ file

1. What product is the most popular in each city?

**Answer:**

cust\_city    |product\_name  |

-------------+--------------+

Atlanta      |Oven mittens  |

Atlanta      |Vacuum cleaner|

Denver       |Vacuum cleaner|

New York     |Cleaner bags  |

San Francisco|Vacuum cleaner|

query provided in ‘reports.sql’ file


