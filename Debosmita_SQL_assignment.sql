/* 1. Customer Revenue Aggregation
Question: Write a query to find the total order amount for each customer in 2023 whose
total order value is greater than $10,000. Show customer name and total order amount */

--Solution:
select c.customer_name, sum(o.order_amount) as total_order_amount from customers c join orders o on c.customer_id = o.customer_id
where o.order_date >='2023-01-01' and o.order_date <= '2024-01-01'
group by c.customer_id, c.customer_name having sum(o.order_amount) > 10000; 


/* 2. Average Salary by Department
List each department with the average salary of its employees */

--Solution:
select d.department_name , avg(e.salary) as avg_salary from  departments d left join employees e  on   d.department_id = e.department_id group by d.department_id, d.department_name order by avg_salary desc;


/* 3. Recent Orders per Customer 
find the 2 most recent orders for each customer. */

--Solution:
select customer_id ,order_id,  order_date, order_amount from (select * , row_number() over (partition by customer_id order by order_date desc) as rk from orders) r where rk<=2 ORDER BY customer_id, order_date desc;


/* 4. First Purchase Date 
find the first purchase date for every customer */

--Solution:
select customer_id , min(order_date) as first_purchase from orders group by customer_id order by customer_id ;


/* 5. Customers With No Orders 
Return a list of all customers who did not place any order in 2023 */

--Solution:
select c.customer_id , c.customer_name from customers  c left join orders o on c.customer_id = o.customer_id and o.order_date >= '2023-01-01' and o.order_date < '2024-01-01' where o.order_id is null order by c.customer_id ;



/* 6. Category Sales Comparison 
Find categories where sales in 2023 increased compared to 2022 */

--Solution:
WITH yearly_sales AS (
    select
        p.category,
        CASE
            WHEN s.sale_date < '2023-01-01' THEN 2022
            ELSE 2023
        END           AS sales_year,
        SUM(s.amount) AS total_sales
    from sales s
     join products p ON s.product_id = p.product_id
    where s.sale_date >= '2022-01-01'
      AND s.sale_date < '2024-01-01'
    GROUP BY
        p.category,
        CASE
            WHEN s.sale_date < '2023-01-01' THEN 2022
            ELSE 2023
        END)
select
    y23.category,
    y23.total_sales AS sales_2023,
    y22.total_sales AS sales_2022,
    (y23.total_sales - y22.total_sales) AS sales_increase
from yearly_sales y23
 join yearly_sales y22  ON  y23.category = y22.category
    AND y22.sales_year = 2022
where y23.sales_year   = 2023
  AND y23.total_sales  > y22.total_sales     
ORDER BY sales_increase DESC;





/* 7. Monthly Churn Rate
For each month, calculate the number of users who churned (moved from 'active' to 'cancelled'). */

--Solution:
select churn_month, count(*) as churned_users
from (select user_id, date_format(`date`, 'yyyy-MM') as churn_month, status, lag(status) over (partition by user_id order by `date` ) as prev_status from subscriptions) t 
where status = 'cancelled' and prev_status = 'active' group by churn_month ;


/* 8. Find Duplicate Records  
find all transactions where a user has made the same amount on the same day more than once */

--Solution:
select user_id , amount, cast (transaction_date as date) as transaction_day , count(*) as occurrence from transactions
group by user_id , amount, cast (transaction_date as date) having count(*) > 1 order by user_id;



/* 9. Detecting Consecutive Days of Activity 
 Find all users who have logged in for at least 3 consecutive days at any time */

--Solution:
with cte as (select user_id, login_date , DATE_SUB(login_date, INTERVAL row_number() over(partition by user_id order by login_date ) day) as  grp
from (select distinct user_id, login_date from logins ) t ),

streak as (select user_id, grp , min(login_date) streak_start , count(*) streak_long from cte group by user_id , grp)

select user_id , streak_start from   streak where streak_long>=3 order by user_id ,streak_start ;


  /* 10. Percentile Salary by Department 
 Show each employee’s salary and the 90th percentile salary for their department */

--Solution:
select emp_id , department , salary ,PERCENTILE_CONT(0.9)  within group (order by salary) over (partition by department) as percentile_salary from employees ;



/* 11. Identify Products Never Sold
 List names of products never sold */

--Solution:
select p.product_id,p.name from products p left join sales s on p.product_id = s.product_id where s.sale_id is null;



/* 12. Running Total per Customer 
For each order,show the customer’s running total spend up to and including that order */

--Solution:
select order_id, customer_id, order_date, amount, sum(amount) over(partition by customer_id order by order_date  rows between unbounded preceding  and current row) running_total from orders;


/* 13. Given subscriptions(user_id, status, date), status is "active" or "cancelled". For each month, calculate the number of users who churned (moved from 'active' to 'cancelled'). */

--Solution:
select churn_month, count(*) as churned_users
from (select user_id, date_format(`date`, 'yyyy-MM') as churn_month, status, lag(status) over (partition by user_id order by `date` ) as prev_status from subscriptions) t 
where status = 'cancelled' and prev_status = 'active' group by churn_month ;






