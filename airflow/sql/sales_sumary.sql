CREATE MATERIALIZED VIEW sales_summary AS
select user_id, sum(price)
from transactions t
where exists (
select user_id 
from users u
inner join webinar using(email)
where date_registration > '2016-04-01' and t.user_id = u.user_id)
group by user_id;