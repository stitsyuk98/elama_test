CREATE MATERIALIZED VIEW IF NOT EXISTS sales_summary AS
select user_id, sum(price)
from transactions t
where exists (
    select user_id 
    from (
        select *
        from webinar w
        where email not in(
            select email
            from users u
            where date_registration < '2016-04-01'
        ) 
    ) as tbl
left join users u using(email)
where date_registration > '2016-04-01' and t.user_id = u.user_id)
group by user_id;