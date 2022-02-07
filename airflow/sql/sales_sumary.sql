CREATE MATERIALIZED VIEW IF NOT EXISTS sales_summary AS
select us.email, sum(t.price)
from transactions t
join users us using(user_id)
where exists (
    select user_id 
    from webinar w
    left join users u using(email)
    where date_registration > '2016-04-01' and t.user_id = u.user_id
        and w.email not in(
            select email
            from users u
            where date_registration < '2016-04-01'
        ) )
group by us.email;