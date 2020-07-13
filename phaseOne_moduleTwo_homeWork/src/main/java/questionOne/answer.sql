with tmp as(
select team ,year,row_number() over (partition by team order by year)
num
from t1
)

select team,count(t.sc)
 from (
select team ,year,year-dense_rank() over(partition by team order by year) as sc
from tmp
) t
group by t.sc,team
having count(t.sc)>2
