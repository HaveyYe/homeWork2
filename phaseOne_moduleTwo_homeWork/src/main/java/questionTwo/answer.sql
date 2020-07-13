with tmp as(
select t2.id,t2.`time`,t2.price,
t2.price-lag(t2.price) over(partition by t2.id order by t2.`time`) as cz
from t2
)
select
tmp2.id,
tmp2.`time`,
tmp2.bg
from(
select
t.*
case when t.scz>0 and t.cz2<0 then '波峰',
 when t.scz<0 and t.cz2 >0 then '波谷' as bg
from(
select
tmp.id,tmp.`time`,tmp.price,tmp.cz as scz,
lead(price) over(partition by id order by tmp.`time`)-tmp.price as cz2
from tmp
) t
)tmp2