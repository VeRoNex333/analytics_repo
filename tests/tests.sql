with a as(
    select * from {{ ref("Charges") }}
)


select 
    plan_id,
    sum(charges) as total_charge_amt
from a
group by plan_id
having 
    total_charge_amt < 0
