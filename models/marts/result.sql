select * from {{ ref("Charges") }}

union all

select * from {{ ref("payment") }}