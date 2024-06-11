select * from {{ ref("Charges") }}

union

select * from {{ ref("payment") }}