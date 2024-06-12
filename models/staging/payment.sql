select
    concat(null) as bill_no,
    receipt_detail.receipt_date as actual_date,
    'R' as row_type,
    cast(coalesce(office_master.srno, 0) as string) as office_id,
    coalesce(office_master.office_code, 'All') as office_code,
    coalesce(office_master.name, 'All') as office,
    coalesce(emp_master.empid, 0) as doctor_id,
    case
        when emp_master.empid is null
        then 'All'
        else concat(emp_master.lastname, ', ', emp_master.firstname)
    end as provider,
    -- coalesce(emp_master.empid, 0) as perf_doctor_id,
    -- case
    -- when emp_master.empid is null then 'All'
    -- else concat(emp_master.lastname, ', ', emp_master.firstname)
    -- end as performing_provider,
    cast(
        case
            when receipt_detail.paid_by = 'I'
            then
                case
                    receipt_detail.ins_type
                    when 'A'
                    then 0
                    when 'C'
                    then cast(receipt_detail.insurance_carrier_id as int64)
                    when 'P'
                    then cast(insurance_plan_master.carrier_id as int64)
                    else null
                end
            else null
        end as int64
    ) as carrier_id,
    case
        when receipt_detail.paid_by = 'P'
        then 'Patient Paid'
        else coalesce(insurance_carrier_master.name, 'All Insurance Carrier')
    end as insurance_carrier,
    receipt_detail.plan_id,
    case
        when receipt_detail.paid_by = 'P'
        then 'Patient Paid'
        else coalesce(insurance_plan_master.name, 'All Insurance Plan')
    end as insurance_plan,
    patient_master.ref_party_id as ref_party_id,
    patient_master.referral_source as referral_source,
    patient_master.patient_type as patient_type,
    patient_master.patient_type_description as patient_type_description,
    cast(null as STRING) as bill_created_by,
    cast(null as STRING) as cpt_created_by,
    cast(null as date) as billheader_created_date,
    cast(null as date) as billdetail_created_date,
    cast(null as string) as billing_id,
    cast(null as string) as cpt_descr,
    0 as patient_count,
    0 as patient_visit_count,
    cast(0 as float64) as cpt_count,
    cast(0 as float64) as charges,
    0 as adjustment_plus,
    0 as allowed_amount,
    sum(receipt_detail.amount) as gross_revenue,
    cast(0 as float64) as pat_write_off,
    cast(0 as float64) as ins_write_off,
    cast(0 as float64) as total_write_off,
    sum(receipt_detail.refund_amount) as refund,
    cast(0 as float64) as open_credit,
    sum(
        case
            when receipt_detail.payment_type = 'A'
            then 0
            else coalesce(receipt_subdetail.over_paid, 0)
        end
    ) as over_paid,
    sum(
        case
            when receipt_detail.payment_type = 'A'
            then 0
            else
                case
                    when receipt_detail.paid_by = 'I'
                    then receipt_detail.unapply_amount
                    else 0
                end
        end
    ) as rmp,
    sum(
        case
            when receipt_detail.payment_type = 'A'
            then
                (
                    coalesce(receipt_detail.unapply_amount, 0)
                    - coalesce(receipt_detail.refund_amount, 0)
                )
                + coalesce(receipt_subdetail.amount, 0)
            else 0
        end
    ) as adjustment_negative,
    cast(0 as float64) as pat_balance_due,
    cast(0 as float64) as ins_balance_due,
    cast(0 as float64) as total_balance_due,
    cast(0 as float64) as ar_30,
    cast(0 as float64) as ar_30_60,
    cast(0 as float64) as ar_60_90,
    cast(0 as float64) as ar_90,
    cast(0 as float64) as pat_net_revenue,
    cast(0 as float64) as ins_net_revenue

from
    (
        select
            case
                when receipt_detail.paid_by = 'I'
                then
                    case
                        receipt_detail.ins_type
                        when 'A'
                        then 0
                        when 'C'
                        then 0
                        when 'P'
                        then cast(receipt_detail.insurance_id as int64)
                        else null
                    end
                else null
            end as plan_id,
            *
        from dbt_vtyagi.receipt_detail
    ) receipt_detail
left join dbt_vtyagi.office_master on receipt_detail.office_id = office_master.srno
left join
    (
        select
            tran_id,
            sum(
                case
                    when receipt_subdetail.op_type = 1
                    then
                        coalesce(receipt_subdetail.amount, 0)
                        - coalesce(receipt_subdetail.refund_amount, 0)
                    else 0
                end
            ) as over_paid,
            sum(receipt_subdetail.amount) as amount
        from dbt_vtyagi.receipt_subdetail
        group by tran_id
    ) as receipt_subdetail
    on receipt_subdetail.tran_id = receipt_detail.receipt_id
left join dbt_vtyagi.emp_master on receipt_detail.doctor_id = emp_master.empid
left join
    dbt_vtyagi.insurance_plan_master
    on receipt_detail.plan_id = insurance_plan_master.srno
left join
    dbt_vtyagi.insurance_carrier_master on carrier_id = insurance_carrier_master.srno
left join
    {{ ref("patient_master_updated") }} as patient_master
    on receipt_detail.patient_id = patient_master.patient_id
    and receipt_detail.paid_by = 'P'
    and patient_master.rowid = 1
group by
    row_type,
    actual_date,
    office_id,
    office_code,
    office,
    doctor_id,
    provider,
    carrier_id,
    insurance_carrier,
    plan_id,
    insurance_plan,
    ref_party_id,
    referral_source,
    patient_type,
    patient_type_description,
    billing_id,
    cpt_descr,
    bill_no,
    bill_created_by,
    cpt_created_by,
    billheader_created_date,
    billdetail_created_date
order by actual_date, row_type, office_id, doctor_id
