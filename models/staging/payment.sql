select
    null as Bill_no,
    receipt_detail.receipt_date as actual_date,
    'R' as row_type,
    coalesce(office_master.srno, 0) as office_id,
    coalesce(office_master.office_code, 'All') as office_code,
    coalesce(office_master.name, 'All') as office,
    coalesce(emp_master.empid, 0) as doctor_id,
    case
        when emp_master.empid is null then 'All'
        else concat(emp_master.lastname, ', ', emp_master.firstname)
    end as provider,
    coalesce(emp_master.empid, 0) as perf_doctor_id,
    case
        when emp_master.empid is null then 'All'
        else concat(emp_master.lastname, ', ', emp_master.firstname)
    end as performing_provider,
    CAST(
        CASE
            WHEN receipt_detail.paid_by = 'I' THEN 
                CASE receipt_detail.ins_type 
                    WHEN 'A' THEN 0
                    WHEN 'C' THEN CAST(receipt_detail.insurance_carrier_id AS INT64)
                    WHEN 'P' THEN CAST(insurance_plan_master.carrier_id AS INT64)
                    ELSE NULL
                END
            ELSE NULL
        END AS INT64
    ) AS carrier_id,
    case
       when receipt_detail.paid_by = 'P' then 'Patient Paid'
       else coalesce(insurance_carrier_master.name, 'All Insurance Carrier')
    end as insurance_carrier,
    receipt_detail.plan_id,
    case
       when receipt_detail.paid_by = 'P' then 'Patient Paid'
       else coalesce(insurance_plan_master.name, 'All Insurance Plan')
    end as insurance_plan,
    patient_master.ref_party_id as ref_party_id,
    patient_master.referral_source as referral_source,
    patient_master.patient_type as patient_type,
    patient_master.patient_type_description as patient_type_description,
    null as Bill_Created_by,
    null as CPT_Created_by,
    null as BillHeader_Created_Date,
    null as BillDetail_Created_Date,
    null as billing_id,
    null as CPT_Descr,
    0 as patient_count,
    0 as patient_visit_count,
    0 as CPT_Count,
    0 as charges,
    0 as adjustment_plus,
    0 as allowed_amount,
    sum(receipt_detail.amount) as gross_revenue,
    0 as pat_write_off,
    0 as Ins_write_off,
    0 as total_write_off,
    sum(receipt_detail.refund_amount) as refund,
    0 as open_credit,
    sum(
       case
           when receipt_detail.payment_type = 'A' then 0
           else coalesce(receipt_subdetail.over_paid, 0)
       end
    ) as over_paid,
    sum(
       case
           when receipt_detail.payment_type = 'A' then 0
           else
               case
                   when receipt_detail.paid_by = 'I' then receipt_detail.unapply_amount
                   else 0
               end
       end
    ) as RMP,
    sum(
       case
           when receipt_detail.payment_type = 'A' then
               (
                   coalesce(receipt_detail.unapply_amount, 0) - coalesce(receipt_detail.refund_amount, 0)
               ) + coalesce(receipt_subdetail.amount, 0)
           else 0
       end
    ) as adjustment_negative,
    0 as pat_balance_due,
    0 as ins_balance_due,
    0 as total_balance_due,
    0 as ar_30,
    0 as ar_30_60,
    0 as ar_60_90,
    0 as ar_90,
    0 as pat_net_revenue,
    0 as ins_net_revenue,
    0 as total_net_revenue

from (SELECT 
        CASE
            WHEN receipt_detail.paid_by = 'I' THEN 
                CASE receipt_detail.ins_type 
                    WHEN 'A' THEN 0
                    WHEN 'C' THEN 0
                    WHEN 'P' THEN CAST(receipt_detail.insurance_id AS INT64)
                    ELSE NULL 
                END
            ELSE NULL 
        END AS plan_id, *
    FROM dbt_vtyagi.receipt_detail
    ) receipt_detail
left join dbt_vtyagi.office_master on receipt_detail.office_id = office_master.srno
left join (
    select
        tran_id,
        sum(
            case
                when receipt_subdetail.op_type = 1 then coalesce(receipt_subdetail.amount, 0) - coalesce(receipt_subdetail.refund_amount, 0)
                else 0
            end
        ) as over_paid,
        sum(receipt_subdetail.amount) as amount
    from dbt_vtyagi.receipt_subdetail
    group by tran_id
) as receipt_subdetail on receipt_subdetail.tran_id = receipt_detail.receipt_id
left join dbt_vtyagi.emp_master on receipt_detail.doctor_id = emp_master.empid 
left join dbt_vtyagi.insurance_plan_master on receipt_detail.plan_id = insurance_plan_master.srno
left join dbt_vtyagi.insurance_carrier_master on carrier_id = insurance_carrier_master.srno
left join (
    select 
        patient_master.id as patient_id,
        patient_master.patient_no as patient_no,
        concat(patient_master.lastname,', ',patient_master.firstname) as patient,
        patient_type_master.patient_type as patient_type,
        patient_type_master.description as patient_type_description,
        referral_party_master.srno as ref_party_id,
        referral_party_master.name as referral_source,
        row_number() over (partition by patient_master.id order by patient_type_detail.srno) as rowid
    from dbt_vtyagi.patient_master 
    left join dbt_vtyagi.referral_party_master on patient_master.referral_party_id = referral_party_master.srno
    left join dbt_vtyagi.patient_type_detail on patient_type_detail.patient_id = patient_master.id
    left join dbt_vtyagi.patient_type_master on patient_type_detail.patient_type = patient_type_master.patient_type
) as patient_master on receipt_detail.patient_id = patient_master.patient_id and receipt_detail.paid_by = 'P' and patient_master.rowid = 1
group by row_type, actual_date, office_id, office_code, office, doctor_id, provider, perf_doctor_id, performing_provider, carrier_id, insurance_carrier, plan_id, insurance_plan, ref_party_id, referral_source, patient_type, patient_type_description, billing_id, CPT_Descr, Bill_no, Bill_Created_by, CPT_Created_by, BillHeader_Created_Date, BillDetail_Created_Date
order by actual_date, row_type, office_id, doctor_id
