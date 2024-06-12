WITH write_offs AS (
    SELECT
        SUM(CASE 
                WHEN billing_subdetail.line_id = -1 THEN billing_subdetail.adjusted_amt 
                ELSE 0 
            END) AS pat_write_off,
        SUM(CASE 
                WHEN billing_subdetail.line_id <> -1 THEN billing_subdetail.adjusted_amt 
                ELSE 0 
            END) AS Ins_write_off
    FROM
        {{ ref("temp_billing_header") }}
    LEFT JOIN(
        SELECT 
            CASE billing_subdetail.ins_category 
                WHEN 'I' THEN 'CI' 
                WHEN 'P' THEN 'PI' 
                ELSE 'P' 
            END AS resp_party_type,
            ROW_NUMBER() OVER (PARTITION BY tran_id,sr_id ORDER BY (CASE WHEN line_id = -1 THEN 999 ELSE line_id END)) AS line_rowid,
            * 
        FROM dbt_vtyagi.billing_subdetail
    ) AS billing_subdetail ON temp_billing_header.tran_id = billing_subdetail.tran_id AND temp_billing_header.sr_id = billing_subdetail.sr_id
),
balances AS (
    SELECT
        SUM(CASE 
                WHEN billing_subdetail.line_id = -1 
                     AND temp_billing_header.is_pending = 1 
                     AND temp_billing_header.pat_balance > 0 
                THEN billing_aging.balance 
                ELSE 0 
            END) AS pat_balance_due,
        SUM(CASE 
                WHEN billing_subdetail.line_id <> -1 
                     AND temp_billing_header.resp_party_id = billing_subdetail.ins_srid 
                     AND temp_billing_header.resp_party_type = billing_subdetail.resp_party_type 
                     AND temp_billing_header.is_pending = 1 
                     AND temp_billing_header.ins_balance > 0 
                THEN temp_billing_header.ins_balance 
                ELSE 0 
            END) AS ins_balance_due
    FROM
        {{ ref("temp_billing_header") }}
    LEFT JOIN(
        SELECT 
            CASE billing_subdetail.ins_category 
                WHEN 'I' THEN 'CI' 
                WHEN 'P' THEN 'PI' 
                ELSE 'P' 
            END AS resp_party_type,
            ROW_NUMBER() OVER (PARTITION BY tran_id,sr_id ORDER BY (CASE WHEN line_id = -1 THEN 999 ELSE line_id END)) AS line_rowid,
            * 
        FROM dbt_vtyagi.billing_subdetail
    ) AS billing_subdetail ON temp_billing_header.tran_id = billing_subdetail.tran_id AND temp_billing_header.sr_id = billing_subdetail.sr_id
    LEFT JOIN (
        SELECT 
            tran_id, sr_id, line_id,
            SUM(billing_aging.amount-billing_aging.amt_adjusted) AS balance 
        FROM dbt_vtyagi.billing_aging
        WHERE billing_aging.amount - billing_aging.amt_adjusted > 0
        GROUP BY tran_id, sr_id, line_id
    ) AS billing_aging ON billing_subdetail.tran_id = billing_aging.tran_id AND billing_subdetail.sr_id = billing_aging.sr_id AND billing_subdetail.line_id = billing_aging.line_id 
) 


SELECT 
    Bill_no,
    temp_billing_header.service_date AS actual_date,
    'C' AS row_type,
    cast(temp_billing_header.office_id as string) AS office_id,
    office_master.office_code AS office_code,
    office_master.name AS office,
    temp_billing_header.doctor_id AS doctor_id,
    CONCAT(emp_master.lastname, '', '', emp_master.firstname) AS Provider,
    CASE 
        WHEN billing_subdetail.tran_id IS NULL THEN 0 
        ELSE COALESCE(insurance_carrier_master.srno, 0) 
    END AS carrier_id,
    CASE 
        WHEN billing_subdetail.tran_id IS NULL THEN '(Zero Charge)' 
        ELSE COALESCE(insurance_carrier_master.name, 'Patient Responsible') 
    END AS insurance_carrier,
    CASE 
        WHEN billing_subdetail.tran_id IS NULL THEN 0 
        ELSE COALESCE(insurance_plan_master.srno, 0) 
    END AS plan_id,
    CASE 
        WHEN billing_subdetail.tran_id IS NULL THEN '(Zero Charge)' 
        ELSE COALESCE(insurance_plan_master.name, 'Patient Responsible') 
    END AS insurance_plan,
    patient_master.ref_party_id AS ref_party_id,
    patient_master.referral_source AS referral_source,
    patient_master.patient_type AS patient_type,
    patient_master.patient_type_description AS patient_type_description,
    Bill_Created_by,
    CPT_Created_by,
    BillHeader_Created_Date,
    BillDetail_Created_Date,
    temp_billing_header.billing_id AS billing_id,
    temp_billing_header.CPT_Descr AS CPT_Descr,
    SUM(
        CASE 
			WHEN COALESCE(billing_subdetail.line_rowid, 1) = 1 AND temp_billing_header.patient_rowid = 1 THEN 1 
            ELSE 0 
        END
    ) AS patient_count,
    SUM(
        CASE 
            WHEN COALESCE(billing_subdetail.line_rowid, 1) = 1 AND temp_billing_header.ref_rowid = 1 THEN 1 
            ELSE 0 
        END
    ) AS patient_visit_count,
    SUM(
        CASE 
            WHEN billing_subdetail.tran_id IS NULL THEN temp_billing_header.billing_qty
            ELSE 
                CASE 
                    WHEN COALESCE(temp_billing_header.ins_min_line_id, 0) <> 0 THEN 
                        CASE 
                            WHEN temp_billing_header.ins_min_line_id = billing_subdetail.line_id THEN temp_billing_header.billing_qty 
                        END
                    ELSE 
                        CASE 
                            WHEN temp_billing_header.min_line_id = billing_subdetail.line_id THEN temp_billing_header.billing_qty 
                            ELSE 0.00 
                        END 
                END 
        END
    ) AS CPT_Count,
    SUM(
        CASE 
            WHEN temp_billing_header.ins_min_line_id IS NOT NULL THEN 
                CASE 
                    WHEN CAST(temp_billing_header.ins_min_line_id AS STRING) = CAST(billing_subdetail.line_id AS STRING) THEN temp_billing_header.total_billing_amount 
                END
            ELSE 
                CASE 
                    WHEN CAST(temp_billing_header.min_line_id AS STRING) = CAST(billing_subdetail.line_id AS STRING) THEN temp_billing_header.total_billing_amount 
                    ELSE 0.00 
                END 
        END
    ) AS Charges,
    -- SUM(
    -- CASE 
    --     WHEN temp_billing_header.adjustment_bill = 'Y' THEN 
    --         CASE 
    --             WHEN CASTbilling_subdetail.line_id = COALESCE(CAST(temp_billing_header.min_line_id AS STRING), '-2') THEN temp_billing_header.total_billing_amount 
    --             ELSE 0.00 
    --         END
    --     ELSE 
    --         0 
    -- END
    -- ) AS adjustment_plus,
    0 as adjustment_plus,
    -- sum(case 
    --         when coalesce(temp_billing_header.ins_min_line_id, 0) <> 0 then (
    --             case 
    --                 when temp_billing_header.ins_min_line_id = billing_subdetail.line_id then temp_billing_header.allowed_amt end
    --         )
    --         else case 
    --                 when temp_billing_header.min_line_id = billing_subdetail.line_id then temp_billing_header.allowed_amt else 0.00 
    --             end 
    --     end
    -- ) as allowed_amount--,
    0 as allowed_amount,
    cast(0 as float64) AS gross_revenue,
    write_offs.pat_write_off,
    write_offs.Ins_write_off,
    write_offs.pat_write_off + write_offs.Ins_write_off AS total_write_off,
    cast(0 as float64) AS refund,
    SUM(
        CASE 
            WHEN billing_subdetail.line_id = -1 THEN temp_billing_header.pat_open_credit 
            ELSE 0.00 
        END
    ) AS open_credit,
    cast(0 as float64) AS over_paid,
    cast(0 as float64) AS RMP,
    cast(0 as float64) AS adjustment_negative,
    balances.pat_balance_due AS pat_balance_due,
    balances.ins_balance_due AS ins_balance_due,
    balances.pat_balance_due + balances.ins_balance_due as total_balance_due,
    CASE 
        WHEN DATE_DIFF(CAST(temp_billing_header.service_date AS DATE), CURRENT_DATE(), DAY) <= 30 THEN balances.pat_balance_due + balances.ins_balance_due 
        ELSE 0 
    END AS ar_30,
    CASE 
        WHEN DATE_DIFF(CAST(temp_billing_header.service_date AS DATE), CURRENT_DATE(), DAY) > 30 
            AND DATE_DIFF(CAST(temp_billing_header.service_date AS DATE), CURRENT_DATE(), DAY) <= 60 THEN balances.pat_balance_due + balances.ins_balance_due
        ELSE 0 
    END AS ar_30_60,
    CASE 
        WHEN DATE_DIFF(CAST(temp_billing_header.service_date AS DATE), CURRENT_DATE(), DAY) > 60 
             AND DATE_DIFF(CAST(temp_billing_header.service_date AS DATE), CURRENT_DATE(), DAY) <= 90 THEN balances.pat_balance_due + balances.ins_balance_due 
        ELSE 0 
    END AS ar_60_90,
    CASE 
        WHEN DATE_DIFF(CAST(temp_billing_header.service_date AS DATE), CURRENT_DATE(), DAY) > 90 THEN balances.pat_balance_due + balances.ins_balance_due 
        ELSE 0 
    END AS ar_90,
    SUM(
        CASE 
            WHEN billing_subdetail.line_id = -1 THEN receipt_subdetail.amount 
            ELSE 0 
        END
    ) AS pat_net_revenue,
    SUM(
        CASE 
            WHEN billing_subdetail.line_id <> -1 THEN receipt_subdetail.amount 
            ELSE 0 
        END
    ) AS ins_net_revenue
    --pat_net_revenue + ins_net_revenue AS total_net_revenue

FROM 
    {{ ref("temp_billing_header") }}
JOIN {{ ref("patient_master_updated") }}  as patient_master ON temp_billing_header.patient_id = patient_master.patient_id AND patient_master.rowid = 1
LEFT JOIN (
    SELECT 
        CASE billing_subdetail.ins_category 
            WHEN 'I' THEN 'CI' 
            WHEN 'P' THEN 'PI' 
            ELSE 'P' 
        END AS resp_party_type,
        ROW_NUMBER() OVER (PARTITION BY tran_id,sr_id ORDER BY (CASE WHEN line_id = -1 THEN 999 ELSE line_id END)) AS line_rowid,
        * 
    FROM 
        dbt_vtyagi.billing_subdetail
) AS billing_subdetail ON temp_billing_header.tran_id = billing_subdetail.tran_id AND temp_billing_header.sr_id = billing_subdetail.sr_id
LEFT JOIN (
    SELECT 
        srv_tran_id, srv_sr_id, line_id,
        SUM(receipt_subdetail.amount) AS amount
    FROM 
        dbt_vtyagi.receipt_subdetail 
    JOIN 
        dbt_vtyagi.receipt_detail ON receipt_subdetail.tran_id = receipt_detail.receipt_id AND receipt_detail.payment_type <> 'A'
    GROUP BY 
        srv_tran_id, srv_sr_id, line_id
) AS receipt_subdetail ON billing_subdetail.tran_id = receipt_subdetail.srv_tran_id AND billing_subdetail.sr_id = receipt_subdetail.srv_sr_id AND billing_subdetail.line_id = receipt_subdetail.line_id
LEFT JOIN (
    SELECT 
        tran_id, sr_id, line_id,
        SUM(billing_aging.amount-billing_aging.amt_adjusted) AS balance 
    FROM 
        dbt_vtyagi.billing_aging
    WHERE 
        billing_aging.amount - billing_aging.amt_adjusted > 0
    GROUP BY 
        tran_id, sr_id, line_id
) AS billing_aging ON billing_subdetail.tran_id = billing_aging.tran_id AND billing_subdetail.sr_id = billing_aging.sr_id AND billing_subdetail.line_id = billing_aging.line_id 
LEFT JOIN 
    dbt_vtyagi.office_master ON office_id = CAST(office_master.srno AS STRING)
LEFT JOIN 
    dbt_vtyagi.emp_master ON temp_billing_header.doctor_id = emp_master.empid
LEFT JOIN 
    dbt_vtyagi.insurance_plan_master ON billing_subdetail.insurance_id = insurance_plan_master.srno 
LEFT JOIN 
    dbt_vtyagi.insurance_carrier_master ON insurance_plan_master.carrier_id = insurance_carrier_master.srno
LEFT JOIN 
    write_offs ON 1=1
LEFT JOIN 
    balances ON 1=1
GROUP BY 
    Bill_NO,row_type,service_date,office_id,office_code,office,doctor_id,provider,carrier_id,insurance_carrier,plan_id,insurance_plan,total_write_off,pat_write_off,Ins_write_off,pat_balance_due,
    ins_balance_due,ref_party_id,referral_source,patient_type,patient_type_description,billing_id,CPT_Descr,Bill_Created_by,CPT_Created_by,BillHeader_Created_Date,BillDetail_Created_Date
