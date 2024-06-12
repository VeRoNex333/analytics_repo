SELECT 
    patient_master.id AS patient_id,
    patient_master.patient_no AS patient_no,
    CONCAT(patient_master.lastname, ' ', patient_master.firstname) AS patient,
    patient_type_master.patient_type AS patient_type,
    patient_type_master.description AS patient_type_description,
    referral_party_master.srno AS ref_party_id,
    referral_party_master.name AS referral_source,
    ROW_NUMBER() OVER (PARTITION BY patient_master.id ORDER BY patient_type_detail.srno) AS rowid
FROM 
    {{ source("dbt_vtyagi", "patient_master") }}
LEFT JOIN 
    {{ source("dbt_vtyagi", "referral_party_master") }} ON patient_master.referral_party_id = referral_party_master.srno
LEFT JOIN 
    {{ source("dbt_vtyagi", "patient_type_detail") }} ON patient_type_detail.patient_id = patient_master.id
LEFT JOIN 
    {{ source("dbt_vtyagi", "patient_type_master") }} ON patient_type_detail.patient_type = patient_type_master.patient_type