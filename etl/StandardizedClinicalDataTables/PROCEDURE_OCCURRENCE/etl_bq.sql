WITH
proc_icd as (SELECT mimic_id as procedure_occurrence_id, subject_id, hadm_id, icd9_code as procedure_source_value, CASE WHEN length(cast(ICD9_CODE as STRING)) = 2 THEN cast(ICD9_CODE as STRING) ELSE concat(substr(cast(ICD9_CODE as STRING), 1, 2), '.', substr(cast(ICD9_CODE as STRING), 3)) END AS concept_code FROM {work_project_id}.{work_dataset_id}.procedures_icd),
local_proc_icd AS (SELECT concept_id as procedure_source_concept_id, concept_code as procedure_source_value FROM {cdm_project_id}.{cdm_dataset_id}.concept WHERE domain_id = 'd_icd_procedures' AND vocabulary_id = 'MIMIC Local Codes'),
concept_proc_icd9 as ( SELECT concept_id as procedure_concept_id, concept_code FROM {cdm_project_id}.{cdm_dataset_id}.concept WHERE vocabulary_id = 'ICD9Proc'),
patients AS (SELECT subject_id, mimic_id as person_id FROM {work_project_id}.{work_dataset_id}.patients),
caregivers AS (SELECT mimic_id AS provider_id, cgid FROM {work_project_id}.{work_dataset_id}.caregivers),
admissions AS (SELECT hadm_id, admittime, dischtime as procedure_datetime, mimic_id as visit_occurrence_id FROM {work_project_id}.{work_dataset_id}.admissions),
  proc_event AS (
  SELECT
    t2.mimic_id AS procedure_source_concept_id,
    t1.mimic_id AS procedure_occurrence_id,
    subject_id,
    cgid,
    hadm_id,
    itemid,
    starttime AS procedure_datetime,
    label AS procedure_source_value,
    value AS quantity -- then it stores the duration... this is a warkaround and may be inproved
  FROM
    {work_project_id}.{work_dataset_id}.procedureevents_mv t1
  LEFT JOIN
    {work_project_id}.{work_dataset_id}.d_items t2
  USING
    (itemid)
     where cancelreason = 0 -- not cancelled
),
gcpt_procedure_to_concept as (SELECT item_id as itemid, concept_id as procedure_concept_id from {work_project_id}.{work_dataset_id}.gcpt_procedure_to_concept),
cpt_event AS ( SELECT mimic_id as procedure_occurrence_id , subject_id , hadm_id , chartdate as procedure_datetime, cpt_cd, subsectionheader as procedure_source_value FROM {work_project_id}.{work_dataset_id}.cptevents),
omop_cpt4 as (SELECT concept_id as procedure_source_concept_id, concept_code as cpt_cd FROM {cdm_project_id}.{cdm_dataset_id}.concept where vocabulary_id = 'CPT4'),
standard_cpt4 as (
    WITH
      table0 AS (
      SELECT
        ROW_NUMBER() OVER(PARTITION BY c1.concept_id ORDER BY relationship_id ASC) AS row_number,
        c2.concept_id as procedure_concept_id,
        c1.concept_code AS cpt_cd
      FROM
        {cdm_project_id}.{cdm_dataset_id}.concept c1
      JOIN
        {cdm_project_id}.{cdm_dataset_id}.concept_relationship cr
      ON
        concept_id_1 = c1.concept_id
        AND relationship_id IN ('CPT4 - SNOMED eq',
          'Maps to')
      LEFT JOIN
        {cdm_project_id}.{cdm_dataset_id}.concept c2
      ON
        concept_id_2 = c2.concept_id
      WHERE
        c1.vocabulary_id ='CPT4'
        AND c2.standard_concept = 'S' )
    SELECT
      procedure_concept_id, cpt_cd
    FROM
      table0
    WHERE
      row_number = 1
),
row_to_insert AS (
SELECT
  procedure_occurrence_id
, patients.person_id
, coalesce(standard_cpt4.procedure_concept_id,0) as procedure_concept_id
, CAST(coalesce(cpt_event.procedure_datetime, admissions.admittime) AS DATE) as procedure_date
, coalesce(cpt_event.procedure_datetime, admissions.admittime) as procedure_datetime
, 257 as procedure_type_concept_id -- Hospitalization Cost Record
, CAST(NULL AS INT64) as modifier_concept_id
, CAST(NULL AS INT64) as quantity
, CAST(NULL AS INT64) as provider_id
, admissions.visit_occurrence_id
, CAST(NULL AS INT64) as visit_detail_id -- the chartdate is never a time, when exist
, cpt_event.procedure_source_value
, omop_cpt4.procedure_source_concept_id as procedure_source_concept_id
, CAST(NULL AS STRING) as modifier_source_value
FROM cpt_event
LEFT JOIN patients USING (subject_id)
LEFT JOIN admissions USING (hadm_id)
LEFT JOIN omop_cpt4 USING (cpt_cd)
LEFT JOIN standard_cpt4 USING (cpt_cd)
UNION ALL
SELECT
  procedure_occurrence_id
, patients.person_id
, coalesce(gcpt_procedure_to_concept.procedure_concept_id, 0) as procedure_concept_id
, CAST(proc_event.procedure_datetime AS DATE) as procedure_date
, (proc_event.procedure_datetime) as procedure_datetime
, 38000275 as procedure_type_concept_id -- EHR order list entry
, null as modifier_concept_id
, quantity as quantity --duration of the procedure in minutes
, caregivers.provider_id as provider_id
, admissions.visit_occurrence_id
, visit_detail_assign.visit_detail_id as visit_detail_id
, procedure_source_value
, procedure_source_concept_id -- from d_items mimic_id
, null as modifier_source_value
FROM proc_event
LEFT JOIN patients USING (subject_id)
LEFT JOIN admissions USING (hadm_id)
LEFT JOIN caregivers USING (cgid)
LEFT JOIN gcpt_procedure_to_concept USING (itemid)
LEFT JOIN {cdm_project_id}.{cdm_dataset_id}.visit_detail_assign ON admissions.visit_occurrence_id = visit_detail_assign.visit_occurrence_id
AND
(--only one visit_detail
(is_first IS TRUE AND is_last IS TRUE)
OR -- first
(is_first IS TRUE AND is_last IS FALSE AND proc_event.procedure_datetime <= visit_detail_assign.visit_end_datetime)
OR -- last
(is_last IS TRUE AND is_first IS FALSE AND proc_event.procedure_datetime > visit_detail_assign.visit_start_datetime)
OR -- middle
(is_last IS FALSE AND is_first IS FALSE AND proc_event.procedure_datetime > visit_detail_assign.visit_start_datetime AND proc_event.procedure_datetime <= visit_detail_assign.visit_end_datetime)
)
UNION ALL
SELECT
  procedure_occurrence_id
, patients.person_id
, coalesce(concept_proc_icd9.procedure_concept_id,0) as procedure_concept_id
, CAST(admissions.procedure_datetime AS DATE) as procedure_date
, (admissions.procedure_datetime) AS procedure_datetime
, 38003622 as procedure_type_concept_id
, null as modifier_concept_id
, null as quantity
, null as provider_id
, admissions.visit_occurrence_id
, null as visit_detail_id
, proc_icd.procedure_source_value
, coalesce(procedure_source_concept_id,0) as procedure_source_concept_id
, null as modifier_source_value
FROM proc_icd
LEFT JOIN local_proc_icd USING (procedure_source_value)
LEFT JOIN patients USING (subject_id)
LEFT JOIN admissions USING (hadm_id)
LEFT JOIN concept_proc_icd9 USING (concept_code)
)
SELECT * 
FROM
row_to_insert