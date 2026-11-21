{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['org_id','document_id','pipeline_version']) }}
select * from {{ ref('stg_documents') }}
-- Use the arrival clock, never the source event timestamp, for late arrivals.
{% if is_incremental() %}
where loaded_at >= dateadd(day,-3,(select coalesce(max(loaded_at),'1970-01-01'::timestamp_ltz) from {{ this }}))
{% endif %}
