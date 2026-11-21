{% snapshot document_approval %}
{{ config(target_schema='AUDIT',unique_key="org_id || ':' || document_id || ':' || pipeline_version",strategy='check',check_cols=['approved'],invalidate_hard_deletes=True) }}
select org_id,document_id,pipeline_version,approved from {{ ref('stg_documents') }}
{% endsnapshot %}
