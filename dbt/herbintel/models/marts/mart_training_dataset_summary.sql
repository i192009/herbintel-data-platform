select org_id,dataset_version,document_count,approval_status,loaded_at from {{ ref('fct_dataset_versions') }}
