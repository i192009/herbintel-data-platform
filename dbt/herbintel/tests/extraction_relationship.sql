select e.document_id from {{ ref('stg_extraction_results') }} e
left join {{ ref('stg_documents') }} d using(org_id,document_id,pipeline_version) where d.document_id is null
