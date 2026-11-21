select org_id,document_id,pipeline_version,count(*) n from {{ ref('int_document_versions') }} group by 1,2,3 having count(*)>1
