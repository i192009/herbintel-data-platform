select d.*, e.quality_score,e.quality_flags,e.page_count,e.extraction_method,e.extraction_timestamp,
 e.processing_status
from {{ ref('int_deduplicated_documents') }} d
join {{ ref('stg_extraction_results') }} e using(org_id,document_id,pipeline_version)
