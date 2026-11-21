select org_id,count(*) as total_documents,count_if(approved and quality_score>=0.75) as ready_documents
from {{ ref('int_document_quality') }} group by 1
