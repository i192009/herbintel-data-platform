select org_id,coalesce(document_language,'unknown') as document_language,count(*) as document_count
from {{ ref('fct_training_documents') }} group by 1,2
