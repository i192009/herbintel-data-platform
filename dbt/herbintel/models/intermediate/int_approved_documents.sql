select * from {{ ref('int_document_quality') }} where approved and quality_score>=0.75 and processing_status='completed'
