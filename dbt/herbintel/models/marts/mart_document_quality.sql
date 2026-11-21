select org_id,document_category,count(*) as document_count,avg(quality_score) as mean_quality,
 count_if(quality_score<0.75) as below_threshold from {{ ref('int_document_quality') }} group by 1,2
