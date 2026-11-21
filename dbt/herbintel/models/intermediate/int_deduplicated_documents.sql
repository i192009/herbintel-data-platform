with latest_sources as (
 select * from {{ ref('int_document_versions') }}
 qualify row_number() over (partition by org_id,source_id order by source_version desc nulls last,
    ingestion_timestamp desc,try_to_number(substr(pipeline_version,2)) desc,document_id)=1
)
select * from latest_sources
qualify row_number() over (partition by org_id,content_checksum order by approved desc,document_id)=1
