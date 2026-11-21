select payload:document_id::varchar as document_id, payload:org_id::varchar as org_id,
 payload:pipeline_version::varchar as pipeline_version,
 payload:quality_score::float as quality_score, payload:quality_flags as quality_flags,
 payload:page_count::integer as page_count, payload:extraction_method::varchar as extraction_method,
 payload:processing_status::varchar as processing_status,
 payload:extraction_timestamp::timestamp_tz as extraction_timestamp, loaded_at
from {{ source('raw','EXTRACTION_RESULTS') }}
qualify row_number() over (partition by org_id,document_id,pipeline_version order by loaded_at desc,filename desc)=1
