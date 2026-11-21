select payload:document_id::varchar as document_id,
 payload:org_id::varchar as org_id, payload:source_id::varchar as source_id,
 payload:source_version::integer as source_version,
 payload:file_checksum::varchar as file_checksum,
 payload:content_checksum::varchar as content_checksum,
 payload:document_language::varchar as document_language,
 payload:document_category::varchar as document_category,
 payload:pipeline_version::varchar as pipeline_version,
 payload:approved::boolean as approved,
 payload:ingestion_timestamp::timestamp_tz as ingestion_timestamp,
 loaded_at, payload:batch_id::varchar as batch_id
from {{ source('raw','DOCUMENT_METADATA') }}
qualify row_number() over (partition by org_id,document_id,pipeline_version order by loaded_at desc,filename desc)=1
