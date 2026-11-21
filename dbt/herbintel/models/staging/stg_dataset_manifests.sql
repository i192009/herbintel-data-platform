select sha2(to_json(payload),256) as manifest_id,
 payload:dataset_version::varchar as dataset_version, payload:batch_id::varchar as batch_id,
 payload:org_id::varchar as org_id, payload:rows::integer as document_count,
 payload:approval_status::varchar as approval_status, payload as manifest, loaded_at
from {{ source('raw','DATASET_MANIFESTS') }}
qualify row_number() over (partition by manifest_id order by loaded_at desc,filename desc)=1
