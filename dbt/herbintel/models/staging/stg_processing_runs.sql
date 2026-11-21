select payload:batch_id::varchar as batch_id, payload:org_id::varchar as org_id,
 payload:pipeline_version::varchar as pipeline_version, payload:counts:completed::integer as completed,
 coalesce(payload:counts:quarantined::integer,0) as quarantined, loaded_at
from {{ source('raw','PROCESSING_RUNS') }}
qualify row_number() over (partition by org_id,batch_id,pipeline_version order by loaded_at desc,filename desc)=1
