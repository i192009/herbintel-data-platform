select org_id,batch_id,pipeline_version,sum(completed) as completed,sum(quarantined) as quarantined,max(loaded_at) as loaded_at
from {{ ref('stg_processing_runs') }} group by 1,2,3
