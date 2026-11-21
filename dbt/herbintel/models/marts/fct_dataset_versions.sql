select * from {{ ref('stg_dataset_manifests') }} where dataset_version is not null
