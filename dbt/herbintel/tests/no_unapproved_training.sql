select * from {{ ref('fct_training_documents') }} where not approved or quality_score<0.75
