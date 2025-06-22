select
    "id"::number as workspace_id,
    "kind"::text as workspace_kind,
    "name"::text as workspace_name
from {{ source('monday', 'workspaces', true) }}
where "state" = 'active'
    and not omnata_is_deleted
