select
    "id"::number as board_id,
    "workspace_id"::number as workspace_id,
    "board_folder_id"::number as board_folder_id,
    "items_count"::number as items_count,
    "name"::text as board_name,
    "description"::text as board_description,
    "state"::text as board_state
from {{ source('monday', 'boards', true) }}
where board_state = 'active'
    and not omnata_is_deleted
