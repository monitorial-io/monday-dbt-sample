select
    "board_id"::number as board_id,
    "id"::number as item_id,
    "updated_at"::timestamp_ntz as updated_at,
    "group_id"::text as group_id,
    "parent_item_id"::number as parent_item_id,
    "name"::text as item_name,
    "creator_id"::number as creator_id,
    "created_at"::timestamp_ntz as created_at,
    "state"::text as item_state,
    "email"::text as board_email
from {{ source('monday', 'board_items', true) }}
where not omnata_is_deleted
