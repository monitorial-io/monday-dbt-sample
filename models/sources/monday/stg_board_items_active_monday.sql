select
    "board_id"::number as board_id,
    "id"::number as item_id,
    "updated_at"::timestamp_ntz as updated_at
from {{ source('monday', 'board_items_active', true) }}
where not omnata_is_deleted
