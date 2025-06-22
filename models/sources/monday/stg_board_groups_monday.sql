select
    "board_id"::number as board_id,
    "id"::text as group_id,
    "title"::text as group_name
from {{ source('monday', 'board_groups', true) }}
where not omnata_is_deleted
