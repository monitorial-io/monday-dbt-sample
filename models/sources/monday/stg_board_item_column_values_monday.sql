select
    "id"::text as column_id,
    "item_id"::number as item_id,
    "board_id"::number as board_id,
    "text"::text as column_text,
    parse_json("value"::text) as column_value,
    "display_value"::text as mirror_column_value,
    "checked"::text as checked_column_value,
    "url"::text as url_column_value
from {{ source('monday', 'board_item_column_values', true) }}
where not omnata_is_deleted
