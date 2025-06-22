select
    "id"::text as column_id,
    "board_id"::number as board_id,
    "archived"::boolean as archived,
    "description"::text as description,
    "type"::text as column_type,
    parse_json("settings_str"::text) as column_settings,
    lower("title"::text) as column_title
from {{ source('monday', 'board_columns', true) }}
where not omnata_is_deleted
