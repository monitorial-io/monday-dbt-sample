select
    "id"::text as id,
    "account_id"::number as account_id,
    "created_at"::timestamp_ntz as created_at,
    "user_id"::number as user_id,
    "board_id"::number as board_id,
    "event"::text as entity_event,
    "entity"::text as entity,               --noqa:rf04
    coalesce(parse_json("data"::text):"pulse_id"::numeric, parse_json("data"::text):"item_id"::numeric) as item_id,
    coalesce(parse_json("data"::text):"pulse_name"::text, parse_json("data"::text):"item_name"::text) as item_name,
    parse_json("data"::text) as event_data
from {{ source('monday', 'activity_logs', true) }}
where not omnata_is_deleted
