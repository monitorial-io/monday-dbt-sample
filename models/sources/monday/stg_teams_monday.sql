select
    "id"::number as team_id,
    "name"::text as team_name
from {{ source('monday', 'teams', true) }}
where not omnata_is_deleted
