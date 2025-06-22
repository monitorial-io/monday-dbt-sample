with board_items as (
    select
        board_pk,
        item_id,
        board_id
    from {{ ref('int_board_items' ) }}
),

board_columns as (
    select
        column_id,
        board_id,
        column_title,
        column_type,
        column_settings
    from {{ ref('stg_board_columns_monday') }}
),

board_item_values as (
    select
        column_id,
        board_id,
        item_id,
        column_value,
        column_text,
        checked_column_value,
        mirror_column_value,
        url_column_value
    from {{ ref('stg_board_item_column_values_monday') }}
),

final as (
    select
        board_items.board_id,
        board_items.item_id,
        board_columns.column_id,
        board_columns.column_title,
        board_columns.column_type,
        board_columns.column_settings,
        case
            when board_columns.column_type = 'numbers'
            then
                case
                    when coalesce(board_item_values.column_text, '') = ''
                    then '0'
                    else board_item_values.column_text
                end
            when board_columns.column_type = 'checkbox'
            then cast(board_item_values.checked_column_value as varchar)
            when board_columns.column_type = 'mirror'
            then board_item_values.mirror_column_value
            when board_columns.column_type = 'link'
            then board_item_values.url_column_value
            when board_columns.column_type = 'formula' then '<<formula based>>'
            when board_columns.column_type in (
                    'email',
                    'country',
                    'date',
                    'dropdown',
                    'hour',
                    'name',
                    'location',
                    'long_text',
                    'phone',
                    'rating',
                    'status',
                    'tags',
                    'text',
                    'world_clock',
                    'item_id'
                )
            then board_item_values.column_text
            else board_item_values.column_value
        end as column_value
    from board_items
    inner join board_columns on board_items.board_id = board_columns.board_id
    inner join board_item_values on
        board_items.item_id = board_item_values.item_id
        and board_items.board_id = board_item_values.board_id
        and board_columns.column_id = board_item_values.column_id
)

select
    board_id,
    item_id,
    column_id,
    column_type,
    column_value,
    column_settings,
    column_title
from final
