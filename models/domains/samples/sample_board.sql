with board_items as (
    select
        board_id,
        item_id,
        group_id,
        item_name
    from {{ ref('int_board_items_flatterned' ) }}
),

board_columns as (
    select
        board_id,
        column_id,
        column_title
    from {{ ref('stg_board_columns_monday' ) }}
),

distinct_columns as (
    select distinct column_title,
    from board_columns
    inner join board_items on
        board_items.board_id = board_columns.board_id
),

column_values as (
    select
        item_id,
        column_id,
        column_value,
        column_title
    from {{ ref('int_board_item_column_values' ) }}
),

items_data as (
    select
        board_items.board_id,
        board_items.item_id,
        board_columns.column_title,
        column_values.column_value
    from board_items
    inner join board_columns on
        board_items.board_id = board_columns.board_id
    left outer join column_values on
        board_items.item_id = column_values.item_id and
        board_columns.column_id = column_values.column_id
),

items_data_pivot as (

    select *
    from items_data
    pivot (
        max(column_value)
        for column_title in (
            select distinct column_title from distinct_columns
        )
    )
),

board_data as (

    select
        board_items.board_id,
        board_items.item_id,
        board_items.group_id,
        board_items.item_name,
        items_data_pivot."'column name'" as column_name,        --noqa: rf05
        column_link.value:"linkedpulseid"::varchar as column_link_item_id
    from board_items
    inner join items_data_pivot on
        board_items.board_item_pk = items_data_pivot.board_item_pk,
        lateral flatten(input => parse_json(items_data_pivot."'your linked column'"):"linkedpulseids", outer => true) as column_link,        --noqa: rf05
    group by all
)

select
    board_id,
    group_id,
    item_id,
    item_name,
    column_name,
    column_link_item_id
from board_data
where board_id is not null
