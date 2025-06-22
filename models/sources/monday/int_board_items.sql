with board_items as (
    select
        board_id,
        item_id,
        group_id,
        parent_item_id,
        item_name,
        item_state
    from {{ ref('stg_board_items_monday') }}
),

active_board_items as (
    select
        item_id,
        board_id
    from {{ ref('stg_board_items_active_monday') }}
),

final as (
    select
        board_items.board_id,
        board_items.item_id,
        board_items.group_id,
        board_items.item_name,
        board_items.parent_item_id
    from board_items
    inner join active_board_items on
        board_items.board_id = active_board_items.board_id
        and board_items.item_id = active_board_items.item_id
)

select
    board_id,
    item_id,
    group_id,
    parent_item_id,
    item_name,
    item_state
from final
