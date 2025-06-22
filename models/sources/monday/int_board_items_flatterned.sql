with boards as (
    select
        board_id,
        board_folder_id
    from {{ ref('stg_boards_monday') }}
),

board_items as (
    select
       board_id,
       item_id,
       group_id,
       parent_item_id,
       item_name,
       item_state
    from {{ ref('int_board_items' ) }}
),

parent_board_items as (
    select
        board_items.item_id,
    from board_items
    inner join boards on board_items.board_id = boards.board_id
    where board_items.parent_item_id is null
),

final as (
    select
        boards.board_id,
        board_items.item_name,
        board_items.item_id,
        board_items.group_id,
        board_items.parent_item_id,
        board_items.item_state,
        boards.board_folder_id,
        iff(board_items.parent_item_id is null, false, true) as is_sub_board_item
    from board_items
    inner join boards on
        board_items.board_id = boards.board_id
    left join parent_board_items on
        board_items.parent_item_id = parent_board_items.item_id
)

select
    board_id,
    board_folder_id,
    parent_item_id,
    item_id,
    group_id,
    item_name,
    item_state
from final
