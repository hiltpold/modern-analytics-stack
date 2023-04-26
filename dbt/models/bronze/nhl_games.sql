with source as (
    select * from {{ source("nhl_data","nhl_games") }}
)

select * from source