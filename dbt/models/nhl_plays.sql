with play_events as (
    select
        json ->> '$.gamePk' as game_pk,
        json ->> '$.date' as game_date,
        json -> '$.liveData.plays.allPlays' as plays,
        unnest(json_transform(
            plays,
            '[{"about":{"dateTime":"VARCHAR","eventId":"UBIGINT",
            "eventIdx":"UBIGINT","goals":{"away":"UBIGINT","home":"UBIGINT"},
            "ordinalNum":"VARCHAR","period":"UBIGINT","periodTime":"VARCHAR",
            "periodTimeRemaining":"VARCHAR","periodType":"VARCHAR"},
            "coordinates":{"x":"DOUBLE","y":"DOUBLE"},
            "result":{"description":"VARCHAR","event":"VARCHAR",
            "eventCode":"VARCHAR","eventTypeId":"VARCHAR",
            "secondaryType":"VARCHAR","emptyNet":"BOOLEAN",
            "gameWinningGoal":"BOOLEAN",
            "strength":{"code":"VARCHAR","name":"VARCHAR"},
            "penaltyMinutes":"UBIGINT","penaltySeverity":"VARCHAR"},
            "players":[{"player":{"fullName":"VARCHAR",
            "id":"UBIGINT","link":"VARCHAR"},"playerType":"VARCHAR",
            "seasonTotal":"UBIGINT"}],"team":{"id":"UBIGINT",
            "link":"VARCHAR","name":"VARCHAR","triCode":"VARCHAR"}}]'
        )) as play_events
    from {{ ref('nhl_games') }}
)

select
    game_pk,
    game_date,
    play_events
from play_events

{{ write_to_parquet('nhl_plays', '../data/silver/export.parquet') }}
