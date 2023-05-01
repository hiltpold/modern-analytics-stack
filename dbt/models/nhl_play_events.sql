with play_events_raw as (
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
        )) as play_events,
    from {{ ref('nhl_games') }}
), 

play_events_flattened as (
    select
        game_pk,
        game_date,
        play_events.about.dateTime as date_time,
        play_events.about.eventId as event_id,
        play_events.about.eventIdx as event_idx,
        play_events.about.goals.away as goals_away,
        play_events.about.goals.home as goals_home,
        play_events.about.ordinalNum as ordinal_num,
        play_events.about.period as period,
        play_events.about.periodTime as period_time,
        play_events.about.periodTimeRemaining as period_time_remaining,
        play_events.about.periodType as period_type,
        play_events.coordinates.x as x_coordinate,
        play_events.coordinates.y as y_coordinate,
        play_events.result.description as event_description,
        play_events.result.event as event,
        play_events.result.eventCode as event_code,
        play_events.result.eventTypeId as event_type_id,
        play_events.result.secondaryType as secondary_type,
        play_events.result.emptyNet as empty_net,
        play_events.result.gameWinningGoal as game_winning_goal,
        play_events.result.strength.code as strength_code,
        play_events.result.strength.name as strength_name,
        play_events.result.penaltyMinutes as penalty_minutes,
        play_events.result.penaltySeverity as penalty_severity,
        play_events.team.id as team_id,
        play_events.team.name as team_name,
        play_events.players as players
    from play_events_raw
),

play_events_final as (
    select 
        * 
    from 
    (
        select *, unnest(players) as player from play_events_flattened where players is not null
        union all 
        select *, null from play_events_flattened where players is null 
    )
)

{{ write_to_parquet("nhl_play_events","../data/gold/play_events.parquet") }}

select * from play_events_final
