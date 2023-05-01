# NHL stats API  
This page covers using your data to choose what Evidence displays with Loops and Conditionals.

```total_events
select event_type_id, count(*) as n from play_events group by event_type_id order by n desc;
```

## Total Events
<BarChart 
    data={total_events} 
    x=event_type_id
    y=n
    swapXY=true 
/>