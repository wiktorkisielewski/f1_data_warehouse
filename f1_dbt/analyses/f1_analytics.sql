-- ===============================
-- F1 ANALYTICS QUERIES
-- ===============================


-- Top 10 drivers by total career points
select
    d.first_name,
    d.last_name,
    sum(f.points) as total_points
from fact_results f
join dim_drivers d
    on f.driver_id = d.driver_id
group by 1, 2
order by total_points desc
limit 10;


-- Constructor dominance by season
select
    f.season,
    c.constructor_name,
    sum(f.points) as season_points
from fact_results f
join dim_constructors c
    on f.constructor_id = c.constructor_id
group by 1, 2
order by f.season desc, season_points desc;


-- Drivers with most race wins (P1 finishes)
select
    d.first_name,
    d.last_name,
    count(*) as wins
from fact_results f
join dim_drivers d
    on f.driver_id = d.driver_id
where f.finish_position = 1
group by 1, 2
order by wins desc
limit 10;


-- Average points per race by driver (min 50 races)
select
    d.first_name,
    d.last_name,
    round(avg(f.points), 2) as avg_points_per_race,
    count(*) as races
from fact_results f
join dim_drivers d
    on f.driver_id = d.driver_id
group by 1, 2
having count(*) >= 50
order by avg_points_per_race desc;


-- Country with most races hosted
select
    r.country,
    count(*) as race_count
from dim_races r
group by r.country
order by race_count desc;


-- Constructors with most wins
select
    c.constructor_name,
    count(*) as wins
from fact_results f
join dim_constructors c
    on f.constructor_id = c.constructor_id
where f.finish_position = 1
group by c.constructor_name
order by wins desc
limit 10;