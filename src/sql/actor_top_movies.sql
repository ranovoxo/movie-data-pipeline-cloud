DROP TABLE IF EXISTS top_actors_by_profit;

CREATE TABLE top_actors_by_profit AS
WITH movie_profits AS (
    SELECT id, (revenue - budget) AS profit
    FROM raw_finances
)
SELECT
    c.name AS actor_name,
    ROUND(AVG(mp.profit)) AS avg_profit,
    COUNT(DISTINCT c.movie_id) AS movie_count
FROM cast_members c
JOIN movie_profits mp ON c.movie_id = mp.id
GROUP BY c.name
HAVING COUNT(DISTINCT c.movie_id) >= 3
ORDER BY avg_profit DESC
LIMIT 20;