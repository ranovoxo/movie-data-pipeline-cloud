-- top 20 most profitable movies by ROI
CREATE TABLE IF NOT EXISTS profitability_gold AS
SELECT
    m.id,
    COALESCE(m.title, f.title) AS title,
    COALESCE(m.release_date, f.release_date) AS release_date,
    m.vote_average,
    m.vote_count,
    m.popularity,
    m.overview,
    m.original_language,
    m.genres,
    f.budget,
    f.revenue,
    (f.revenue - f.budget) AS profit,
    ROUND((f.revenue - f.budget)::NUMERIC / NULLIF(f.budget, 0), 2) AS roi,
    EXTRACT(YEAR FROM COALESCE(m.release_date::DATE, f.release_date::DATE))::INT AS release_year
FROM movies_silver m
INNER JOIN raw_finances f ON m.id = f.id
WHERE f.budget > 0 AND f.revenue > 0
ORDER BY roi DESC
LIMIT 20;