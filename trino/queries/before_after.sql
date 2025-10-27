-- Demo: BEFORE (raw sources) vs AFTER (GAV views)

-- AFTER: simple GAV query
SELECT player_name, season, team_abbr, position, height, weight, src_position
FROM memory.gav.global_player_season
WHERE team_abbr = 'LAL' AND season BETWEEN 2019 AND 2021
ORDER BY season DESC, player_name

-- BEFORE: equivalent raw join across Postgres + Mongo
SELECT
  COALESCE(LOWER(b.full_name), LOWER(pt.player))            AS player_name,
  CAST(pt.season AS INTEGER)                                AS season,
  UPPER(pt.team)                                            AS team_abbr,
  COALESCE(b.position, UPPER(pt.pos), UPPER(ppg.pos))       AS position,
  b.height, b.weight,
  CASE WHEN b.position IS NOT NULL THEN 'player_bio'
       WHEN pt.pos IS NOT NULL THEN 'player_totals'
       WHEN ppg.pos IS NOT NULL THEN 'player_per_game'
  END                                                       AS src_position
FROM postgresql.staging.player_totals pt
LEFT JOIN postgresql.staging.player_per_game ppg
  ON ppg.player_id = pt.player_id AND ppg.season = pt.season
LEFT JOIN mongodb.lsdm.player_bio b
  ON b.player_id = CAST(pt.player_id AS VARCHAR)
WHERE UPPER(pt.team) = 'LAL' AND CAST(pt.season AS INTEGER) BETWEEN 2019 AND 2021
ORDER BY season DESC, player_name

-- Team meta AFTER
SELECT season, team_abbr, team_name, coach, arena_name, src_coach, src_arena
FROM memory.gav.global_team_season
WHERE team_abbr IN ('BOS','LAL') AND season = 2020
ORDER BY team_abbr;

-- Team meta BEFORE
SELECT
  CAST(ts.season AS INTEGER)                                 AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                 AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team), LOWER(td.nickname)) AS team_name,
  LOWER(td.meta.head_coach)                                  AS coach,
  COALESCE(LOWER(ts.arena), LOWER(td.meta.arena.name))       AS arena_name
FROM postgresql.staging.team_summaries ts
LEFT JOIN postgresql.staging.team_abbrev ta
  ON ta.season = ts.season AND ta.abbreviation = ts.abbreviation
LEFT JOIN mongodb.lsdm.team_details td
  ON UPPER(td.abbreviation) = COALESCE(ts.abbreviation, ta.abbreviation)
WHERE COALESCE(ts.abbreviation, ta.abbreviation) IN ('BOS','LAL') AND CAST(ts.season AS INTEGER) = 2020
ORDER BY team_abbr;


