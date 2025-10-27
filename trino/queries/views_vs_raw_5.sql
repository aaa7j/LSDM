-- Views vs Raw: 5 concise examples to showcase simplicity
-- Run with: java -jar trino-cli.jar --server http://localhost:8080 --file trino/queries/views_vs_raw_5.sql

-- 1) Roster + position/physicals for a team over seasons
-- AFTER (view)
SELECT player_name, season, team_abbr, position, height_cm, weight_kg, src_position
FROM memory.gav.global_player_season
WHERE team_abbr = 'LAL' AND season BETWEEN 2019 AND 2021
ORDER BY season DESC, player_name
LIMIT 20;

-- BEFORE (raw)
SELECT
  COALESCE(LOWER(cpi.full_name), LOWER(pt.player))            AS player_name,
  TRY_CAST(pt.season AS INTEGER)                              AS season,
  UPPER(pt.team)                                              AS team_abbr,
  COALESCE(cpi.position, UPPER(pt.pos), UPPER(ppg.pos))       AS position,
  cpi.height, cpi.weight,
  CASE WHEN cpi.position IS NOT NULL THEN 'common_player_info'
       WHEN pt.pos IS NOT NULL THEN 'player_totals'
       WHEN ppg.pos IS NOT NULL THEN 'player_per_game'
  END                                                         AS src_position
FROM mongodb.lsdm.player_totals pt
LEFT JOIN postgresql.staging.player_per_game ppg
  ON ppg.player_id = pt.player_id
 AND TRY_CAST(ppg.season AS INTEGER) = TRY_CAST(pt.season AS INTEGER)
 AND UPPER(ppg.team) = UPPER(pt.team)
LEFT JOIN mongodb.lsdm.common_player_info cpi
  ON cpi.player_id = pt.player_id
WHERE UPPER(pt.team) = 'LAL' AND TRY_CAST(pt.season AS INTEGER) BETWEEN 2019 AND 2021
ORDER BY season DESC, player_name
LIMIT 20;

-- 2) Team meta (team name, coach, arena) for selected teams in a season
-- AFTER (view)
SELECT season, team_abbr, team_name, coach, arena_name, src_coach, src_arena
FROM memory.gav.dim_team_season
WHERE team_abbr IN ('BOS','LAL') AND season = 2020
ORDER BY team_abbr;

-- BEFORE (raw)
SELECT
  TRY_CAST(ts.season AS INTEGER)                                           AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                               AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name), LOWER(td.nickname))        AS team_name,
  LOWER(COALESCE(td.meta.head_coach, nhc.name))                            AS coach,
  LOWER(COALESCE(ts.arena, td.meta.arena.name))                            AS arena_name,
  CASE WHEN ts.arena IS NOT NULL THEN 'team_summaries' ELSE 'team_details' END AS src_arena,
  CASE WHEN td.abbreviation IS NOT NULL THEN 'team_details'
       WHEN nhc.name IS NOT NULL THEN 'nba_head_coaches' END               AS src_coach
FROM postgresql.staging.team_summaries ts
LEFT JOIN mongodb.lsdm.team_abbrev ta
  ON TRY_CAST(ta.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(ta.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN mongodb.lsdm.team_details td
  ON UPPER(td.abbreviation) = UPPER(COALESCE(ts.abbreviation, ta.abbreviation))
LEFT JOIN postgresql.staging.nba_head_coaches nhc
  ON TRY_CAST(ts.season AS INTEGER) BETWEEN TRY_CAST(SUBSTRING(nhc.start_season, 1, 4) AS INTEGER)
                     AND TRY_CAST(SUBSTRING(nhc.end_season,   1, 4) AS INTEGER)
 AND REGEXP_LIKE(UPPER(nhc.teams), CONCAT('(^|[,\\s])', UPPER(ts.abbreviation), '([,\\s]|$)'))
WHERE COALESCE(ts.abbreviation, ta.abbreviation) IN ('BOS','LAL')
  AND TRY_CAST(ts.season AS INTEGER) = 2020
ORDER BY team_abbr;

-- 3) Top 10 by PER for a given season (advanced metrics with fallback and normalization)
-- AFTER (view)
SELECT player_name, team_abbr, per, ts_percent, ws, vorp
FROM memory.gav.global_player_season
WHERE season = 2016 AND team_abbr = 'CLE'
ORDER BY per DESC NULLS LAST
LIMIT 10;

-- BEFORE (raw)
WITH adv AS (
  SELECT player_id,
         TRY_CAST(season AS INTEGER) AS season,
         AVG(per)        AS per,
         AVG(ts_percent) AS ts_percent,
         AVG(ws)         AS ws,
         AVG(bpm)        AS bpm,
         AVG(vorp)       AS vorp
  FROM postgresql.staging.player_advanced
  GROUP BY 1,2
), ss AS (
  SELECT LOWER(player) AS player_name,
         TRY_CAST(year AS INTEGER) AS season,
         AVG(per)        AS per,
         AVG(ts_percent) AS ts_percent,
         AVG(ws)         AS ws,
         AVG(vorp)       AS vorp
  FROM postgresql.staging.seasons_stats
  GROUP BY 1,2
)
SELECT
  COALESCE(LOWER(cpi.full_name), LOWER(pt.player))                        AS player_name,
  UPPER(pt.team)                                                          AS team_abbr,
  COALESCE(adv.per, ss.per)                                               AS per,
  CASE WHEN COALESCE(adv.ts_percent, ss.ts_percent) > 1
       THEN COALESCE(adv.ts_percent, ss.ts_percent) / 100.0
       ELSE COALESCE(adv.ts_percent, ss.ts_percent) END                   AS ts_percent,
  COALESCE(adv.ws, ss.ws)                                                 AS ws,
  COALESCE(adv.vorp, ss.vorp)                                             AS vorp
FROM mongodb.lsdm.player_totals pt
LEFT JOIN adv
  ON adv.player_id = pt.player_id
 AND adv.season    = TRY_CAST(pt.season AS INTEGER)
LEFT JOIN ss
  ON ss.player_name = LOWER(pt.player)
 AND ss.season      = TRY_CAST(pt.season AS INTEGER)
LEFT JOIN mongodb.lsdm.common_player_info cpi
  ON cpi.player_id = pt.player_id
WHERE TRY_CAST(pt.season AS INTEGER) = 2016 AND UPPER(pt.team) = 'CLE'
ORDER BY per DESC NULLS LAST
LIMIT 10;

-- 4) Highest-attendance games in a season (date/season normalization and nesting)
-- AFTER (view)
SELECT game_id, game_date, season, home_team_city, home_team_name, away_team_city, away_team_name, attendance
FROM memory.gav.global_game
WHERE season = 2025 AND attendance IS NOT NULL
ORDER BY attendance DESC
LIMIT 10;

-- BEFORE (raw)
SELECT
  g.game_id,
  TRY_CAST(g.date AS DATE) AS game_date,
  COALESCE(TRY_CAST(g.season AS INTEGER), TRY_CAST(SUBSTR(g.season, 1, 4) AS INTEGER), year(TRY_CAST(g.date AS DATE))) AS season,
  LOWER(g.home.team_city)  AS home_team_city,
  LOWER(g.home.team_name)  AS home_team_name,
  LOWER(g.away.team_city)  AS away_team_city,
  LOWER(g.away.team_name)  AS away_team_name,
  TRY_CAST(g.attendance AS INTEGER) AS attendance
FROM mongodb.lsdm.games g
WHERE COALESCE(TRY_CAST(g.season AS INTEGER), TRY_CAST(SUBSTR(g.season, 1, 4) AS INTEGER), year(TRY_CAST(g.date AS DATE))) = 2025
  AND g.attendance IS NOT NULL
ORDER BY attendance DESC
LIMIT 10;

-- 5) Triple-doubles per player over a period (large box score CSV normalization)
-- AFTER (view)
SELECT player_name, COUNT(*) AS triple_doubles
FROM memory.gav.player_game_box
WHERE season BETWEEN 2010 AND 2020
  AND pts >= 10 AND reb >= 10 AND ast >= 10
GROUP BY player_name
ORDER BY triple_doubles DESC
LIMIT 15;

-- 6) Statistiche avanzate di squadra per stagione (LAL vs BOS, 2020)
-- Senza GAV (raw)
SELECT
  TRY_CAST(ts.season AS INTEGER) AS season,
  UPPER(ts.abbreviation)         AS team_abbr,
  LOWER(ts.team)                 AS team_name,
  ts.o_rtg,
  ts.d_rtg,
  ts.n_rtg,
  ts.pace,
  pg.pts AS points_per_game
FROM postgresql.staging.team_summaries ts
LEFT JOIN postgresql.staging.team_stats_per_100_poss t100
  ON TRY_CAST(t100.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(t100.abbreviation)        = UPPER(ts.abbreviation)
LEFT JOIN postgresql.staging.team_stats_per_game pg
  ON TRY_CAST(pg.season AS INTEGER)   = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(pg.abbreviation)          = UPPER(ts.abbreviation)
WHERE TRY_CAST(ts.season AS INTEGER) = 2020
  AND UPPER(ts.abbreviation) IN ('LAL','BOS')
ORDER BY team_abbr;

-- 7) Affluenza media stagionale per squadra (top 10, 2025)
-- Senza GAV (raw)
SELECT
  TRY_CAST(ts.season AS INTEGER)                                   AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                       AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name), LOWER(td.nickname)) AS team_name,
  ts.attend, ts.attend_g
FROM postgresql.staging.team_summaries ts
LEFT JOIN mongodb.lsdm.team_abbrev ta
  ON TRY_CAST(ta.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(ta.abbreviation)        = UPPER(ts.abbreviation)
LEFT JOIN mongodb.lsdm.team_details td
  ON UPPER(td.abbreviation) = UPPER(COALESCE(ts.abbreviation, ta.abbreviation))
WHERE TRY_CAST(ts.season AS INTEGER) = 2025
ORDER BY ts.attend DESC NULLS LAST
LIMIT 10;

-- Con GAV (vista)
SELECT season, team_abbr, team_name, attend, attend_g
FROM memory.gav.global_team_season
WHERE season = 2025
ORDER BY attend DESC NULLS LAST
LIMIT 10;

-- Con GAV (vista)
  o_rtg,
  d_rtg,
  n_rtg,
  pace,
  points_per_game
FROM memory.gav.global_team_season
WHERE season = 2020
  AND team_abbr IN ('LAL','BOS')
ORDER BY team_abbr;

-- BEFORE (raw)
SELECT LOWER(player_name) AS player_name, COUNT(*) AS triple_doubles
FROM postgresql.staging.nba_player_box_score_stats_1950_2022
WHERE TRY_CAST(season AS INTEGER) BETWEEN 2010 AND 2020
  AND TRY_CAST(pts AS INTEGER) >= 10
  AND TRY_CAST(reb AS DOUBLE)  >= 10
  AND TRY_CAST(ast AS INTEGER) >= 10
GROUP BY LOWER(player_name)
ORDER BY triple_doubles DESC
LIMIT 15;





