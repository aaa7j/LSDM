-- Quality checks for GAV views (run with trino-cli --file trino/queries/qc.sql)

-- 1) Nulls in critical keys
-- Players
SELECT 'global_player_season NULL keys' AS check, COUNT(*) AS rows
FROM memory.gav.global_player_season
WHERE player_id IS NULL OR season IS NULL OR team_abbr IS NULL;

-- Teams
SELECT 'global_team_season NULL keys' AS check, COUNT(*) AS rows
FROM memory.gav.global_team_season
WHERE season IS NULL OR team_abbr IS NULL;

-- Games
SELECT 'global_game NULL keys' AS check, COUNT(*) AS rows
FROM memory.gav.global_game
WHERE game_id IS NULL OR season IS NULL;

-- 2) Duplicate keys
-- Players per-season duplicates
SELECT 'dup player-season' AS check, player_id, season, COUNT(*) AS n
FROM memory.gav.global_player_season
GROUP BY player_id, season, team_abbr HAVING COUNT(*) > 1
ORDER BY n DESC LIMIT 20;

-- Teams per-season duplicates
SELECT 'dup team-season' AS check, season, team_abbr, COUNT(*) AS n
FROM memory.gav.global_team_season
GROUP BY season, team_abbr HAVING COUNT(*) > 1
ORDER BY n DESC LIMIT 20;

-- Games duplicate game_id
SELECT 'dup game_id' AS check, game_id, COUNT(*) AS n
FROM memory.gav.global_game
GROUP BY game_id HAVING COUNT(*) > 1
ORDER BY n DESC LIMIT 20;

-- 3) Out-of-range and anomalies
-- Player totals plausibility
SELECT 'player totals out-of-range' AS check, COUNT(*) AS rows
FROM memory.gav.global_player_season
WHERE g < 0 OR g > 110 OR gs < 0 OR gs > 110
   OR pts < 0 OR pts > 5000 OR ast < 0 OR trb < 0;

-- Percent metrics expected in [0,1]
SELECT 'percent in (0,1) violations' AS check, COUNT(*) AS rows
FROM memory.gav.global_player_season
WHERE ts_percent IS NOT NULL AND (ts_percent < 0 OR ts_percent > 1);


-- Team per-game plausibility
SELECT 'team per-game plausibility' AS check, COUNT(*) AS rows
FROM memory.gav.global_team_season
WHERE tpg_pts IS NOT NULL AND (tpg_pts < 50 OR tpg_pts > 200);

-- 4) Join consistency (orphans)
-- Players present in Postgres but missing in Mongo bio
SELECT 'orphans PPG -> CPI' AS check, COUNT(DISTINCT ppg.player_id) AS missing
FROM postgresql.staging.player_per_game ppg
LEFT JOIN mongodb.lsdm.common_player_info cpi ON cpi.player_id = ppg.player_id
WHERE cpi.player_id IS NULL;

-- Team summaries without a matching Mongo team_details
SELECT 'orphans TS -> TEAM_DETAILS' AS check, COUNT(*) AS missing
FROM postgresql.staging.team_summaries ts
LEFT JOIN mongodb.lsdm.team_details td ON UPPER(td.abbreviation) = UPPER(ts.abbreviation)
WHERE td.abbreviation IS NULL;

-- 5) Provenance distribution
SELECT 'src_position share' AS check, src_position, COUNT(*) AS rows
FROM memory.gav.global_player_season
GROUP BY src_position
ORDER BY rows DESC;

SELECT 'src_arena share' AS check, src_arena, COUNT(*) AS rows
FROM memory.gav.dim_team_season
GROUP BY src_arena
ORDER BY rows DESC;

SELECT 'src_coach share' AS check, src_coach, COUNT(*) AS rows
FROM memory.gav.dim_team_season
GROUP BY src_coach
ORDER BY rows DESC;

-- 6) Season bounds sanity
SELECT 'season bounds player' AS check, MIN(season) AS min_season, MAX(season) AS max_season
FROM memory.gav.global_player_season;

SELECT 'season bounds team' AS check, MIN(season) AS min_season, MAX(season) AS max_season
FROM memory.gav.global_team_season;




