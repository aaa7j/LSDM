-- Create logical GAV schema in Trino (in-memory)
CREATE SCHEMA IF NOT EXISTS memory.gav;

-- Global player per-season view (CPI-first attributes, PT-driven roster)
CREATE OR REPLACE VIEW memory.gav.global_player_season AS
WITH
pt_roster AS (
  SELECT
    pt.player_id                                                AS pt_player_id,
    MAX(pt.player)                                   AS player_raw,
    REGEXP_REPLACE(TRANSLATE(LOWER(pt.player), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', '') AS pt_norm_name,
    REGEXP_REPLACE(TRANSLATE(LOWER(regexp_extract(pt.player, '(^| )([^ ]+)$', 2)), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', '') AS pt_last_norm,
    SUBSTR(REGEXP_REPLACE(TRANSLATE(LOWER(regexp_extract(pt.player, '^([A-Za-z0-9]+)', 1)), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', ''), 1, 2) AS pt_first2_norm,
    TRY_CAST(pt.season AS INTEGER)                              AS season,
    UPPER(pt.team)                                              AS team_abbr,
    UPPER(pt.pos)                                               AS pt_pos,
    MAX(COALESCE(TRY_CAST(pt.g  AS INTEGER), 0))                AS g,
    MAX(COALESCE(TRY_CAST(pt.gs AS INTEGER), 0))                AS gs,
    MAX(TRY_CAST(pt.pts AS DOUBLE))                             AS pts,
    MAX(TRY_CAST(pt.ast AS DOUBLE))                             AS ast,
    MAX(TRY_CAST(pt.trb AS DOUBLE))                             AS trb
  FROM mongodb.lsdm.player_totals pt
  GROUP BY pt.player_id, LOWER(pt.player),
           REGEXP_REPLACE(TRANSLATE(LOWER(pt.player), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', ''),
           REGEXP_REPLACE(TRANSLATE(LOWER(regexp_extract(pt.player, '(^| )([^ ]+)$', 2)), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', ''),
           SUBSTR(REGEXP_REPLACE(TRANSLATE(LOWER(regexp_extract(pt.player, '^([A-Za-z0-9]+)', 1)), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', ''), 1, 2),
           TRY_CAST(pt.season AS INTEGER), UPPER(pt.team), UPPER(pt.pos)
),
cpi_info AS (
  SELECT player_id, full_name, first_name, last_name, position, height, weight
  FROM mongodb.lsdm.common_player_info
),
cand AS (
  -- Priority 1: by exact player_id
  SELECT pr.*, c.player_id AS cpi_id, c.full_name AS cpi_full_name, c.position AS cpi_pos,
         c.height AS cpi_height, c.weight AS cpi_weight, 1 AS prio
  FROM pt_roster pr JOIN cpi_info c ON c.player_id = pr.pt_player_id
  UNION ALL
  -- Priority 2: by normalized full_name
  SELECT pr.*, c.player_id, c.full_name, c.position, c.height, c.weight, 2 AS prio
  FROM pt_roster pr JOIN cpi_info c
    ON REGEXP_REPLACE(TRANSLATE(LOWER(c.full_name), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', '') = pr.pt_norm_name
  UNION ALL
  -- Priority 3: by last_name + first 2 of first_name (normalized)
  SELECT pr.*, c.player_id, c.full_name, c.position, c.height, c.weight, 3 AS prio
  FROM pt_roster pr JOIN cpi_info c
    ON REGEXP_REPLACE(TRANSLATE(LOWER(c.last_name),  'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', '') = pr.pt_last_norm
   AND SUBSTR(REGEXP_REPLACE(TRANSLATE(LOWER(c.first_name), 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd'), '[^a-z0-9]', ''), 1, 2) = pr.pt_first2_norm
  UNION ALL
  -- Fallback: no CPI match
  SELECT pr.*, CAST(NULL AS VARCHAR) AS cpi_id, CAST(NULL AS VARCHAR) AS cpi_full_name,
         CAST(NULL AS VARCHAR) AS cpi_pos, CAST(NULL AS VARCHAR) AS cpi_height,
         CAST(NULL AS VARCHAR) AS cpi_weight, 99 AS prio
  FROM pt_roster pr
),
best AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY pt_player_id, season, team_abbr ORDER BY prio) AS rn
  FROM cand
),
joined AS (
  SELECT
    COALESCE(cpi_id, pt_player_id)                               AS player_id,
    COALESCE(cpi_full_name, player_raw)                          AS player_name,
    season,
    team_abbr,
    COALESCE(UPPER(cpi_pos), pt_pos)                             AS position,
    CAST(ROUND(
      CASE WHEN cpi_height IS NOT NULL AND REGEXP_LIKE(cpi_height, '^[0-9]+-[0-9]+$')
           THEN 2.54 * (
             TRY_CAST(element_at(split(cpi_height, '-'), 1) AS DOUBLE) * 12 +
             TRY_CAST(element_at(split(cpi_height, '-'), 2) AS DOUBLE)
           )
      END
    ) AS INTEGER)                                                AS height_cm,
    CAST(ROUND(
      CASE WHEN cpi_weight IS NOT NULL
           THEN 0.45359237 * TRY_CAST(regexp_extract(cpi_weight, '^[0-9]+', 0) AS DOUBLE)
      END
    ) AS INTEGER)                                                AS weight_kg,
    g, gs, pts, ast, trb,
    CASE WHEN cpi_pos IS NOT NULL THEN 'common_player_info' ELSE 'player_totals' END AS src_position
  FROM best
  WHERE rn = 1
)
SELECT
  j.player_id,
  TRANSLATE(j.player_name, 'áàäâãåéèëêíìïîóòöôõúùüûñçčćšžđ', 'aaaaaaeeeeiiiiooooouuuuncccszd') AS player_name,
  j.season,
  j.team_abbr,
  UPPER(j.position)                              AS position,
  j.height_cm                                    AS height_cm,
  j.weight_kg                                    AS weight_kg,
  j.g, j.gs, j.pts, j.ast, j.trb,
  ppg.pts_per_game, ppg.ast_per_game, ppg.trb_per_game,
  COALESCE(adv.per, ss.per)                      AS per,
  CASE WHEN COALESCE(adv.ts_percent, ss.ts_percent) > 1 THEN COALESCE(adv.ts_percent, ss.ts_percent) / 100.0
       ELSE COALESCE(adv.ts_percent, ss.ts_percent) END       AS ts_percent,
  COALESCE(adv.ws, ss.ws)                        AS ws,
  adv.bpm,
  COALESCE(adv.vorp, ss.vorp)                    AS vorp,
  psi.experience                                 AS experience_years,
  j.src_position
FROM joined j
LEFT JOIN (
  SELECT player_id,
         TRY_CAST(season AS INTEGER) AS season,
         UPPER(team) AS team_abbr,
         AVG(pts_per_game) AS pts_per_game,
         AVG(ast_per_game) AS ast_per_game,
         AVG(trb_per_game) AS trb_per_game
  FROM postgresql.staging.player_per_game
  GROUP BY 1,2,3
) ppg
  ON ppg.player_id = j.player_id AND ppg.season = j.season AND ppg.team_abbr = j.team_abbr
LEFT JOIN (
  SELECT player_id,
         TRY_CAST(season AS INTEGER) AS season,
         AVG(per)        AS per,
         AVG(ts_percent) AS ts_percent,
         AVG(ws)         AS ws,
         AVG(bpm)        AS bpm,
         AVG(vorp)       AS vorp
  FROM postgresql.staging.player_advanced
  GROUP BY 1,2
) adv
  ON adv.player_id = j.player_id AND adv.season = j.season
LEFT JOIN (
  SELECT player_id,
         TRY_CAST(season AS INTEGER) AS season,
         MAX(experience) AS experience
  FROM postgresql.staging.player_season_info
  GROUP BY 1,2
) psi
  ON psi.player_id = j.player_id AND psi.season = j.season
LEFT JOIN (
  SELECT LOWER(player) AS player_name,
         TRY_CAST(year AS INTEGER) AS season,
         AVG(per)        AS per,
         AVG(ts_percent) AS ts_percent,
         AVG(ws)         AS ws,
         AVG(vorp)       AS vorp
  FROM postgresql.staging.seasons_stats
  GROUP BY 1,2
) ss
  ON ss.player_name = LOWER(j.player_name) AND ss.season = j.season;

-- Global team per-season view (comprehensive metrics)
CREATE OR REPLACE VIEW memory.gav.global_team_season AS
SELECT
  TRY_CAST(ts.season AS INTEGER)                                         AS season,
  COALESCE(ts.abbreviation, tpg.abbreviation, t100.abbreviation, ta.abbreviation) AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name))                          AS team_name,
  ts.attend, ts.attend_g,
  -- Summary ratings (team_summaries)
  ts.mov, ts.sos, ts.srs, ts.o_rtg, ts.d_rtg, ts.n_rtg, ts.pace,
  ts.f_tr, ts.x3p_ar, ts.ts_percent, ts.e_fg_percent, ts.tov_percent,
  ts.orb_percent, ts.ft_fga, ts.opp_e_fg_percent, ts.opp_tov_percent,
  ts.drb_percent, ts.opp_ft_fga,
  -- Per-team aggregates (team_stats_per_game)
  tpg.g AS games_pg,
  tpg.mp AS tpg_minutes,
  tpg.fg AS tpg_fg, tpg.fga AS tpg_fga, tpg.fg_percent AS tpg_fg_percent,
  tpg.x3p AS tpg_x3p, tpg.x3pa AS tpg_x3pa, tpg.x3p_percent AS tpg_x3p_percent,
  tpg.x2p AS tpg_x2p, tpg.x2pa AS tpg_x2pa, tpg.x2p_percent AS tpg_x2p_percent,
  tpg.ft AS tpg_ft, tpg.fta AS tpg_fta, tpg.ft_percent AS tpg_ft_percent,
  tpg.orb AS tpg_orb, tpg.drb AS tpg_drb, tpg.trb AS tpg_trb, tpg.ast AS tpg_ast,
  tpg.stl AS tpg_stl, tpg.blk AS tpg_blk, tpg.tov AS tpg_tov, tpg.pf AS tpg_pf, tpg.pts AS tpg_pts,
  -- Derived: points per game with fallback from team_totals when per-game table is missing
  COALESCE(
    tpg.pts,
    CASE
      WHEN tot.pts IS NOT NULL AND tot.g IS NOT NULL AND TRY_CAST(tot.g AS DOUBLE) <> 0 THEN TRY_CAST(tot.pts AS DOUBLE) / TRY_CAST(tot.g AS DOUBLE)
    END
  ) AS points_per_game,

  -- Per 100 possessions (team_stats_per_100_poss)
  t100.fg_per_100_poss, t100.fga_per_100_poss, t100.fg_percent AS t100_fg_percent,
  t100.x3p_per_100_poss, t100.x3pa_per_100_poss, t100.x3p_percent AS t100_x3p_percent,
  t100.x2p_per_100_poss, t100.x2pa_per_100_poss, t100.x2p_percent AS t100_x2p_percent,
  t100.ft_per_100_poss, t100.fta_per_100_poss, t100.ft_percent AS t100_ft_percent,
  t100.orb_per_100_poss, t100.drb_per_100_poss, t100.trb_per_100_poss,
  t100.ast_per_100_poss, t100.stl_per_100_poss, t100.blk_per_100_poss,
  t100.tov_per_100_poss, t100.pf_per_100_poss, t100.pts_per_100_poss,
  -- Opponent per 100 possessions
  opp.opp_fg_per_100_poss, opp.opp_fga_per_100_poss, opp.opp_fg_percent,
  opp.opp_x3p_per_100_poss, opp.opp_x3pa_per_100_poss, opp.opp_x3p_percent,
  opp.opp_x2p_per_100_poss, opp.opp_x2pa_per_100_poss, opp.opp_x2p_percent,
  opp.opp_ft_per_100_poss, opp.opp_fta_per_100_poss, opp.opp_ft_percent,
  opp.opp_orb_per_100_poss, opp.opp_drb_per_100_poss, opp.opp_trb_per_100_poss,
  opp.opp_ast_per_100_poss, opp.opp_stl_per_100_poss, opp.opp_blk_per_100_poss,
  opp.opp_tov_per_100_poss, opp.opp_pf_per_100_poss, opp.opp_pts_per_100_poss,
  -- Totals (team_totals)
  tot.mp AS tot_minutes, tot.fg AS tot_fg, tot.fga AS tot_fga, tot.fg_percent AS tot_fg_percent,
  tot.x3p AS tot_x3p, tot.x3pa AS tot_x3pa, tot.x3p_percent AS tot_x3p_percent,
  tot.x2p AS tot_x2p, tot.x2pa AS tot_x2pa, tot.x2p_percent AS tot_x2p_percent,
  tot.ft AS tot_ft, tot.fta AS tot_fta, tot.ft_percent AS tot_ft_percent,
  tot.orb AS tot_orb, tot.drb AS tot_drb, tot.trb AS tot_trb, tot.ast AS tot_ast,
  tot.stl AS tot_stl, tot.blk AS tot_blk, tot.tov AS tot_tov, tot.pf AS tot_pf, tot.pts AS tot_pts
FROM postgresql.staging.team_summaries ts
LEFT JOIN postgresql.staging.team_stats_per_game tpg
  ON TRY_CAST(tpg.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(tpg.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN postgresql.staging.team_stats_per_100_poss t100
  ON TRY_CAST(t100.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(t100.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN postgresql.staging.opponent_stats_per_100_poss opp
  ON TRY_CAST(opp.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(opp.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN postgresql.staging.team_totals tot
  ON TRY_CAST(tot.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(tot.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN mongodb.lsdm.team_abbrev ta
  ON TRY_CAST(ta.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(ta.abbreviation) = UPPER(ts.abbreviation)
WHERE ts.season IS NOT NULL
  AND COALESCE(ts.abbreviation, tpg.abbreviation, t100.abbreviation, ta.abbreviation) IS NOT NULL
  AND COALESCE(ts.abbreviation, tpg.abbreviation, t100.abbreviation, ta.abbreviation) <> '';

-- Global game view (from Mongo games JSON)
CREATE OR REPLACE VIEW memory.gav.global_game AS
SELECT
  g.game_id,
  TRY_CAST(g.date AS DATE)                                   AS game_date,
  COALESCE(TRY_CAST(g.season AS INTEGER), TRY_CAST(SUBSTR(g.season,1,4) AS INTEGER), year(TRY_CAST(g.date AS DATE))) AS season,
  g.home.team_id          AS home_team_id,
  LOWER(g.home.team_city) AS home_team_city,
  LOWER(g.home.team_name) AS home_team_name,
  TRY_CAST(g.home.score AS INTEGER)                           AS home_score,
  g.away.team_id          AS away_team_id,
  LOWER(g.away.team_city) AS away_team_city,
  LOWER(g.away.team_name) AS away_team_name,
  TRY_CAST(g.away.score AS INTEGER)                           AS away_score,
  LOWER(g.winner)         AS winner,
  TRY_CAST(g.attendance AS INTEGER)                           AS attendance
FROM mongodb.lsdm.games g;

-- Player dimension (from common_player_info)
CREATE OR REPLACE VIEW memory.gav.dim_player AS
SELECT DISTINCT
  cpi.player_id,
  LOWER(cpi.full_name)      AS full_name,
  LOWER(cpi.first_name)     AS first_name,
  LOWER(cpi.last_name)      AS last_name,
  cpi.birthdate,
  LOWER(cpi.country)        AS country,
  LOWER(cpi.school)         AS college,
  cpi.height,
  cpi.weight,
  UPPER(cpi.team_abbreviation) AS team_abbr,
  cpi.from_year, cpi.to_year, cpi.draft_year, cpi.draft_round, cpi.draft_number
FROM mongodb.lsdm.common_player_info cpi;

-- Team dimension (from team_details)
CREATE OR REPLACE VIEW memory.gav.dim_team_season AS
SELECT DISTINCT
  TRY_CAST(ts.season AS INTEGER)                                         AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                             AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name), LOWER(td.nickname))      AS team_name,
  LOWER(td.meta.city)                                                    AS city,
  LOWER(COALESCE(ts.arena, td.meta.arena.name))                          AS arena_name,
  TRY_CAST(td.meta.arena.capacity AS INTEGER)                            AS arena_capacity,
  LOWER(COALESCE(td.meta.head_coach, nhc.name))                          AS coach,
  ts.lg                                                                  AS league,
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
WHERE ts.season IS NOT NULL
  AND COALESCE(ts.abbreviation, ta.abbreviation) IS NOT NULL
  AND COALESCE(ts.abbreviation, ta.abbreviation) <> '';

-- Player game box scores (from large historical CSV)
CREATE OR REPLACE VIEW memory.gav.player_game_box AS
SELECT
  season,
  game_id,
  LOWER(player_name) AS player_name,
  UPPER(team)        AS team_abbr,
  TRY_CAST(game_date AS DATE) AS game_date,
  LOWER(matchup)     AS matchup,
  LOWER(wl)          AS wl,
  min                AS minutes,
  fgm,
  TRY_CAST(fga AS DOUBLE) AS fga,
  fg_pct,
  TRY_CAST(fg3m AS DOUBLE) AS fg3m,
  TRY_CAST(fg3a AS DOUBLE) AS fg3a,
  TRY_CAST(fg3_pct AS DOUBLE) AS fg3_pct,
  ftm,
  TRY_CAST(fta AS DOUBLE) AS fta,
  ft_pct,
  TRY_CAST(oreb AS DOUBLE) AS oreb,
  TRY_CAST(dreb AS DOUBLE) AS dreb,
  TRY_CAST(reb AS DOUBLE)  AS reb,
  ast,
  TRY_CAST(stl AS DOUBLE) AS stl,
  TRY_CAST(blk AS DOUBLE) AS blk,
  TRY_CAST(tov AS DOUBLE) AS tov,
  pf,
  pts,
  TRY_CAST(plus_minus AS DOUBLE) AS plus_minus
FROM postgresql.staging.nba_player_box_score_stats_1950_2022;
