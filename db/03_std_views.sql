-- Normalize and standardize staging into std.*

-- season normalization helper (keep as view-wide expressions)

DROP VIEW IF EXISTS std.player_totals;
CREATE VIEW std.player_totals AS
SELECT
  season,
  LOWER(TRIM(player)) AS player_name,
  NULLIF(TRIM(player_id), '') AS player_id,
  UPPER(TRIM(team)) AS team_abbr,
  UPPER(TRIM(pos)) AS position,
  season::text AS season_start,
  (season::int + 1)::text AS season_end,
  CONCAT(season, '-', RIGHT((season::int + 1)::text, 2)) AS season_label,
  g, gs, mp, pts, ast, trb,
  fg, fga, fg_percent, x3p, x3pa, x3p_percent,
  x2p, x2pa, x2p_percent, e_fg_percent,
  ft, fta, ft_percent, orb, drb, stl, blk, tov, pf, trp_dbl
FROM staging.player_totals;

DROP VIEW IF EXISTS std.player_per_game;
CREATE VIEW std.player_per_game AS
SELECT
  season,
  LOWER(TRIM(player)) AS player_name,
  NULLIF(TRIM(player_id), '') AS player_id,
  UPPER(TRIM(team)) AS team_abbr,
  UPPER(TRIM(pos)) AS position,
  season::text AS season_start,
  (season::int + 1)::text AS season_end,
  CONCAT(season, '-', RIGHT((season::int + 1)::text, 2)) AS season_label,
  g, gs,
  mp_per_game, pts_per_game, ast_per_game, trb_per_game,
  fg_per_game, fga_per_game, fg_percent,
  x3p_per_game, x3pa_per_game, x3p_percent,
  x2p_per_game, x2pa_per_game, x2p_percent,
  e_fg_percent, ft_per_game, fta_per_game, ft_percent,
  orb_per_game, drb_per_game, stl_per_game, blk_per_game, tov_per_game, pf_per_game
FROM staging.player_per_game;

DROP VIEW IF EXISTS std.player_bio;
CREATE VIEW std.player_bio AS
SELECT
  NULLIF(TRIM(player_id), '') AS player_id,
  LOWER(TRIM(full_name)) AS full_name,
  LOWER(TRIM(first_name)) AS first_name,
  LOWER(TRIM(last_name)) AS last_name,
  birthdate,
  country,
  school AS college,
  position,
  height,
  weight,
  UPPER(TRIM(team_abbreviation)) AS team_abbr,
  src
FROM staging.player_bio;

DROP VIEW IF EXISTS std.team_summaries;
CREATE VIEW std.team_summaries AS
SELECT
  season,
  UPPER(TRIM(abbreviation)) AS team_abbr,
  LOWER(TRIM(team)) AS team_name,
  LOWER(TRIM(arena)) AS arena_name,
  attend,
  attend_g
FROM staging.team_summaries;

DROP VIEW IF EXISTS std.team_abbrev;
CREATE VIEW std.team_abbrev AS
SELECT
  season,
  UPPER(TRIM(abbreviation)) AS team_abbr,
  LOWER(TRIM(team)) AS team_name
FROM staging.team_abbrev;

DROP VIEW IF EXISTS std.team_details;
CREATE VIEW std.team_details AS
SELECT
  NULLIF(TRIM(team_id), '') AS team_id,
  UPPER(TRIM(abbreviation)) AS team_abbr,
  LOWER(TRIM(nickname)) AS team_name,
  LOWER(TRIM(city)) AS city,
  LOWER(TRIM(arena)) AS arena_name,
  headcoach AS coach,
  arenacapacity,
  src
FROM staging.team_details;

DROP VIEW IF EXISTS std.games;
CREATE VIEW std.games AS
SELECT
  game_id,
  game_date,
  season_label,
  home_team_id,
  home_team_city,
  home_team_name,
  home_score,
  away_team_id,
  away_team_city,
  away_team_name,
  away_score,
  attendance
FROM staging.games;

DROP VIEW IF EXISTS std.player_advanced;
CREATE VIEW std.player_advanced AS
SELECT
  season,
  LOWER(TRIM(player)) AS player_name,
  NULLIF(TRIM(player_id), '') AS player_id,
  UPPER(TRIM(team)) AS team_abbr,
  per, ts_percent, ws, bpm, vorp
FROM staging.player_advanced;

