-- Players per-season (Totals)
DROP TABLE IF EXISTS staging.player_totals CASCADE;
CREATE TABLE staging.player_totals (
  season text,
  lg text,
  player text,
  player_id text,
  age int,
  team text,
  pos text,
  g int,
  gs int,
  mp double precision,
  fg double precision,
  fga double precision,
  fg_percent double precision,
  x3p double precision,
  x3pa double precision,
  x3p_percent double precision,
  x2p double precision,
  x2pa double precision,
  x2p_percent double precision,
  e_fg_percent double precision,
  ft double precision,
  fta double precision,
  ft_percent double precision,
  orb double precision,
  drb double precision,
  trb double precision,
  ast double precision,
  stl double precision,
  blk double precision,
  tov double precision,
  pf double precision,
  pts double precision,
  trp_dbl int
);

-- Players per-game
DROP TABLE IF EXISTS staging.player_per_game CASCADE;
CREATE TABLE staging.player_per_game (
  season text,
  lg text,
  player text,
  player_id text,
  age int,
  team text,
  pos text,
  g int,
  gs int,
  mp_per_game double precision,
  fg_per_game double precision,
  fga_per_game double precision,
  fg_percent double precision,
  x3p_per_game double precision,
  x3pa_per_game double precision,
  x3p_percent double precision,
  x2p_per_game double precision,
  x2pa_per_game double precision,
  x2p_percent double precision,
  e_fg_percent double precision,
  ft_per_game double precision,
  fta_per_game double precision,
  ft_percent double precision,
  orb_per_game double precision,
  drb_per_game double precision,
  trb_per_game double precision,
  ast_per_game double precision,
  stl_per_game double precision,
  blk_per_game double precision,
  tov_per_game double precision,
  pf_per_game double precision,
  pts_per_game double precision
);

-- Player bio (from common_player_info.csv or JSONL)
DROP TABLE IF EXISTS staging.player_bio CASCADE;
CREATE TABLE staging.player_bio (
  player_id text,
  full_name text,
  first_name text,
  last_name text,
  birthdate text,
  school text,
  country text,
  height text,
  weight text,
  position text,
  team_id text,
  team_name text,
  team_abbreviation text,
  from_year text,
  to_year text,
  draft_year text,
  draft_round text,
  draft_number text,
  src text
);

-- Team summaries per-season
DROP TABLE IF EXISTS staging.team_summaries CASCADE;
CREATE TABLE staging.team_summaries (
  season text,
  lg text,
  team text,
  abbreviation text,
  playoffs text,
  age double precision,
  w int,
  l int,
  pw int,
  pl int,
  mov double precision,
  sos double precision,
  srs double precision,
  o_rtg double precision,
  d_rtg double precision,
  n_rtg double precision,
  pace double precision,
  f_tr double precision,
  x3p_ar double precision,
  ts_percent double precision,
  e_fg_percent double precision,
  tov_percent double precision,
  orb_percent double precision,
  ft_fga double precision,
  opp_e_fg_percent double precision,
  opp_tov_percent double precision,
  drb_percent double precision,
  opp_ft_fga double precision,
  arena text,
  attend int,
  attend_g int
);

-- Team Abbrev per-season
DROP TABLE IF EXISTS staging.team_abbrev CASCADE;
CREATE TABLE staging.team_abbrev (
  season text,
  lg text,
  team text,
  abbreviation text,
  playoffs text
);

-- Team details static (fallback)
DROP TABLE IF EXISTS staging.team_details CASCADE;
CREATE TABLE staging.team_details (
  team_id text,
  abbreviation text,
  nickname text,
  yearfounded text,
  city text,
  arena text,
  arenacapacity text,
  owner text,
  generalmanager text,
  headcoach text,
  dleagueaffiliation text,
  facebook text,
  instagram text,
  twitter text,
  src text
);

-- Games
DROP TABLE IF EXISTS staging.games CASCADE;
CREATE TABLE staging.games (
  game_id text,
  game_date text,
  season_label text,
  home_team_id text,
  home_team_city text,
  home_team_name text,
  home_score int,
  away_team_id text,
  away_team_city text,
  away_team_name text,
  away_score int,
  winner text,
  game_type text,
  attendance int,
  arena_id text
);

-- Advanced player stats per-season
DROP TABLE IF EXISTS staging.player_advanced CASCADE;
CREATE TABLE staging.player_advanced (
  season text,
  lg text,
  player text,
  player_id text,
  age int,
  team text,
  pos text,
  g int,
  gs int,
  mp double precision,
  per double precision,
  ts_percent double precision,
  x3p_ar double precision,
  f_tr double precision,
  orb_percent double precision,
  drb_percent double precision,
  trb_percent double precision,
  ast_percent double precision,
  stl_percent double precision,
  blk_percent double precision,
  tov_percent double precision,
  usg_percent double precision,
  ows double precision,
  dws double precision,
  ws double precision,
  ws_48 double precision,
  obpm double precision,
  dbpm double precision,
  bpm double precision,
  vorp double precision
);

-- Box score logs (legacy per-game)
DROP TABLE IF EXISTS staging.player_box_score CASCADE;
CREATE TABLE staging.player_box_score (
  season text,
  game_id text,
  player_name text,
  team text,
  game_date text,
  matchup text,
  wl text,
  min text,
  fgm double precision,
  fga double precision,
  fg_pct double precision,
  fg3m double precision,
  fg3a double precision,
  fg3_pct double precision,
  ftm double precision,
  fta double precision,
  ft_pct double precision,
  oreb double precision,
  dreb double precision,
  reb double precision,
  ast double precision,
  stl double precision,
  blk double precision,
  tov double precision,
  pf double precision,
  pts double precision,
  plus_minus double precision
);

-- Additional optional staging tables for more CSVs
DROP TABLE IF EXISTS staging.team_stats_per_game CASCADE;
CREATE TABLE staging.team_stats_per_game (
  season text,
  lg text,
  team text,
  abbreviation text,
  playoffs text,
  g int,
  mp_per_game double precision,
  fg_per_game double precision,
  fga_per_game double precision,
  fg_percent double precision,
  x3p_per_game double precision,
  x3pa_per_game double precision,
  x3p_percent double precision,
  x2p_per_game double precision,
  x2pa_per_game double precision,
  x2p_percent double precision,
  ft_per_game double precision,
  fta_per_game double precision,
  ft_percent double precision,
  orb_per_game double precision,
  drb_per_game double precision,
  trb_per_game double precision,
  ast_per_game double precision,
  stl_per_game double precision,
  blk_per_game double precision,
  tov_per_game double precision,
  pf_per_game double precision,
  pts_per_game double precision
);

DROP TABLE IF EXISTS staging.team_stats_per_100 CASCADE;
CREATE TABLE staging.team_stats_per_100 (
  season text,
  lg text,
  team text,
  abbreviation text,
  playoffs text,
  g int,
  mp double precision,
  fg_per_100_poss double precision,
  fga_per_100_poss double precision,
  fg_percent double precision,
  x3p_per_100_poss double precision,
  x3pa_per_100_poss double precision,
  x3p_percent double precision,
  x2p_per_100_poss double precision,
  x2pa_per_100_poss double precision,
  x2p_percent double precision,
  e_fg_percent double precision,
  ft_per_100_poss double precision,
  fta_per_100_poss double precision,
  ft_percent double precision,
  orb_per_100_poss double precision,
  drb_per_100_poss double precision,
  trb_per_100_poss double precision,
  ast_per_100_poss double precision,
  stl_per_100_poss double precision,
  blk_per_100_poss double precision,
  tov_per_100_poss double precision,
  pf_per_100_poss double precision,
  pts_per_100_poss double precision
);

DROP TABLE IF EXISTS staging.team_totals CASCADE;
CREATE TABLE staging.team_totals (
  season text,
  lg text,
  team text,
  abbreviation text,
  playoffs text,
  g int,
  mp double precision,
  fg double precision,
  fga double precision,
  fg_percent double precision,
  x3p double precision,
  x3pa double precision,
  x3p_percent double precision,
  x2p double precision,
  x2pa double precision,
  x2p_percent double precision,
  e_fg_percent double precision,
  ft double precision,
  fta double precision,
  ft_percent double precision,
  orb double precision,
  drb double precision,
  trb double precision,
  ast double precision,
  stl double precision,
  blk double precision,
  tov double precision,
  pf double precision,
  pts double precision
);

DROP TABLE IF EXISTS staging.opponent_stats_per_100 CASCADE;
CREATE TABLE staging.opponent_stats_per_100 (
  season text,
  lg text,
  team text,
  abbreviation text,
  g int,
  mp double precision,
  opp_fg_per_100_poss double precision,
  opp_fga_per_100_poss double precision,
  opp_fg_percent double precision,
  opp_x3p_per_100_poss double precision,
  opp_x3pa_per_100_poss double precision,
  opp_x3p_percent double precision,
  opp_x2p_per_100_poss double precision,
  opp_x2pa_per_100_poss double precision,
  opp_x2p_percent double precision,
  opp_e_fg_percent double precision,
  opp_ft_per_100_poss double precision,
  opp_fta_per_100_poss double precision,
  opp_ft_percent double precision,
  opp_orb_per_100_poss double precision,
  opp_drb_per_100_poss double precision,
  opp_trb_per_100_poss double precision,
  opp_ast_per_100_poss double precision,
  opp_stl_per_100_poss double precision,
  opp_blk_per_100_poss double precision,
  opp_tov_per_100_poss double precision,
  opp_pf_per_100_poss double precision,
  opp_pts_per_100_poss double precision
);

DROP TABLE IF EXISTS staging.player_shooting CASCADE;
CREATE TABLE staging.player_shooting (
  season text,
  lg text,
  player text,
  player_id text,
  age int,
  team text,
  pos text,
  g int,
  mp double precision,
  fg_pct double precision,
  x2p_percent double precision,
  x3p_percent double precision,
  ft_percent double precision
);

DROP TABLE IF EXISTS staging.player_play_by_play CASCADE;
CREATE TABLE staging.player_play_by_play (
  season text,
  lg text,
  player text,
  player_id text,
  age int,
  team text,
  pos text,
  g int,
  gs int,
  mp double precision
);

DROP TABLE IF EXISTS staging.seasons_stats CASCADE;
CREATE TABLE staging.seasons_stats (
  year text,
  player text,
  pos text,
  age int,
  tm text,
  g int,
  gs int,
  mp double precision,
  per double precision,
  ts_percent double precision,
  ws double precision,
  vorp double precision
);

DROP TABLE IF EXISTS staging.nba_head_coaches CASCADE;
-- Match CSV header from data/NBA_head_coaches.csv (handle embedded commas/quotes)
CREATE TABLE staging.nba_head_coaches (
  name text,
  teams text,
  start_season text,
  end_season text,
  years_in_rule text,
  birth_date text,
  nationality text,
  start_season_short text,
  end_season_short text,
  num_of_teams text
);
