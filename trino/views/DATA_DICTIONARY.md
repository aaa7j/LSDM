GAV Data Dictionary (summary)

How to see live schemas in Trino
- All columns/types: run trino/queries/gav_schema.sql with the CLI
  - `java -jar trino-cli.jar --server http://localhost:8080 --file trino/queries/gav_schema.sql`
- Or per view: `DESCRIBE memory.gav.<view>` and `SHOW CREATE VIEW memory.gav.<view>`

Views and column provenance (concise)

1) memory.gav.global_player_season
- Key/grain: player_id, season, team_abbr
- From Mongo lsdm.player_totals (pt): player, season, team, pos, g, gs, pts, ast, trb
- From Postgres staging.player_per_game (ppg): season, team, pos, g, gs, pts_per_game, ast_per_game, trb_per_game
- From Postgres staging.player_advanced (adv, aggregated by player_id+season): per, ts_percent (0–1 normalized), ws, bpm, vorp
- From Postgres staging.seasons_stats (ss, aggregated fallback): per, ts_percent (0–1 normalized), ws, vorp
- From Postgres staging.player_season_info (psi, aggregated): experience
- From Mongo lsdm.common_player_info (cpi): full_name -> player_name, position (priority), height, weight, team_abbreviation (used in team_abbr fallback)
- Notable fusions: position = cpi -> pt -> ppg; team_abbr = pt -> ppg -> cpi; ts_percent normalized to [0,1]

2) memory.gav.global_team_season
- Key/grain: season, team_abbr
- From Postgres staging.team_summaries (ts): base team info + summary ratings (mov, srs, o_rtg, d_rtg, pace, ts_percent, ...)
- From Postgres staging.team_stats_per_game (tpg): per-team aggregates (fg,fga,fg_percent, x3p,x3pa, ... pts as tpg_pts)
- From Postgres staging.team_stats_per_100_poss (t100): per-100 metrics
- From Postgres staging.opponent_stats_per_100_poss (opp): opponent per-100 metrics
- From Postgres staging.team_totals (tot): season totals
- From Mongo lsdm.team_abbrev (ta): team_name/abbreviation fallback by season
- From Mongo lsdm.team_details (td): arena_name, head_coach
- From Postgres staging.nba_head_coaches (nhc): head coach fallback mapped by season window + team mention
- Notable fusions: team_abbr = ts -> tpg/t100 -> ta; team_name = ts -> ta -> td; arena_name = ts -> td; coach = td -> nhc; hard filter excludes NULL/empty keys

3) memory.gav.global_game
- Key/grain: game_id
- From Mongo lsdm.games: date -> game_date, season (from season, or 4-char prefix, or year(date)); home/away nested fields; attendance

4) memory.gav.dim_player
- Key/grain: player_id
- From Mongo lsdm.common_player_info: full_name, first_name, last_name, birthdate, country (nationality), school (college), height, weight, team_abbreviation, draft fields

5) memory.gav.dim_team
- Key/grain: team_abbr
- From Mongo lsdm.team_details: team_name, city, arena_name, arena_capacity, head_coach

6) memory.gav.player_game_box
- Key/grain: season, game_id, player_name, team_abbr
- From Postgres staging.nba_player_box_score_stats_1950_2022: game_date, fgm/fga/fg_pct, fg3*, ft*, reb/ast/stl/blk/tov/pf/pts, plus_minus

Notes
- All cross-catalog joins use TRY_CAST for robust typing (Mongo strings vs Postgres numeric)
- Grain for player facts is per-team-season (player traded mid-season → multiple rows)
- QC queries live in trino/queries/qc.sql

