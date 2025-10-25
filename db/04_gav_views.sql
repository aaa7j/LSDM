-- GAV integration views with provenance columns

DROP VIEW IF EXISTS gav.global_player_season;
CREATE VIEW gav.global_player_season AS
WITH base AS (
  SELECT
    COALESCE(pt.player_id, ppg.player_id) AS player_id,
    COALESCE(pt.player_name, ppg.player_name) AS player_name,
    COALESCE(pt.team_abbr, ppg.team_abbr) AS team_abbr,
    pt.season::int AS season,
    COALESCE(pb.position, pt.position, ppg.position) AS position,
    pb.height,
    pb.weight,
    CASE WHEN pb.player_id IS NOT NULL THEN 'player_bio' END AS src_height,
    CASE WHEN pb.player_id IS NOT NULL THEN 'player_bio' END AS src_weight,
    CASE 
      WHEN pb.position IS NOT NULL THEN 'player_bio'
      WHEN pt.position IS NOT NULL THEN 'player_totals'
      WHEN ppg.position IS NOT NULL THEN 'player_per_game'
    END AS src_position,
    pt.g, pt.gs,
    pt.pts, pt.ast, pt.trb,
    ppg.pts_per_game, ppg.ast_per_game, ppg.trb_per_game,
    adv.per, adv.ts_percent, adv.ws, adv.bpm, adv.vorp
  FROM std.player_totals pt
  FULL OUTER JOIN std.player_per_game ppg
    ON pt.player_id IS NOT NULL AND pt.player_id = ppg.player_id AND pt.season = ppg.season
  LEFT JOIN std.player_bio pb
    ON pb.player_id IS NOT NULL AND pb.player_id = COALESCE(pt.player_id, ppg.player_id)
  LEFT JOIN std.player_advanced adv
    ON adv.player_id = COALESCE(pt.player_id, ppg.player_id) AND adv.season = COALESCE(pt.season, ppg.season)
)
SELECT * FROM base;

DROP VIEW IF EXISTS gav.global_team_season;
CREATE VIEW gav.global_team_season AS
WITH base AS (
  SELECT
    ts.season::int AS season,
    COALESCE(ts.team_abbr, ta.team_abbr, td.team_abbr) AS team_abbr,
    COALESCE(ts.team_name, ta.team_name, td.team_name) AS team_name,
    COALESCE(ts.arena_name, td.arena_name) AS arena_name,
    td.city,
    td.coach,
    ts.attend,
    ts.attend_g,
    CASE WHEN ts.arena_name IS NOT NULL THEN 'team_summaries' ELSE 'team_details' END AS src_arena,
    CASE WHEN td.coach IS NOT NULL THEN 'team_details' END AS src_coach
  FROM std.team_summaries ts
  FULL OUTER JOIN std.team_abbrev ta
    ON ts.season = ta.season AND ts.team_abbr = ta.team_abbr
  LEFT JOIN std.team_details td
    ON COALESCE(ts.team_abbr, ta.team_abbr) = td.team_abbr
)
SELECT * FROM base;

