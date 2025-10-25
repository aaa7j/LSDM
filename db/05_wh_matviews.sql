-- Optional: snapshot materializations for performance / Spark export

DROP MATERIALIZED VIEW IF EXISTS wh.global_player_season_mv;
CREATE MATERIALIZED VIEW wh.global_player_season_mv AS
SELECT * FROM gav.global_player_season;

DROP MATERIALIZED VIEW IF EXISTS wh.global_team_season_mv;
CREATE MATERIALIZED VIEW wh.global_team_season_mv AS
SELECT * FROM gav.global_team_season;

