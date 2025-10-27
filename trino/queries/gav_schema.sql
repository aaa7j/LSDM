-- List all GAV views and their columns (name + type)
SELECT table_name, ordinal_position, column_name, data_type
FROM memory.information_schema.columns
WHERE table_schema = 'gav'
ORDER BY table_name, ordinal_position;

-- Per‑view DESCRIBE
DESCRIBE memory.gav.global_player_season;
DESCRIBE memory.gav.global_team_season;
DESCRIBE memory.gav.global_game;
DESCRIBE memory.gav.dim_player;
DESCRIBE memory.gav.dim_team_season;
DESCRIBE memory.gav.player_game_box;

-- Show CREATE text
SHOW CREATE VIEW memory.gav.global_player_season;
SHOW CREATE VIEW memory.gav.global_team_season;
SHOW CREATE VIEW memory.gav.global_game;
SHOW CREATE VIEW memory.gav.dim_player;
SHOW CREATE VIEW memory.gav.dim_team_season;
SHOW CREATE VIEW memory.gav.player_game_box;
