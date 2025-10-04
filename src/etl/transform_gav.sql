-- src/etl/transform_gav.sql
-- Tutte le SELECT qui sotto presuppongono le viste temp standardizzate
-- create in read_sources.py. Non usare COALESCE su colonne che non esistono.

-- ========== GLOBAL_PLAYER ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_PLAYER AS
SELECT
  p.PLAYER_ID                                        AS player_id,
  COALESCE(c.FIRST_NAME, element_at(SPLIT(p.PLAYER_NAME, ' '), 1))        AS first_name,
  COALESCE(c.LAST_NAME,  element_at(SPLIT(p.PLAYER_NAME, ' '), -1))       AS last_name,
  COALESCE(c.DISPLAY_FIRST_LAST, p.PLAYER_NAME)               AS full_name,
  c.POSITION                                                AS position,
  c.HEIGHT                                                  AS height,
  c.WEIGHT                                                  AS weight,
  c.BIRTHDATE                                               AS birth_date,
  c.COUNTRY                                                 AS nationality,
  c.COLLEGE                                                 AS college,
  c.SEASON_EXP                                              AS experience,
  p.TEAM_ID                                                 AS team_id,       -- può essere NULL
  c.JERSEY                                                  AS jersey_number
FROM player p
LEFT JOIN common_player_info c
  ON p.PLAYER_ID = c.PERSON_ID
;

-- ========== GLOBAL_TEAM ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_TEAM AS
SELECT
  t.TEAM_ID                                         AS team_id,
  COALESCE(i.TEAM_NAME, t.NICKNAME)                 AS team_name,
  COALESCE(i.TEAM_CITY, t.CITY)                     AS city,
  CAST(i.TEAM_STATE AS STRING)                      AS state,
  CAST(i.ARENA AS STRING)                           AS arena,
  i.YEARFOUNDED                                     AS founded_year,
  CAST(NULL AS INT)                                 AS championships_won,
  CAST(i.HEADCOACH AS STRING)                       AS coach,
  CAST(NULL AS STRING)                              AS team_colors,
  CAST(i.CONFERENCE AS STRING)                      AS conference,
  CAST(i.DIVISION AS STRING)                        AS division
FROM team t
LEFT JOIN team_info_common i
  ON t.TEAM_ID = i.TEAM_ID
;


-- ========== GLOBAL_GAME ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_GAME AS
WITH game_enriched AS (
  SELECT
    g.GAME_ID             AS game_id,
    g.GAME_DATE_EST       AS game_date,
    g.HOME_TEAM_ID        AS home_team_id,
    g.VISITOR_TEAM_ID     AS away_team_id,
    gi.ARENA_NAME         AS location,
    gi.ATTENDANCE         AS attendance,
    s.GAME_STATUS_TEXT    AS game_duration,
    s.PTS_home            AS final_score_home,
    s.PTS_away            AS final_score_away,
    s.PERIODS             AS period_count
  FROM game g
  LEFT JOIN game_summary s
    ON g.GAME_ID = s.GAME_ID
  LEFT JOIN game_info gi
    ON g.GAME_ID = gi.GAME_ID
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY game_id) AS rn
  FROM game_enriched
)
SELECT
  game_id,
  game_date,
  home_team_id,
  away_team_id,
  location,
  attendance,
  game_duration,
  final_score_home,
  final_score_away,
  period_count
FROM dedup
WHERE rn = 1
;

-- ========== GLOBAL_PLAY_BY_PLAY ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_PLAY_BY_PLAY AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY GAME_ID, EVENTNUM
      ORDER BY
        COALESCE(PERIOD, -1) DESC,
        COALESCE(PCTIMESTRING, ''),
        COALESCE(WCTIMESTRING, '')
    ) AS rn
  FROM play_by_play
)
SELECT
  GAME_ID                      AS game_id,
  EVENTNUM                     AS eventnum,
  EVENTMSGTYPE                 AS eventmsgtype,
  EVENTMSGACTIONTYPE           AS eventmsgactiontype,
  PERIOD                       AS period,
  WCTIMESTRING                 AS wctimestring,
  PCTIMESTRING                 AS pctimestring,
  HOMEDESCRIPTION              AS homedescription,
  NEUTRALDESCRIPTION           AS neutraldescription,
  VISITORDESCRIPTION           AS visitordescription,
  SCORE                        AS score,
  SCOREMARGIN                  AS scoremargin,
  PERSON1TYPE                  AS person1type,
  PLAYER1_ID                   AS player1_id,
  PLAYER1_NAME                 AS player1_name,
  PLAYER1_TEAM_ID              AS player1_team_id,
  PERSON2TYPE                  AS person2type,
  PLAYER2_ID                   AS player2_id,
  PLAYER2_NAME                 AS player2_name,
  PLAYER2_TEAM_ID              AS player2_team_id,
  PERSON3TYPE                  AS person3type,
  PLAYER3_ID                   AS player3_id,
  PLAYER3_NAME                 AS player3_name,
  PLAYER3_TEAM_ID              AS player3_team_id,
  VIDEO_AVAILABLE_FLAG         AS video_available_flag
FROM ranked
WHERE rn = 1
;

-- ========== GLOBAL_LINE_SCORE ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_LINE_SCORE AS
SELECT
  GAME_ID   AS game_id,
  TEAM_ID   AS team_id,
  PERIOD    AS period,
  MAX(PTS)  AS points
FROM line_score
GROUP BY GAME_ID, TEAM_ID, PERIOD
;

-- ========== GLOBAL_OFFICIAL ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_OFFICIAL AS
SELECT
  OFFICIAL_ID       AS official_id,
  MAX(OFFICIAL_NAME) AS official_name,
  MAX(OFFICIAL_TYPE) AS role,
  MAX(EXPERIENCE)    AS experience
FROM officials
WHERE OFFICIAL_ID IS NOT NULL
GROUP BY OFFICIAL_ID
;

-- ========== GLOBAL_GAME_OFFICIAL ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_GAME_OFFICIAL AS
SELECT DISTINCT
  GAME_ID       AS game_id,
  OFFICIAL_ID   AS official_id,
  OFFICIAL_NAME AS official_name,
  OFFICIAL_TYPE AS role
FROM officials
WHERE OFFICIAL_ID IS NOT NULL
;

-- ========== GLOBAL_OTHER_STATS ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_OTHER_STATS AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY GAME_ID, TEAM_ID
      ORDER BY GAME_ID
    ) AS rn
  FROM other_stats
)
SELECT
  GAME_ID   AS game_id,
  TEAM_ID   AS team_id,
  PTS       AS points,
  REB       AS rebounds,
  AST       AS assists,
  STL       AS steals,
  BLK       AS blocks,
  TOV       AS turnovers,
  PF        AS fouls
FROM ranked
WHERE rn = 1
;


-- ========== GLOBAL_DRAFT_COMBINE ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_DRAFT_COMBINE AS
SELECT
  PLAYER_ID           AS player_id,
  YEAR                AS year,
  HEIGHT              AS height_measurement,
  WEIGHT              AS weight_measurement,
  WINGSPAN            AS wingspan,
  VERTICAL_JUMP       AS vertical_jump,
  BENCH_REPS          AS bench_press_reps,
  SHUTTLE_RUN         AS shuttle_run_time
FROM draft_combine_stats
;

-- ========== GLOBAL_DRAFT_HISTORY ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_DRAFT_HISTORY AS
SELECT
  PLAYER_ID     AS player_id,
  TEAM_ID       AS team_id,
  DRAFT_YEAR    AS draft_year,
  DRAFT_ROUND   AS draft_round,
  DRAFT_NUMBER  AS draft_pick,
  OVERALL_PICK  AS overall_pick
FROM draft_history
;
