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
  CAST(COALESCE(i.ARENA, d.ARENA) AS STRING)        AS arena,
  i.YEARFOUNDED                                     AS founded_year,
  CAST(NULL AS INT)                                 AS championships_won,
  CAST(COALESCE(i.HEADCOACH, d.HEADCOACH) AS STRING) AS coach,
  CAST(NULL AS STRING)                              AS team_colors,
  CAST(i.CONFERENCE AS STRING)                      AS conference,
  CAST(i.DIVISION AS STRING)                        AS division
FROM team t
LEFT JOIN team_info_common i
  ON t.TEAM_ID = i.TEAM_ID
LEFT JOIN team_details d
  ON t.TEAM_ID = d.TEAM_ID
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
    ROW_NUMBER() OVER (
      PARTITION BY game_id
      ORDER BY
        CASE WHEN final_score_home IS NULL OR final_score_away IS NULL THEN 1 ELSE 0 END,
        CASE WHEN attendance IS NULL THEN 1 ELSE 0 END,
        game_id
    ) AS rn
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
),
base AS (
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
),
scored AS (
  SELECT
    b.*,
    REGEXP_REPLACE(COALESCE(b.score, ''), '\\s+', '') AS score_clean,
    SPLIT(REGEXP_REPLACE(COALESCE(b.score, ''), '\\s+', ''), '-') AS sa
  FROM base b
),
nums AS (
  SELECT
    n.*,
    CASE WHEN size(sa) = 2 THEN TRY_CAST(element_at(sa, 1) AS INT) END AS score_home,
    CASE WHEN size(sa) = 2 THEN TRY_CAST(element_at(sa, 2) AS INT) END AS score_away
  FROM scored n
),
lagged AS (
  SELECT
    l.*,
    COALESCE(
      LAST_VALUE(score_home, true) OVER (
        PARTITION BY game_id ORDER BY period, eventnum
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS prev_home,
    COALESCE(
      LAST_VALUE(score_away, true) OVER (
        PARTITION BY game_id ORDER BY period, eventnum
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS prev_away
  FROM nums l
),
calc AS (
  SELECT
    f.*,
    GREATEST(
      CASE WHEN f.score_home IS NOT NULL THEN f.score_home - f.prev_home ELSE 0 END,
      CASE WHEN f.score_away IS NOT NULL THEN f.score_away - f.prev_away ELSE 0 END
    ) AS points
  FROM lagged f
)
SELECT
  game_id,
  eventnum,
  eventmsgtype,
  eventmsgactiontype,
  period,
  wctimestring,
  pctimestring,
  homedescription,
  neutraldescription,
  visitordescription,
  score,
  scoremargin,
  person1type,
  player1_id,
  player1_name,
  player1_team_id,
  person2type,
  player2_id,
  player2_name,
  player2_team_id,
  person3type,
  player3_id,
  player3_name,
  player3_team_id,
  video_available_flag,
  score_home,
  score_away,
  prev_home,
  prev_away,
  points,
  CASE WHEN (CASE WHEN score_home IS NOT NULL THEN score_home - prev_home ELSE 0 END) >
            (CASE WHEN score_away IS NOT NULL THEN score_away - prev_away ELSE 0 END)
       THEN 'HOME'
       WHEN (CASE WHEN score_away IS NOT NULL THEN score_away - prev_away ELSE 0 END) >
            (CASE WHEN score_home IS NOT NULL THEN score_home - prev_home ELSE 0 END)
       THEN 'AWAY' END AS scoring_side,
  CASE WHEN points > 0 THEN 1 ELSE 0 END AS is_scoring,
  CASE WHEN points > 0 THEN player1_id ELSE NULL END AS scorer_id,
  CASE WHEN eventmsgtype = 1 AND points > 0 THEN player2_id ELSE NULL END AS assist_id,
  CASE WHEN eventmsgtype = 2 AND player3_id IS NOT NULL THEN player3_id ELSE NULL END AS blocker_id,
  COALESCE(player1_team_id, player2_team_id, player3_team_id) AS actor_team_id,
  CASE WHEN pctimestring IS NOT NULL AND INSTR(pctimestring, ':') > 0
         THEN TRY_CAST(element_at(SPLIT(pctimestring, ':'), 1) AS INT) * 60 +
              TRY_CAST(element_at(SPLIT(pctimestring, ':'), 2) AS INT)
         ELSE NULL
  END AS period_seconds_remaining,
  CASE WHEN eventmsgtype = 1 THEN 'FIELD_GOAL_MADE'
       WHEN eventmsgtype = 2 THEN 'FIELD_GOAL_MISSED'
       WHEN eventmsgtype = 3 THEN 'FREE_THROW'
       WHEN eventmsgtype = 4 THEN 'REBOUND'
       WHEN eventmsgtype = 5 THEN 'TURNOVER'
       WHEN eventmsgtype = 6 THEN 'FOUL'
       WHEN eventmsgtype = 7 THEN 'VIOLATION'
       WHEN eventmsgtype = 8 THEN 'SUBSTITUTION'
       WHEN eventmsgtype = 9 THEN 'TIMEOUT'
       WHEN eventmsgtype = 10 THEN 'JUMP_BALL'
       ELSE 'OTHER' END AS event_type_label,
  TRY_CAST(regexp_extract(COALESCE(homedescription, visitordescription, neutraldescription), '([0-9]{1,3})', 1) AS INT) AS shot_distance_ft,
  CASE WHEN LOWER(COALESCE(homedescription, visitordescription, neutraldescription)) LIKE '%3pt%'
       THEN 1 ELSE 0 END AS is_three_pt,
  CASE WHEN eventmsgtype = 4 AND LOWER(COALESCE(homedescription, visitordescription, neutraldescription)) LIKE '%off:%'
            AND regexp_extract(LOWER(COALESCE(homedescription, visitordescription, neutraldescription)), 'off:([0-9]+)', 1) <> '0'
       THEN 1 ELSE 0 END AS is_offensive_rebound,
  CASE WHEN eventmsgtype = 4 AND LOWER(COALESCE(homedescription, visitordescription, neutraldescription)) LIKE '%def:%' THEN 1 ELSE 0 END AS is_defensive_rebound
FROM calc
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
      ORDER BY (
        (CASE WHEN PTS IS NULL THEN 1 ELSE 0 END) +
        (CASE WHEN REB IS NULL THEN 1 ELSE 0 END) +
        (CASE WHEN AST IS NULL THEN 1 ELSE 0 END) +
        (CASE WHEN STL IS NULL THEN 1 ELSE 0 END) +
        (CASE WHEN BLK IS NULL THEN 1 ELSE 0 END) +
        (CASE WHEN TOV IS NULL THEN 1 ELSE 0 END) +
        (CASE WHEN PF  IS NULL THEN 1 ELSE 0 END)
      ), GAME_ID
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

-- ========== GLOBAL_SCORING_EVENTS ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_SCORING_EVENTS AS
SELECT
  game_id,
  eventnum,
  period,
  pctimestring,
  wctimestring,
  scorer_id    AS player_id,
  assist_id,
  points,
  scoring_side,
  actor_team_id AS team_id,
  score_home,
  score_away
FROM GLOBAL_PLAY_BY_PLAY
WHERE is_scoring = 1
;

-- ========== GLOBAL_PLAYER_GAME_OFFENSE ==========
CREATE OR REPLACE TEMP VIEW GLOBAL_PLAYER_GAME_OFFENSE AS
WITH shots AS (
  SELECT
    e.game_id,
    e.eventnum,
    e.player1_id       AS shooter_id,
    e.eventmsgtype     AS t,
    e.points           AS pts,
    e.is_three_pt      AS is3,
    COALESCE(e.player2_id, NULL) AS assist_id
  FROM GLOBAL_PLAY_BY_PLAY e
),
agg AS (
  SELECT
    CAST(shooter_id AS INT) AS player_id,
    game_id,
    SUM(CASE WHEN t = 1 AND pts IN (2,3) THEN 1 ELSE 0 END)              AS fgm,
    SUM(CASE WHEN t IN (1,2) THEN 1 ELSE 0 END)                          AS fga,
    SUM(CASE WHEN t = 1 AND is3 = 1 THEN 1 ELSE 0 END)                   AS fg3m,
    SUM(CASE WHEN t = 2 AND is3 = 1 THEN 1 ELSE 0 END)                   AS fg3a_miss,
    SUM(CASE WHEN t = 3 AND pts = 1 THEN 1 ELSE 0 END)                   AS ftm,
    SUM(CASE WHEN t = 3 THEN 1 ELSE 0 END)                               AS fta,
    SUM(CASE WHEN t = 1 AND pts IN (2,3) THEN pts ELSE 0 END) +
    SUM(CASE WHEN t = 3 THEN pts ELSE 0 END)                             AS points,
    SUM(CASE WHEN t = 1 AND pts IN (2,3) AND assist_id IS NOT NULL THEN 1 ELSE 0 END) AS ast
  FROM shots
  WHERE shooter_id IS NOT NULL
  GROUP BY shooter_id, game_id
)
SELECT
  game_id,
  player_id,
  points,
  fgm,
  fga,
  fg3m,
  /* Approximate 3PA as 3PM + missed 3PT (detected by text) */
  (fg3m + CASE WHEN fg3a_miss IS NULL THEN 0 ELSE fg3a_miss END) AS fg3a,
  ftm,
  fta,
  ast
FROM agg
;
