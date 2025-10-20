import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

# ---------- Spark Session (Windows/OneDrive safe) ----------
def get_spark(app_name="BasketballAnalytics"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", "C:/spark-tmp")
        .config("hadoop.tmp.dir", "C:/spark-tmp")  
        .config("spark.sql.warehouse.dir", "C:/spark-tmp/wh")
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=C:/spark-tmp -Dfile.encoding=UTF-8")
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=C:/spark-tmp -Dfile.encoding=UTF-8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "256")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setSystemProperty("hadoop.home.dir", "C:/spark-tmp")
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    hconf.set("fs.file.impl.disable.cache", "true")
    return spark


def _read_csv(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", "\"")
        .csv(path)
    )

def _exists(base, name):
    return os.path.exists(os.path.join(base, f"{name}.csv"))

def _cols(df):
    return {c.lower() for c in df.columns}

def _safe_int(column: Column) -> Column:
    stringified = F.trim(column.cast("string"))
    cleaned = F.regexp_replace(stringified, ',', '')
    cleaned = F.regexp_replace(cleaned, r"\.0+$", "")
    return (
        F.when(stringified.isNull() | (stringified == ''), F.lit(None))
        .otherwise(cleaned.cast("double"))
        .cast("int")
    )


def register_sources(spark, base="data"):
    """
    Legge i CSV in `base` e registra temp views **standardizzate**
    con i nomi/colonne che i GAV si aspettano.
    Ritorna l'elenco delle view registrate.
    """
    registered = []

    # -------------------- PLAYER (standardized) --------------------
    # Espone: PLAYER_ID (int), PLAYER_NAME (string), TEAM_ID (int, nullable)
    if _exists(base, "player"):
        df = _read_csv(spark, f"{base}/player.csv")
        c = _cols(df)
        exprs = []

        # PLAYER_ID
        if "player_id" in c:
            exprs.append(F.col("player_id").cast("int").alias("PLAYER_ID"))
        elif "id" in c:
            exprs.append(F.col("id").cast("int").alias("PLAYER_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("PLAYER_ID"))

        # PLAYER_NAME
        if "player_name" in c:
            exprs.append(F.col("player_name").alias("PLAYER_NAME"))
        elif "full_name" in c:
            exprs.append(F.col("full_name").alias("PLAYER_NAME"))
        elif {"first_name", "last_name"} <= c:
            exprs.append(F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("PLAYER_NAME"))
        else:
            exprs.append(F.lit(None).alias("PLAYER_NAME"))

        # TEAM_ID (nullable)
        if "team_id" in c:
            exprs.append(F.col("team_id").cast("int").alias("TEAM_ID"))
        elif "teamid" in c:
            exprs.append(F.col("teamid").cast("int").alias("TEAM_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("TEAM_ID"))

        df.select(*exprs).createOrReplaceTempView("player")
        print("Registered view 'player' (standardized)")
        registered.append("player")

    # --------------- COMMON_PLAYER_INFO (standardized) ---------------
    # Espone: PERSON_ID, FIRST_NAME, LAST_NAME, DISPLAY_FIRST_LAST, BIRTHDATE,
    # COUNTRY, COLLEGE, HEIGHT, WEIGHT, SEASON_EXP, JERSEY, POSITION
    if _exists(base, "common_player_info"):
        df = _read_csv(spark, f"{base}/common_player_info.csv")
        c = _cols(df)
        exprs = []

        # PERSON_ID
        if "person_id" in c:
            exprs.append(F.col("person_id").cast("int").alias("PERSON_ID"))
        elif "player_id" in c:
            exprs.append(F.col("player_id").cast("int").alias("PERSON_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("PERSON_ID"))

        # FIRST/LAST/DISPLAY
        exprs.append((F.col("first_name") if "first_name" in c else F.lit(None)).alias("FIRST_NAME"))
        exprs.append((F.col("last_name")  if "last_name"  in c else F.lit(None)).alias("LAST_NAME"))
        if "display_first_last" in c:
            exprs.append(F.col("display_first_last").alias("DISPLAY_FIRST_LAST"))
        elif {"first_name","last_name"} <= c:
            exprs.append(F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("DISPLAY_FIRST_LAST"))
        else:
            exprs.append(F.lit(None).alias("DISPLAY_FIRST_LAST"))

        # BIRTHDATE / COUNTRY / COLLEGE (mappa school -> COLLEGE)
        exprs.append((F.col("birthdate") if "birthdate" in c else F.lit(None)).alias("BIRTHDATE"))
        exprs.append((F.col("country")   if "country"   in c else F.lit(None)).alias("COUNTRY"))
        if "college" in c:
            exprs.append(F.col("college").alias("COLLEGE"))
        elif "school" in c:
            exprs.append(F.col("school").alias("COLLEGE"))
        else:
            exprs.append(F.lit(None).alias("COLLEGE"))

        # HEIGHT / WEIGHT / POSITION / SEASON_EXP / JERSEY
        exprs.append((F.col("height")     if "height"     in c else F.lit(None)).alias("HEIGHT"))
        exprs.append((F.col("weight")     if "weight"     in c else F.lit(None)).alias("WEIGHT"))
        exprs.append((F.col("position")   if "position"   in c else F.lit(None)).alias("POSITION"))
        exprs.append((F.col("season_exp") if "season_exp" in c else F.lit(None)).alias("SEASON_EXP"))
        exprs.append((F.col("jersey")     if "jersey"     in c else F.lit(None)).alias("JERSEY"))

        df.select(*exprs).createOrReplaceTempView("common_player_info")
        print("Registered view 'common_player_info' (standardized)")
        registered.append("common_player_info")

    # ----------------------- TEAM (standardized) -----------------------
    # Espone: TEAM_ID, CITY, NICKNAME, ABBREVIATION
    if _exists(base, "team"):
        df = _read_csv(spark, f"{base}/team.csv")
        c = _cols(df)
        exprs = []

        if "team_id" in c:
            exprs.append(F.col("team_id").cast("int").alias("TEAM_ID"))
        elif "id" in c:
            exprs.append(F.col("id").cast("int").alias("TEAM_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("TEAM_ID"))

        if "city" in c:
            exprs.append(F.col("city").alias("CITY"))
        elif "team_city" in c:
            exprs.append(F.col("team_city").alias("CITY"))
        else:
            exprs.append(F.lit(None).alias("CITY"))

        if "nickname" in c:
            exprs.append(F.col("nickname").alias("NICKNAME"))
        elif "team_name" in c:
            exprs.append(F.col("team_name").alias("NICKNAME"))
        else:
            exprs.append(F.lit(None).alias("NICKNAME"))

        if "abbreviation" in c:
            exprs.append(F.col("abbreviation").alias("ABBREVIATION"))
        elif "team_abbreviation" in c:
            exprs.append(F.col("team_abbreviation").alias("ABBREVIATION"))
        else:
            exprs.append(F.lit(None).alias("ABBREVIATION"))

        df.select(*exprs).createOrReplaceTempView("team")
        print("Registered view 'team' (standardized)")
        registered.append("team")

    # ---------------- TEAM_INFO_COMMON (standardized) -----------------
    # Espone: TEAM_ID, TEAM_NAME, TEAM_CITY, TEAM_ABBREVIATION,
    # CONFERENCE, DIVISION, TEAM_STATE, ARENA, YEARFOUNDED, HEADCOACH
    if _exists(base, "team_info_common"):
        df = _read_csv(spark, f"{base}/team_info_common.csv")
        c = _cols(df)
        exprs = []

        if "team_id" in c:
            exprs.append(F.col("team_id").cast("int").alias("TEAM_ID"))
        elif "id" in c:
            exprs.append(F.col("id").cast("int").alias("TEAM_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("TEAM_ID"))

        exprs.append(((F.col("team_name")         if "team_name"         in c else F.lit(None)).cast("string")).alias("TEAM_NAME"))
        exprs.append(((F.col("team_city")         if "team_city"         in c else F.lit(None)).cast("string")).alias("TEAM_CITY"))
        exprs.append(((F.col("team_abbreviation") if "team_abbreviation" in c else F.lit(None)).cast("string")).alias("TEAM_ABBREVIATION"))
        exprs.append(((F.col("team_conference")   if "team_conference"   in c else F.lit(None)).cast("string")).alias("CONFERENCE"))
        exprs.append(((F.col("team_division")     if "team_division"     in c else F.lit(None)).cast("string")).alias("DIVISION"))

        exprs.append(((F.col("team_state")  if "team_state"  in c else F.lit(None)).cast("string")).alias("TEAM_STATE"))
        exprs.append(((F.col("arena")       if "arena"       in c else F.lit(None)).cast("string")).alias("ARENA"))
        exprs.append(((F.col("yearfounded") if "yearfounded" in c else F.lit(None)).cast("int")).alias("YEARFOUNDED"))
        exprs.append(((F.col("headcoach")   if "headcoach"   in c else F.lit(None)).cast("string")).alias("HEADCOACH"))

        df.select(*exprs).createOrReplaceTempView("team_info_common")
        print("Registered view 'team_info_common' (standardized)")
        registered.append("team_info_common")

    # ------------------------ GAME (standardized) ------------------------
    # Espone: GAME_ID, GAME_DATE_EST, HOME_TEAM_ID, VISITOR_TEAM_ID
    if _exists(base, "game"):
        df = _read_csv(spark, f"{base}/game.csv")
        c = _cols(df)
        exprs = []
        exprs.append((F.col("game_id") if "game_id" in c else F.lit(None)).cast("long").alias("GAME_ID"))
        if "game_date_est" in c:
            exprs.append(F.col("game_date_est").alias("GAME_DATE_EST"))
        elif "game_date" in c:
            exprs.append(F.col("game_date").alias("GAME_DATE_EST"))
        else:
            exprs.append(F.lit(None).alias("GAME_DATE_EST"))
        if "home_team_id" in c:
            exprs.append(F.col("home_team_id").cast("int").alias("HOME_TEAM_ID"))
        elif "team_id_home" in c:
            exprs.append(F.col("team_id_home").cast("int").alias("HOME_TEAM_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("HOME_TEAM_ID"))
        if "visitor_team_id" in c:
            exprs.append(F.col("visitor_team_id").cast("int").alias("VISITOR_TEAM_ID"))
        elif "team_id_away" in c:
            exprs.append(F.col("team_id_away").cast("int").alias("VISITOR_TEAM_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("VISITOR_TEAM_ID"))

        df.select(*exprs).createOrReplaceTempView("game")
        print("Registered view 'game' (standardized)")
        registered.append("game")

    # ---------------------- TEAM_DETAILS (standardized) ----------------------
    # Espone: TEAM_ID, ARENA, HEADCOACH (opzionali)
    if _exists(base, "team_details"):
        df = _read_csv(spark, f"{base}/team_details.csv")
        c = _cols(df)
        exprs = []
        if "team_id" in c:
            exprs.append(F.col("team_id").cast("int").alias("TEAM_ID"))
        elif "id" in c:
            exprs.append(F.col("id").cast("int").alias("TEAM_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("TEAM_ID"))
        exprs.append(((F.col("arena") if "arena" in c else F.lit(None)).cast("string")).alias("ARENA"))
        exprs.append(((F.col("headcoach") if "headcoach" in c else F.lit(None)).cast("string")).alias("HEADCOACH"))

        df.select(*exprs).createOrReplaceTempView("team_details")
        print("Registered view 'team_details' (standardized)")
        registered.append("team_details")

    # -------------------- GAME_SUMMARY (standardized) --------------------
    # Espone: GAME_ID, GAME_STATUS_TEXT, PTS_home, PTS_away, PERIODS
    if _exists(base, "game_summary"):
        df = _read_csv(spark, f"{base}/game_summary.csv")
        c = _cols(df)
        exprs = []
        exprs.append((F.col("game_id") if "game_id" in c else F.lit(None)).cast("long").alias("GAME_ID"))
        exprs.append((F.col("game_status_text") if "game_status_text" in c else F.lit(None)).alias("GAME_STATUS_TEXT"))
        exprs.append(_safe_int(F.col("pts_home") if "pts_home" in c else F.lit(None)).alias("PTS_home"))
        exprs.append(_safe_int(F.col("pts_away") if "pts_away" in c else F.lit(None)).alias("PTS_away"))
        exprs.append(_safe_int(F.col("periods")  if "periods"  in c else F.lit(None)).alias("PERIODS"))
        df.select(*exprs).createOrReplaceTempView("game_summary")
        print("Registered view 'game_summary' (standardized)")
        registered.append("game_summary")

    # ---------------------- GAME_INFO (standardized) ----------------------
    # Espone: GAME_ID, ARENA_NAME, ATTENDANCE
    if _exists(base, "game_info"):
        df = _read_csv(spark, f"{base}/game_info.csv")
        c = _cols(df)
        exprs = []
        exprs.append((F.col("game_id") if "game_id" in c else F.lit(None)).cast("long").alias("GAME_ID"))
        exprs.append((F.col("arena_name") if "arena_name" in c else F.lit(None)).alias("ARENA_NAME"))
        exprs.append(_safe_int(F.col("attendance") if "attendance" in c else F.lit(None)).alias("ATTENDANCE"))
        df.select(*exprs).createOrReplaceTempView("game_info")
        print("Registered view 'game_info' (standardized)")
        registered.append("game_info")

    # ---------------------- LINE_SCORE (standardized) ----------------------
    # Input "wide" -> output "long": GAME_ID, TEAM_ID, PERIOD, PTS
    if _exists(base, "line_score"):
        df = _read_csv(spark, f"{base}/line_score.csv")
        cset = _cols(df)
        def has(name): return name in cset

        base_cols = [ (F.col("game_id") if has("game_id") else F.lit(None)).cast("long").alias("GAME_ID") ]

        home_team_col = (
            F.col("team_id_home").cast("int") if has("team_id_home")
            else F.col("home_team_id").cast("int") if has("home_team_id")
            else F.lit(None).cast("int")
        )
        away_team_col = (
            F.col("team_id_away").cast("int") if has("team_id_away")
            else F.col("visitor_team_id").cast("int") if has("visitor_team_id")
            else F.lit(None).cast("int")
        )

        home_periods, away_periods = [], []
        for i in range(1, 5):
            hn, an = f"pts_qtr{i}_home", f"pts_qtr{i}_away"
            if has(hn): home_periods.append((hn, i))
            if has(an): away_periods.append((an, i))
        for i in range(1, 11):
            hn, an = f"pts_ot{i}_home", f"pts_ot{i}_away"
            per = 4 + i
            if has(hn): home_periods.append((hn, per))
            if has(an): away_periods.append((an, per))

        def build_rows(periods, team_col):
            rows = None
            for col_name, per in periods:
                sel = [*base_cols, team_col.alias("TEAM_ID"), F.lit(per).cast("int").alias("PERIOD"), _safe_int(F.col(col_name)).alias("PTS")]
                part = df.select(*sel)
                rows = part if rows is None else rows.unionByName(part)
            return rows

        home_rows = build_rows(home_periods, home_team_col)
        away_rows = build_rows(away_periods, away_team_col)

        if home_rows is not None and away_rows is not None:
            long_df = home_rows.unionByName(away_rows)
        elif home_rows is not None:
            long_df = home_rows
        elif away_rows is not None:
            long_df = away_rows
        else:
            # fallback: solo totali
            parts = []
            if has("pts_home"):
                parts.append(df.select(*base_cols, home_team_col.alias("TEAM_ID"),
                                       F.lit(None).cast("int").alias("PERIOD"),
                                       _safe_int(F.col("pts_home")).alias("PTS")))
            if has("pts_away"):
                parts.append(df.select(*base_cols, away_team_col.alias("TEAM_ID"),
                                       F.lit(None).cast("int").alias("PERIOD"),
                                       _safe_int(F.col("pts_away")).alias("PTS")))
            long_df = parts[0] if parts else df.select(*base_cols,
                                                       F.lit(None).cast("int").alias("TEAM_ID"),
                                                       F.lit(None).cast("int").alias("PERIOD"),
                                                       F.lit(None).cast("int").alias("PTS"))

        long_df = long_df.where(F.col("TEAM_ID").isNotNull() & F.col("PTS").isNotNull())
        long_df.createOrReplaceTempView("line_score")
        print("Registered view 'line_score' (standardized)")
        registered.append("line_score")

    # ---------------------- OTHER_STATS (standardized) ----------------------
    # Input "wide" -> output long: GAME_ID, TEAM_ID, PTS, REB, AST, STL, BLK, TOV, PF
    if _exists(base, "other_stats"):
        df = _read_csv(spark, f"{base}/other_stats.csv")
        cset = _cols(df)
        def has(n): return n in cset

        game_id_col = (F.col("game_id") if has("game_id") else F.lit(None)).cast("long").alias("GAME_ID")
        team_home = _safe_int(F.col("team_id_home") if has("team_id_home") else F.col("home_team_id") if has("home_team_id") else F.lit(None))
        team_away = _safe_int(F.col("team_id_away") if has("team_id_away") else F.col("visitor_team_id") if has("visitor_team_id") else F.lit(None))

        def pick(hname, aname):
            h = F.col(hname) if has(hname) else None
            a = F.col(aname) if has(aname) else None
            return (_safe_int(h) if h is not None else None,
                    _safe_int(a) if a is not None else None)

        pts_h, pts_a = (None, None)
        # REB: prefer team_rebounds
        reb_h, reb_a = pick("team_rebounds_home", "team_rebounds_away")
        ast_h, ast_a = (None, None)
        stl_h, stl_a = (None, None)
        blk_h, blk_a = (None, None)
        # TOV: prefer total_turnovers
        tov_h, tov_a = pick("total_turnovers_home", "total_turnovers_away")
        # PF: se non presente -> NULL
        pf_h, pf_a = pick("pf_home", "pf_away")

        def row(team_expr, pts, reb, ast, stl, blk, tov, pf):
            return [
                game_id_col,
                team_expr.alias("TEAM_ID"),
                (pts if pts is not None else F.lit(None).cast("int")).alias("PTS"),
                (reb if reb is not None else F.lit(None).cast("int")).alias("REB"),
                (ast if ast is not None else F.lit(None).cast("int")).alias("AST"),
                (stl if stl is not None else F.lit(None).cast("int")).alias("STL"),
                (blk if blk is not None else F.lit(None).cast("int")).alias("BLK"),
                (tov if tov is not None else F.lit(None).cast("int")).alias("TOV"),
                (pf  if pf  is not None else F.lit(None).cast("int")).alias("PF"),
            ]

        home_df = df.select(*row(team_home, pts_h, reb_h, ast_h, stl_h, blk_h, tov_h, pf_h))
        away_df = df.select(*row(team_away, pts_a, reb_a, ast_a, stl_a, blk_a, tov_a, pf_a))
        long_df = home_df.unionByName(away_df).where(F.col("TEAM_ID").isNotNull())
        long_df.createOrReplaceTempView("other_stats")
        print("Registered view 'other_stats' (standardized)")
        registered.append("other_stats")

    # ---------------------- OFFICIALS (standardized) ----------------------
    # Espone: GAME_ID, OFFICIAL_ID, OFFICIAL_NAME, OFFICIAL_TYPE (role), EXPERIENCE (nullable)
    if _exists(base, "officials"):
        df = _read_csv(spark, f"{base}/officials.csv")
        c = _cols(df)
        exprs = []
        exprs.append((F.col("game_id") if "game_id" in c else F.lit(None)).cast("long").alias("GAME_ID"))

        # OFFICIAL_ID
        if "official_id" in c:
            exprs.append(F.col("official_id").cast("int").alias("OFFICIAL_ID"))
        elif "person_id" in c:
            exprs.append(F.col("person_id").cast("int").alias("OFFICIAL_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("OFFICIAL_ID"))

        # OFFICIAL_NAME
        if "official_name" in c:
            exprs.append(F.col("official_name").alias("OFFICIAL_NAME"))
        elif {"first_name","last_name"} <= c:
            exprs.append(F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("OFFICIAL_NAME"))
        else:
            exprs.append(F.lit(None).alias("OFFICIAL_NAME"))

        # ROLE / TYPE
        if "role" in c:
            exprs.append(F.col("role").alias("OFFICIAL_TYPE"))
        elif "official_type" in c:
            exprs.append(F.col("official_type").alias("OFFICIAL_TYPE"))
        else:
            exprs.append(F.lit(None).alias("OFFICIAL_TYPE"))

        # EXPERIENCE
        if "experience" in c:
            exprs.append(F.col("experience").cast("int").alias("EXPERIENCE"))
        else:
            exprs.append(F.lit(None).cast("int").alias("EXPERIENCE"))

        df.select(*exprs).createOrReplaceTempView("officials")
        print("Registered view 'officials' (standardized)")
        registered.append("officials")

    # ----------------- DRAFT_COMBINE_STATS (standardized) -----------------
    # Espone: PLAYER_ID, HEIGHT, WEIGHT, WINGSPAN, VERTICAL_JUMP, BENCH_REPS, SHUTTLE_RUN
    if _exists(base, "draft_combine_stats"):
        df = _read_csv(spark, f"{base}/draft_combine_stats.csv")
        c = _cols(df)
        exprs = []
        if "player_id" in c:
            exprs.append(F.col("player_id").cast("int").alias("PLAYER_ID"))
        elif "person_id" in c:
            exprs.append(F.col("person_id").cast("int").alias("PLAYER_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("PLAYER_ID"))

        exprs.append((F.col("height")           if "height"           in c else F.lit(None)).cast("double").alias("HEIGHT"))
        exprs.append((F.col("weight")           if "weight"           in c else F.lit(None)).cast("double").alias("WEIGHT"))
        exprs.append((F.col("wingspan")         if "wingspan"         in c else F.lit(None)).cast("double").alias("WINGSPAN"))
        exprs.append((F.col("vertical_jump")    if "vertical_jump"    in c else F.lit(None)).cast("double").alias("VERTICAL_JUMP"))
        exprs.append((F.col("bench_press_reps") if "bench_press_reps" in c else F.lit(None)).cast("int").alias("BENCH_REPS"))
        exprs.append((F.col("shuttle_run_time") if "shuttle_run_time" in c else F.lit(None)).cast("double").alias("SHUTTLE_RUN"))
        if "season" in c:
            exprs.append(F.col("season").cast("int").alias("YEAR"))
        else:
            exprs.append(F.lit(None).cast("int").alias("YEAR"))

        df.select(*exprs).createOrReplaceTempView("draft_combine_stats")
        print("Registered view 'draft_combine_stats' (standardized)")
        registered.append("draft_combine_stats")

    # --------------------- DRAFT_HISTORY (standardized) -------------------
    # Espone: PLAYER_ID, TEAM_ID, DRAFT_YEAR, DRAFT_ROUND, DRAFT_NUMBER, OVERALL_PICK
    if _exists(base, "draft_history"):
        df = _read_csv(spark, f"{base}/draft_history.csv")
        c = _cols(df)
        exprs = []
        if "player_id" in c:
            exprs.append(F.col("player_id").cast("int").alias("PLAYER_ID"))
        elif "person_id" in c:
            exprs.append(F.col("person_id").cast("int").alias("PLAYER_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("PLAYER_ID"))

        if "team_id" in c:
            exprs.append(F.col("team_id").cast("int").alias("TEAM_ID"))
        elif "org_team_id" in c:
            exprs.append(F.col("org_team_id").cast("int").alias("TEAM_ID"))
        else:
            exprs.append(F.lit(None).cast("int").alias("TEAM_ID"))

        exprs.append((F.col("season")       if "season"       in c else F.col("draft_year") if "draft_year" in c else F.lit(None)).cast("int").alias("DRAFT_YEAR"))
        exprs.append((F.col("round_number") if "round_number" in c else F.col("draft_round") if "draft_round" in c else F.lit(None)).cast("int").alias("DRAFT_ROUND"))
        exprs.append((F.col("round_pick")   if "round_pick"   in c else F.col("draft_pick")  if "draft_pick"  in c else F.lit(None)).cast("int").alias("DRAFT_NUMBER"))
        exprs.append((F.col("overall_pick") if "overall_pick" in c else F.lit(None)).cast("int").alias("OVERALL_PICK"))

        df.select(*exprs).createOrReplaceTempView("draft_history")
        print("Registered view 'draft_history' (standardized)")
        registered.append("draft_history")

    # --------------------- PLAY_BY_PLAY (standardized) -------------------
    # Espone i campi usati nel GAV minimo:
    # GAME_ID, EVENTNUM, EVENTMSGTYPE, EVENTMSGACTIONTYPE, PERIOD,
    # WCTIMESTRING, PCTIMESTRING,
    # HOMEDESCRIPTION, NEUTRALDESCRIPTION, VISITORDESCRIPTION,
    # SCORE, SCOREMARGIN,
    # PERSON1TYPE, PLAYER1_ID, PLAYER1_NAME, PLAYER1_TEAM_ID,
    # PERSON2TYPE, PLAYER2_ID, PLAYER2_NAME, PLAYER2_TEAM_ID,
    # PERSON3TYPE, PLAYER3_ID, PLAYER3_NAME, PLAYER3_TEAM_ID,
    # VIDEO_AVAILABLE_FLAG
    if _exists(base, "play_by_play"):
        df = _read_csv(spark, f"{base}/play_by_play.csv")
        c = _cols(df)

        def safe(name, typ="string"):
            col = F.col(name)
            if name.lower() in c:
                if typ == "int":
                    return _safe_int(col).alias(name.upper())
                if typ == "long":
                    return _safe_int(col).cast("long").alias(name.upper())
                return (col if typ == "string" else col.cast(typ)).alias(name.upper())
            if typ == "int":
                return _safe_int(F.lit(None)).alias(name.upper())
            if typ == "long":
                return _safe_int(F.lit(None)).cast("long").alias(name.upper())
            return F.lit(None).cast(typ).alias(name.upper())

        cols = [
            safe("game_id", "long"),
            safe("eventnum", "int"),
            safe("eventmsgtype", "int"),
            safe("eventmsgactiontype", "int"),
            safe("period", "int"),
            safe("wctimestring"),
            safe("pctimestring"),
            safe("homedescription"),
            safe("neutraldescription"),
            safe("visitordescription"),
            safe("score"),
            safe("scoremargin"),
            safe("person1type", "int"),
            safe("player1_id", "int"),
            safe("player1_name"),
            safe("player1_team_id", "int"),
            safe("person2type", "int"),
            safe("player2_id", "int"),
            safe("player2_name"),
            safe("player2_team_id", "int"),
            safe("person3type", "int"),
            safe("player3_id", "int"),
            safe("player3_name"),
            safe("player3_team_id", "int"),
            safe("video_available_flag", "int"),
        ]
        df.select(*cols).createOrReplaceTempView("play_by_play")
        print("Registered view 'play_by_play' (standardized)")
        registered.append("play_by_play")

    return registered
