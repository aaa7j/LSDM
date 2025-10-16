from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InspectTables") \
    .config("spark.sql.warehouse.dir", "warehouse") \
    .getOrCreate()

for name in ["team_info_common", "team_details", "common_player_info"]:
    try:
        df = spark.table(name)
        print(f"\n{name.upper()} ->", df.columns)
        df.show(1, truncate=False)
    except Exception as e:
        print(f"\n{name.upper()} not found:", e)
