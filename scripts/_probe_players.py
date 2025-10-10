import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master('local[1]').appName('probe').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
try:
    df = spark.read.parquet('warehouse/global_player')
    print('COLUMNS:', df.columns)
    cols = set(df.columns)
    if 'full_name' in cols:
        sample = (df.where(F.lower(F.col('full_name')).contains('jordan'))
                    .select('player_id','full_name','first_name','last_name')
                    .limit(20).collect())
        for r in sample:
            print('ROW:', [r[c] for c in ['player_id','full_name','first_name','last_name'] if c in cols])
    else:
        print('No full_name column present')
finally:
    spark.stop()
