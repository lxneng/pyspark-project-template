from pyspark.sql import Window
import pyspark.sql.functions as f


def get_topN(df, group_by_columns, order_by_column, n=10):
    window = Window.partitionBy(group_by_columns).orderBy(order_by_column.desc())
    return df.select('*', f.rank().over(window).alias('rank')).filter(f.col('rank') <= n).drop("rank")