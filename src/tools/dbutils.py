def read_tidb(spark, config, sql, **kwargs):
    return (spark.read.format("jdbc")
    .options(url="jdbc:mysql://{0}:{1}/{2}".format(config['tidb']['host'], config['tidb']['port'], config['tidb']['db']),
             driver='com.mysql.cj.jdbc.Driver',
             user=config['tidb']['user'],
             password=config['tidb']['password'],
             fetchsize=10000)
    .option("query", sql)
    .load())
