"""
PYTHONIOENCODING=utf8 spark-submit --master yarn --packages mysql:mysql-connector-java:8.0.24 --py-files hdfs:/user/lxneng/pyspk-demo/dist/jobs.zip,hdfs:/user/lxneng/pyspk-demo/dist/libs.zip,hdfs:///user/lxneng/pyspk-demo/dist/config.yml hdfs:///user/lxneng/pyspk-demo/dist/main.py --job mysqljdbc
"""
from tools.dbutils import read_tidb


def run(spark, config, **kwargs):
    df = read_tidb(spark, config, "select id, title, mp_id from content.content")
    df.show(10)

    df.groupby("mp_id").count().show()

    return 
