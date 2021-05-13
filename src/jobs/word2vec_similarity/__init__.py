"""
PYTHONIOENCODING=utf8 spark-submit --master yarn --deploy-mode cluster --queue root.dw --packages mysql:mysql-connector-java:8.0.24 --py-files hdfs:/user/lxneng/pyspk-demo/dist/jobs.zip,hdfs:/user/lxneng/pyspk-demo/dist/libs.zip,hdfs:///user/lxneng/pyspk-demo/dist/config.yml hdfs:///user/lxneng/pyspk-demo/dist/main.py --job word2vec_similarity
"""
from tools.dbutils import read_tidb
from tools.udfs import (
    jieba_seg_udf,
    sim
)
from tools.utils import get_topN
from pyspark.ml.feature import Word2Vec
from pyspark.sql.functions import (
    col,
    collect_list
)


def run(spark, config, **kwargs):
    df = read_tidb(spark, config, "select id, title, mp_id from content.content")

    df = df.withColumn('words', jieba_seg_udf(df['title']))
    # df.show(10)

    model = Word2Vec(numPartitions=10, inputCol='words', outputCol='vecs', seed=42).fit(df)
    df_transformed = model.transform(df)
    df_cross = df_transformed.select(
        col('id').alias('id1'),
        col('vecs').alias('vecs1')).crossJoin(df_transformed.select(
            col('id').alias('id2'),
            col('vecs').alias('vecs2'))
    )
    
    df_cross = df_cross.withColumn('sim', sim(df_cross['vecs1'], df_cross['vecs2']))
    df_simi = df_cross.filter(col('sim')<1)
    
    df_simi_top10 = get_topN(df_simi, col('id1'), col('sim'), n=10)
    
    df_simi_top10.show()
    df_simi_top10.groupby("id1").agg(collect_list("id2").alias("sim_ids")).show(10)

    return 
