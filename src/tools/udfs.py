import jieba
from pyspark.sql.types import (
    StringType,
    ArrayType,
    FloatType
)
from pyspark.sql.functions import udf
from scipy import spatial

def jieba_seg(x):
    return [w for w in jieba.cut(x) if len(w)>1]

jieba_seg_udf = udf(jieba_seg, ArrayType(StringType()))


@udf(returnType=FloatType())
def sim(x, y):
    return float(1 - spatial.distance.cosine(x, y))