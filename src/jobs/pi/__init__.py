from random import random
from operator import add


NUMBER_OF_STEPS_FACTOR = 1000000


def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0


def run(spark, *args, **kwargs):
    partitions = int(kwargs.get('partitions', 4))
    number_of_steps = partitions * NUMBER_OF_STEPS_FACTOR
    count = spark.sparkContext\
        .parallelize(range(1, number_of_steps + 1),
                     partitions).map(f).reduce(add)
    return 4.0 * count / number_of_steps
