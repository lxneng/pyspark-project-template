import argparse
import importlib
import time
import os
import sys
import logging
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark


logging.basicConfig(
    level='INFO',
    format='%(asctime)s %(levelname)-5.5s [%(name)s] %(message)s')
log = logging.getLogger('pyspk-jobs')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job',
                        type=str,
                        required=True,
                        dest='job_name',
                        help="The name of the job module you want to run. (ex: pi will run job on jobs.pi package)")
    parser.add_argument('--job-args',
                        nargs='*',
                        help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")
    args = parser.parse_args()
    log.info("Called with arguments: {}".format(args))

    job_args = dict()

    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        log.info('job_args_tuples: {}'.format(job_args_tuples))
        job_args = {a[0]: a[1] for a in job_args_tuples}

    spark = pyspark.sql.SparkSession\
        .builder\
        .appName(args.job_name)\
        .getOrCreate()

    job_module = importlib.import_module('jobs.%s' % args.job_name)

    start = time.time()
    res = job_module.run(spark, **job_args)
    spark.stop()
    end = time.time()
    took_s = end - start
    log.info('[JOB {job} RESULT]: {result}, took {took_s} seconds'.format(job=args.job_name, result=res, took_s=took_s))
