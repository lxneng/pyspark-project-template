import argparse
import importlib
import time
import os
import sys
import logging
from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

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


def load_config(fp):
    with open(fp, 'r') as f:
        data = load(f, Loader=Loader)
    return data


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
    parser.add_argument('--config',
                        type=str,
                        default=os.path.join(os.path.dirname(__file__), "config.yml"),
                        help="config file")
    parser.add_argument('--env',
                        type=str,
                        default="development",
                        help="environment")
    args = parser.parse_args()
    log.info("Called with arguments: {}".format(args))

    job_args = dict()
    config = load_config(args.config).get(args.env)
    log.info(config)

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
    res = job_module.run(spark, config, **job_args)
    spark.stop()
    end = time.time()
    took_s = end - start
    log.info('[JOB {job} RESULT]: {result}, took {took_s} seconds'.format(job=args.job_name, result=res, took_s=took_s))
