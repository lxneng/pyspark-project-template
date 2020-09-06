# PySpark Project Template

## Usage

```
(base) eric@pop-os:~/Projects/pyspark-project-template/src$ python main.py --help
usage: main.py [-h] --job JOB_NAME [--job-args [JOB_ARGS [JOB_ARGS ...]]]

Run a PySpark job

optional arguments:
  -h, --help            show this help message and exit
  --job JOB_NAME        The name of the job module you want to run. (ex: pi
                        will run job on jobs.pi package)
  --job-args [JOB_ARGS [JOB_ARGS ...]]
                        Extra arguments to send to the PySpark job (example:
                        --job-args template=manual-email1 foo=bar

(base) eric@pop-os:~/Projects/pyspark-project-template/src$ python main.py --job pi --job-args partition=2
2020-08-24 13:47:02,866 INFO  [pyspk-jobs] Called with arguments: Namespace(job_args=['partition=20'], job_name='pi')
2020-08-24 13:47:02,866 INFO  [pyspk-jobs] job_args_tuples: [['partition', '2']]
2020-08-24 13:47:08,263 INFO  [pyspk-jobs] [JOB pi RESULT]: 3.140354, took 2.50959849357605 seconds
```

### Packaging

```
make all
```

### upload to hdfs

```
hdfscli upload -f dist pyspk-demo
```

### run

```
spark-submit --master yarn --py-files hdfs:/user/lxneng/pyspk-demo/dist/jobs.zip,hdfs:/user/lxneng/pyspk-demo/dist/libs.zip hdfs:///user/lxneng/pyspk-demo/dist/main.py --job pi
```
