# Development Mode Demo

1. Start workers (same approach as `submit.sh`):
```
sbatch --nodes=<nodes> submit_dev.sh
```

2. Launch a client terminal:
```
bash examples/develop/launch_client.sh
```

3. Find the `MASTER_URL` from the `submit_dev.sh` job logs. You can locate the log paths with:
```
scontrol show job <JOB_ID>
```

4. Connect with PySpark using that master:
```
pyspark --master <MASTER_URL>
```

5. Check current parallelism (how many threads the cluster can offer):
```
>>> sc.defaultParallelism
```