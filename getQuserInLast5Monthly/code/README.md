# EMR job control

这是一个辅助提交EMR任务的程序，通过cloud-manager实现了在同一个集群里提交任务，提高集群利用率

### 使用方式

1. 设置好 config.py 里的 SECRET_KEY
2. 通过 python add-job.py -h 可以看到所有设置参数
3. 将要执行的脚本传到S3, 建议任务脚本写得比较通用，可以传入参数进行微调设置
4. 调用add-job.py 脚本提交任务

如下例子实现了新增一个预先确定好类型的集群执行s3上的脚本的任务，将这个任务命名为countcountry:

```
python add-job.py \
	-n countcountry \
	-f s3://datamining.ym/cluster-backup/datamining-cluster/home/scripts/test/countcountry.py \
	-c datamining-cluster \
	-a 1
```

可以通过传递 -d client 参数，让任务使用 yarn-client 模式运行。

如果python脚本有两个，可以通过传递 -r s3://path/to/required.py 参数添加第二个python脚本

如果python脚本有多于两个，需要先将除了主脚本外的其它脚本打包成 zip 格式，然后传到S3， 然后 -r s3://path/to/required.zip 来添加
