# Intellij IDEA 整合Hadoop

### 前言

为了学习hadoop相关的内容，已经通过虚拟机搭建好了三台机器，构建了一个简单的hadoop集群，其中一台master，两台slave。客户端程序需要在`windows`平台下实现，我的操作系统是`win10`，java的IDE选择了`Intellij IDEA`，参考了很多博客以及大数据课程中老师的讲解，踩了很多坑，总算成功运行`WordCount`的`demo`，这里简单记录一下

### 环境需求及配置

#### 我的系统环境

+ win10
+ Intellij IDEA 2017 及以上
+ jdk 8
+ maven 3
+ hadoop 2.7.3
+ 虚拟主机三台，操作系统为Cent OS 6，一台名为master，剩余两台分别为slave1、slave2

### 具体的配置

##### 1. 启动hadoop集群

保证`192.168.163.101:50070`、`192.168.163.102:8088`(我的master机器的50070、8088端口)可以访问

##### 2. 修改hosts文件

hosts文件目录 : `C:\Windows\System32\drivers\etc\hosts`

在文件末尾追加如下内容(具体的结合自己的服务器考虑)

```
# 客户机的ip
172.20.10.3 localhost
# master主机的ip
192.168.163.101 master
# slave的ip
192.168.163.102 slave1
192.168.163.103 slave2
```

修改后，再访问hadoop的页面就可以使用`master:50070`、`master:8088`作为url了

##### 3. 获取hadoop包

从master主机中将已经配置好的`hadoop`文件夹下载到本机(下载方法随意)

##### 4. 配置环境变量

这里主要是配置`hadoop`的环境变量，当然`java`环境也是必须的。

+ 新建环境变量 : `HADOOP_HOME`,内容为从主机中下载的`hadoop`文件夹的路径(eg : E:\summer_train\dev\hadoop-2.7.3)
+ 新建环境变量 : `HADOOP_BIN_PATH`,内容为`hadoop`目录下`bin`目录的路径，值为`%HADOOP_HOME%\bin`
+ 新建环境变量 : `HADOOP_USER_NAME`,内容为`root`
+ 在环境变量`Path`后追加两项`%HADOOP_HOME%\bin;`,`%HADOOP_HOME%\sbin;`

##### 5. 添加一些windows平台需要的库

具体需要的内容已经放到了`hadoop/bin/`目录下，下载后即可使用。

将其中的`hadoop.dll`复制到``C:\Windows\System32`目录下，其余内容复制到之前下载的`hadoop`目录的`bin`目录下

### HadoopIntellijPlugin

##### 获取插件

在正式进行开发前，可以先安装一个连接hadoop集群并进行文件管理的插件，`eclipse`的`hadoop`插件相对成熟一些且比较容易获取，`IDEA`的话目前我只在`github`上找到了一款

[插件地址](https://github.com/fangyuzhong2016/HadoopIntellijPlugin)

[该插件的中文简介](http://www.fangyuzhong.com/archives/389)

##### 安装插件

具体的安装配置方法已有描述，这里简单补充一下。

将代码clone到本地后，解压并使用`IDEA`打开，在`pom.xml`中更改`IDEA`的安装路径为本机的安装路径，`hadoop`的版本可以不用改（我改了之后是报错了），等待maven下载完成依赖后，使用maven进行项目的编译。

具体步骤如下 : 

+ 设置maven运行

![](https://github.com/XingToMax/BigDataLearning/blob/master/images/1.png)

+ 选取maven

  ​		![](https://github.com/XingToMax/BigDataLearning/blob/master/images/2.png)

+ 设置clean命令

  ​		![](https://github.com/XingToMax/BigDataLearning/blob/master/images/3.png)

应用修改后使用maven运行项目，完成clean命令后，将clean改为`assembly:assembly`，成功执行后，在`target`目录下生成了名为`HadoopIntellijPlugin-1.0.zip`的文件。在`File->Settings`中打开`Plugins`，选择`Install plugin from disk`，选择生成的zip文件即可安装。安装成功后，在菜单栏就会出现一个`Hadoop`项，可以进行文件系统的操作。·

##### 使用插件

新建连接 :

​	M/R 地址 : master 端口 : 9001

​	HDFS地址 : master 端口 : 9000

一些视图 ：

![](https://github.com/XingToMax/BigDataLearning/blob/master/images/4.png)

![](https://github.com/XingToMax/BigDataLearning/blob/master/images/5.png)

### Helloworld : WordCount

新建maven项目

##### pom.xml

``` xml
    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.7.2</version>
        </dependency>
        <dependency>
            <groupId>commons-cli</groupId>
            <artifactId>commons-cli</artifactId>
            <version>1.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-core</artifactId>
            <version>2.7.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-jobclient</artifactId>
            <version>2.7.2</version>
        </dependency>
        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.17</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-examples</artifactId>
            <version>2.7.2</version>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.16.6</version>
        </dependency>
        <dependency>
            <groupId>org.apache.zookeeper</groupId>
            <artifactId>zookeeper</artifactId>
            <version>3.4.8</version>
        </dependency>
    </dependencies>
```

##### 新建java类`WordCount`

``` java
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/tomax/words.log"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/tomax/res.log"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

##### 上传words.log至hdfs下的/tomax目录

``` 
aa bb aa cc ab
bb aa ac cc dd
asdfasd sdf aa
tomax tomax aa
tomax bb
to sdf 
aa sdf 
tomax fgfdg
asdf gfg
ssrfgf sd
```

##### 执行，查看res.log下的输出文件(注意，如果执行前该文件已存在会报错，需删除后再执行)

``` 
aa	6
ab	1
ac	1
asdf	1
asdfasd	1
bb	3
cc	2
dd	1
fgfdg	1
gfg	1
sd	1
sdf	3
ssrfgf	1
to	1
tomax	4
```

##### 为了方便理解，在`map`和`reduce`中增加输出，有控制台关键输出如下

```
18/07/12 21:08:26 INFO mapreduce.Job: Running job: job_local1134057227_0001
18/07/12 21:08:26 INFO mapred.LocalJobRunner: OutputCommitter set in config null
18/07/12 21:08:26 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
18/07/12 21:08:26 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
18/07/12 21:08:26 INFO mapred.LocalJobRunner: Waiting for map tasks
18/07/12 21:08:26 INFO mapred.LocalJobRunner: Starting task: attempt_local1134057227_0001_m_000000_0
18/07/12 21:08:26 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
18/07/12 21:08:26 INFO util.ProcfsBasedProcessTree: ProcfsBasedProcessTree currently is supported only on Linux.
18/07/12 21:08:26 INFO mapred.Task:  Using ResourceCalculatorProcessTree : org.apache.hadoop.yarn.util.WindowsBasedProcessTree@21bb8722
18/07/12 21:08:26 INFO mapred.MapTask: Processing split: hdfs://master:9000/tomax/words.log:0+116
18/07/12 21:08:26 INFO mapred.MapTask: (EQUATOR) 0 kvi 26214396(104857584)
18/07/12 21:08:26 INFO mapred.MapTask: mapreduce.task.io.sort.mb: 100
18/07/12 21:08:26 INFO mapred.MapTask: soft limit at 83886080
18/07/12 21:08:26 INFO mapred.MapTask: bufstart = 0; bufvoid = 104857600
18/07/12 21:08:26 INFO mapred.MapTask: kvstart = 26214396; length = 6553600
18/07/12 21:08:26 INFO mapred.MapTask: Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
map 输入 <key,value> : <0,aa bb aa cc ab>
map 输出 <key,value> : <aa,1>
map 输出 <key,value> : <bb,1>
map 输出 <key,value> : <aa,1>
map 输出 <key,value> : <cc,1>
map 输出 <key,value> : <ab,1>
map 输入 <key,value> : <15,bb aa ac cc dd>
map 输出 <key,value> : <bb,1>
map 输出 <key,value> : <aa,1>
map 输出 <key,value> : <ac,1>
map 输出 <key,value> : <cc,1>
map 输出 <key,value> : <dd,1>
map 输入 <key,value> : <30,asdfasd sdf aa>
map 输出 <key,value> : <asdfasd,1>
map 输出 <key,value> : <sdf,1>
map 输出 <key,value> : <aa,1>
map 输入 <key,value> : <45,tomax tomax aa>
map 输出 <key,value> : <tomax,1>
map 输出 <key,value> : <tomax,1>
map 输出 <key,value> : <aa,1>
map 输入 <key,value> : <60,tomax bb>
map 输出 <key,value> : <tomax,1>
map 输出 <key,value> : <bb,1>
map 输入 <key,value> : <69,to sdf >
map 输出 <key,value> : <to,1>
map 输出 <key,value> : <sdf,1>
map 输入 <key,value> : <77,aa sdf >
map 输出 <key,value> : <aa,1>
map 输出 <key,value> : <sdf,1>
map 输入 <key,value> : <85,tomax fgfdg>
map 输出 <key,value> : <tomax,1>
map 输出 <key,value> : <fgfdg,1>
map 输入 <key,value> : <97,asdf gfg>
map 输出 <key,value> : <asdf,1>
map 输出 <key,value> : <gfg,1>
map 输入 <key,value> : <106,ssrfgf sd>
map 输出 <key,value> : <ssrfgf,1>
map 输出 <key,value> : <sd,1>
18/07/12 21:08:26 INFO mapred.LocalJobRunner: 
18/07/12 21:08:26 INFO mapred.MapTask: Starting flush of map output
18/07/12 21:08:26 INFO mapred.MapTask: Spilling map output
18/07/12 21:08:26 INFO mapred.MapTask: bufstart = 0; bufend = 226; bufvoid = 104857600
18/07/12 21:08:26 INFO mapred.MapTask: kvstart = 26214396(104857584); kvend = 26214288(104857152); length = 109/6553600
reduce 输入 <key,value> : <aa,[1,1,1,1,1,1,]>
reduce 输出 <key,value> : <aa,6>
reduce 输入 <key,value> : <ab,[1,]>
reduce 输出 <key,value> : <ab,1>
reduce 输入 <key,value> : <ac,[1,]>
reduce 输出 <key,value> : <ac,1>
reduce 输入 <key,value> : <asdf,[1,]>
reduce 输出 <key,value> : <asdf,1>
reduce 输入 <key,value> : <asdfasd,[1,]>
reduce 输出 <key,value> : <asdfasd,1>
reduce 输入 <key,value> : <bb,[1,1,1,]>
reduce 输出 <key,value> : <bb,3>
reduce 输入 <key,value> : <cc,[1,1,]>
reduce 输出 <key,value> : <cc,2>
reduce 输入 <key,value> : <dd,[1,]>
reduce 输出 <key,value> : <dd,1>
reduce 输入 <key,value> : <fgfdg,[1,]>
reduce 输出 <key,value> : <fgfdg,1>
reduce 输入 <key,value> : <gfg,[1,]>
reduce 输出 <key,value> : <gfg,1>
reduce 输入 <key,value> : <sd,[1,]>
reduce 输出 <key,value> : <sd,1>
reduce 输入 <key,value> : <sdf,[1,1,1,]>
reduce 输出 <key,value> : <sdf,3>
reduce 输入 <key,value> : <ssrfgf,[1,]>
reduce 输出 <key,value> : <ssrfgf,1>
reduce 输入 <key,value> : <to,[1,]>
reduce 输出 <key,value> : <to,1>
reduce 输入 <key,value> : <tomax,[1,1,1,1,]>
reduce 输出 <key,value> : <tomax,4>
18/07/12 21:08:26 INFO mapred.MapTask: Finished spill 0
18/07/12 21:08:26 INFO mapred.Task: Task:attempt_local1134057227_0001_m_000000_0 is done. And is in the process of committing
18/07/12 21:08:26 INFO mapred.LocalJobRunner: map
18/07/12 21:08:26 INFO mapred.Task: Task 'attempt_local1134057227_0001_m_000000_0' done.
18/07/12 21:08:26 INFO mapred.LocalJobRunner: Finishing task: attempt_local1134057227_0001_m_000000_0
18/07/12 21:08:26 INFO mapred.LocalJobRunner: map task executor complete.
18/07/12 21:08:26 INFO mapred.LocalJobRunner: Waiting for reduce tasks
18/07/12 21:08:26 INFO mapred.LocalJobRunner: Starting task: attempt_local1134057227_0001_r_000000_0
18/07/12 21:08:26 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
18/07/12 21:08:26 INFO util.ProcfsBasedProcessTree: ProcfsBasedProcessTree currently is supported only on Linux.
18/07/12 21:08:26 INFO mapred.Task:  Using ResourceCalculatorProcessTree : org.apache.hadoop.yarn.util.WindowsBasedProcessTree@167094bb
18/07/12 21:08:26 INFO mapred.ReduceTask: Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@704f0f66
18/07/12 21:08:26 INFO reduce.MergeManagerImpl: MergerManager: memoryLimit=2655623424, maxSingleShuffleLimit=663905856, mergeThreshold=1752711552, ioSortFactor=10, memToMemMergeOutputsThreshold=10
18/07/12 21:08:26 INFO reduce.EventFetcher: attempt_local1134057227_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
18/07/12 21:08:26 INFO reduce.LocalFetcher: localfetcher#1 about to shuffle output of map attempt_local1134057227_0001_m_000000_0 decomp: 156 len: 160 to MEMORY
18/07/12 21:08:26 INFO reduce.InMemoryMapOutput: Read 156 bytes from map-output for attempt_local1134057227_0001_m_000000_0
18/07/12 21:08:26 INFO reduce.MergeManagerImpl: closeInMemoryFile -> map-output of size: 156, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->156
18/07/12 21:08:26 INFO reduce.EventFetcher: EventFetcher is interrupted.. Returning
18/07/12 21:08:26 INFO mapred.LocalJobRunner: 1 / 1 copied.
18/07/12 21:08:26 INFO reduce.MergeManagerImpl: finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
18/07/12 21:08:26 INFO mapred.Merger: Merging 1 sorted segments
18/07/12 21:08:26 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 151 bytes
18/07/12 21:08:26 INFO reduce.MergeManagerImpl: Merged 1 segments, 156 bytes to disk to satisfy reduce memory limit
18/07/12 21:08:26 INFO reduce.MergeManagerImpl: Merging 1 files, 160 bytes from disk
18/07/12 21:08:26 INFO reduce.MergeManagerImpl: Merging 0 segments, 0 bytes from memory into reduce
18/07/12 21:08:26 INFO mapred.Merger: Merging 1 sorted segments
18/07/12 21:08:26 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 151 bytes
18/07/12 21:08:26 INFO mapred.LocalJobRunner: 1 / 1 copied.
reduce 输入 <key,value> : <aa,[6,]>
reduce 输出 <key,value> : <aa,6>
reduce 输入 <key,value> : <ab,[1,]>
reduce 输出 <key,value> : <ab,1>
reduce 输入 <key,value> : <ac,[1,]>
reduce 输出 <key,value> : <ac,1>
reduce 输入 <key,value> : <asdf,[1,]>
reduce 输出 <key,value> : <asdf,1>
reduce 输入 <key,value> : <asdfasd,[1,]>
reduce 输出 <key,value> : <asdfasd,1>
reduce 输入 <key,value> : <bb,[3,]>
reduce 输出 <key,value> : <bb,3>
reduce 输入 <key,value> : <cc,[2,]>
reduce 输出 <key,value> : <cc,2>
reduce 输入 <key,value> : <dd,[1,]>
reduce 输出 <key,value> : <dd,1>
reduce 输入 <key,value> : <fgfdg,[1,]>
reduce 输出 <key,value> : <fgfdg,1>
reduce 输入 <key,value> : <gfg,[1,]>
reduce 输出 <key,value> : <gfg,1>
reduce 输入 <key,value> : <sd,[1,]>
reduce 输出 <key,value> : <sd,1>
reduce 输入 <key,value> : <sdf,[3,]>
reduce 输出 <key,value> : <sdf,3>
reduce 输入 <key,value> : <ssrfgf,[1,]>
reduce 输出 <key,value> : <ssrfgf,1>
reduce 输入 <key,value> : <to,[1,]>
reduce 输出 <key,value> : <to,1>
reduce 输入 <key,value> : <tomax,[4,]>
reduce 输出 <key,value> : <tomax,4>
18/07/12 21:08:26 INFO Configuration.deprecation: mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
18/07/12 21:08:26 INFO mapred.Task: Task:attempt_local1134057227_0001_r_000000_0 is done. And is in the process of committing
18/07/12 21:08:26 INFO mapred.LocalJobRunner: 1 / 1 copied.
18/07/12 21:08:26 INFO mapred.Task: Task attempt_local1134057227_0001_r_000000_0 is allowed to commit now
18/07/12 21:08:26 INFO output.FileOutputCommitter: Saved output of task 'attempt_local1134057227_0001_r_000000_0' to hdfs://master:9000/tomax/res8.log/_temporary/0/task_local1134057227_0001_r_000000
18/07/12 21:08:26 INFO mapred.LocalJobRunner: reduce > reduce
18/07/12 21:08:26 INFO mapred.Task: Task 'attempt_local1134057227_0001_r_000000_0' done.
18/07/12 21:08:26 INFO mapred.LocalJobRunner: Finishing task: attempt_local1134057227_0001_r_000000_0
18/07/12 21:08:26 INFO mapred.LocalJobRunner: reduce task executor complete.
18/07/12 21:08:27 INFO mapreduce.Job: Job job_local1134057227_0001 running in uber mode : false
18/07/12 21:08:27 INFO mapreduce.Job:  map 100% reduce 100%
18/07/12 21:08:27 INFO mapreduce.Job: Job job_local1134057227_0001 completed successfully
18/07/12 21:08:27 INFO mapreduce.Job: Counters: 35
	File System Counters
		FILE: Number of bytes read=666
		FILE: Number of bytes written=562146
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=232
		HDFS: Number of bytes written=94
		HDFS: Number of read operations=13
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=4
	Map-Reduce Framework
		Map input records=10
		Map output records=28
		Map output bytes=226
		Map output materialized bytes=160
		Input split bytes=99
		Combine input records=28
		Combine output records=15
		Reduce input groups=15
		Reduce shuffle bytes=160
		Reduce input records=15
		Reduce output records=15
		Spilled Records=30
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=4
		Total committed heap usage (bytes)=590872576
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=116
	File Output Format Counters 
		Bytes Written=94



```

简单分析一下

+ 首先，数据是逐行进入map的，在map中将每一行数据拆成了一个一个单词，然后map为<单词,1>的键值对
+ 然后，系统将map层处理的结果进行了简单的处理，将同key的数据汇总，同key的value合并成集合
+ 接着，在combiner层，通过reduce将key集合计算成数量输出
+ 最后的reduce层起始做了重复的工作，因为内容已经在上一层处理完了



一个比较简单的sample，新手起步，如有不足，还望指出





