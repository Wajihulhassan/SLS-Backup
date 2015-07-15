#!/bin/bash	

DFS_R=1
MAP_M=8000	#mapreduce.map.memory.mb
RED_M=8096      #mapreduce.reduce.memory.mb	
JVM_MAX_M=6000  #mapreduce.map.java.opts
JVM_MAX_R=6000  #mapreduce.reduce.java.opts
NO_REDUCERS=1
EXP_NO=1
IO_SORT=10	#mapreduce.task.io.sort.factor
IO_SORT_MB=100	#mapreduce.task.io.sort.mb
INPUT_BUFFER=1	#mapreduce.reduce.input.buffer.percent
MEM_TO_MEM=false

JOB_NAME=terasort
EXP_NAME=Sort
R_START=1	#mapreduce.job.reduce.slowstart.completedmaps



LOG_PATH_H=`pwd`/logs/
EXAMPLES_JAR=/HDD-2TB/wajih/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.3.0.jar

CLUSH_SLAVES=`cat $HADOOP_CONF_DIR/slaves | sed 's/dco-node//g' | sed 's/-10g//g' | tr "\n" "," | sed 's/,$//g'`


clush -w dco-node[$CLUSH_SLAVES] '/root/cgroups-add-reducers.sh </dev/null >& ~/cgroup-out-file &'
echo $CLUSH_SLAVES


cleanup (){
	/HDD-2TB/wajih/bin/hadoop fs -rm -r /HDD-2TB/wajih/logs/root/logs/*
 	/HDD-2TB/wajih/bin/hadoop fs -rm -r /teraOu*
	/HDD-2TB/wajih/bin/hadoop fs -rm -r /iostat/*
	clush -w dco-node[$CLUSH_SLAVES] 'echo 1 > /proc/sys/vm/drop_caches'
}

experiment(){
	cleanup
	clush -w dco-node[$CLUSH_SLAVES] "echo \"Cgroup limit ${CGROUP_LIMIT}M\" > /SSD-200GB/logs/THIS_RUN_CFG"
        clush -w dco-node[$CLUSH_SLAVES] "echo \"${CGROUP_LIMIT}M\" > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes"
        clush -w dco-node[$CLUSH_SLAVES] "echo \"${CGROUP_LIMIT}M\" > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes"

	DIR_NAME="LOG-${EXP_NO}-$BLOCK_NO-DFS_R-${DFS_R}-MAP_M-${MAP_M}-RED_M-${RED_M}-RED_JVM-${JVM_MAX_R}-CGLMT-${CGROUP_LIMIT}-RED_start-${R_START}"
	DIR_SUFFIX=$DIR_NAME-`date +"%Y-%m-%d_%H-%M-%S"`
	STATS_DIR=$LOG_PATH_H/TeraSort-Mar12-detailed
	FINAL_DIR=${STATS_DIR}/$DIR_SUFFIX/
	
	mkdir -p $FINAL_DIR
	cal=$((${JVM_MAX_R} * 1048576))
	echo $cal
	
	echo "-----------------------Before job ${BLOCK_NO} $DIR_NAME-----------------------------"
	 /HDD-2TB/wajih/bin/hadoop jar $EXAMPLES_JAR $JOB_NAME \
		-D mapreduce.map.memory.mb=$MAP_M \
		-D dfs.replication=$DFS_R \
		-D mapred.reduce.tasks=$NO_REDUCERS \
		-D mapreduce.reduce.memory.mb=$RED_M \
		-D mapreduce.map.java.opts="-Xms${JVM_MAX_M}m -Xmx${JVM_MAX_M}m" \
		-D mapreduce.reduce.java.opts="-Xms${JVM_MAX_R}m -Xmx${JVM_MAX_R}m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
		-D mapreduce.job.reduce.slowstart.completedmaps=${R_START} \
		-D mapreduce.reduce.merge.memtomem.enabled=${MEM_TO_MEM} \
		-D mapreduce.reduce.input.buffer.percent=${INPUT_BUFFER} \
		-D mapreduce.reduce.memory.totalbytes=$cal \
		-D mapreduce.task.io.sort.mb=${IO_SORT_MB} \
		-D mapreduce.task.io.sort.factor=${IO_SORT} \
		  /$INPUT /teraOutput1 2>&1 | tee $FINAL_DIR/exp.txt
	echo "-----------------------After job ${BLOCK_NO} $DIR_NAME-----------------------------"

	APP_ID=`/HDD-2TB/wajih/bin/hadoop fs -ls /HDD-2TB/wajih/logs/root/logs/ | awk '{print $8}'| awk -F/ '{print $7}'`
	JOB_ID=`echo $APP_ID | sed 's/[a-z]//g'`
	JOB_ID2=`echo job$JOB_ID`

	sleep 15	
	 /HDD-2TB/wajih/bin/yarn logs -applicationId $APP_ID > $FINAL_DIR/log_yarn.txt
	
	 /HDD-2TB/wajih/bin/mapred job -logs $JOB_ID2 > $FINAL_DIR/log_mapred.txt
	 ~/total-stats.sh $FINAL_DIR $STATS_DIR $CGROUP_LIMIT
	 cp /HDD-2TB/wajih/experiment-scripts-wajih/Final-Experiments/com-detailed-terasort.sh $FINAL_DIR
	 conf_file=`bin/hdfs dfs -ls -R / | grep -i $JOB_ID2 | grep conf.xml | grep xml | awk '{print $8}'`
	 /HDD-2TB/wajih/bin/hdfs dfs -copyToLocal $conf_file $FINAL_DIR
}


cgroup_clean(){
        for node in `seq 167 170`; do echo $node; ssh dco-node$node 'echo 400M > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes; echo 400M > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes'; done
    
        for node in `seq 167 170`; do echo $node; ssh dco-node$node 'echo 400M > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes; echo 400M > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes'; done
}

run(){
	experiment
}

stop_start(){
	/HDD-2TB/wajih/sbin/stop-all.sh
        /HDD-2TB/wajih/sbin/start-all.sh
        /HDD-2TB/wajih/bin/hadoop dfsadmin -safemode leave
}


BLOCK_NO=30GB
INPUT=terasort30g

for EXP_NO in 1 2 3 4 5 
do
cgroup_clean
for JVM_MAX_R in 2048 4096 6144 8192 10240 12288 14336 16384 18432 20480 22528 24576 26624 28672 30720 32768 34816 36864 38912 40960 43008 45056 47104 49152 51200 53248 55296 57344 59392 61440 63488 65536
#for JVM_MAX_R in 1500 2500 3500 4500 5500 6500 7500 8500 9500 10500 11500 12500 13500 14500 15500 16500 17500 18500 19500 20500 21500 22500 23500 24500 25500 26500 27500 28500 29500 30500 31500 32500 33500 34500 35500 36500 37500 38500 39500 40500 41500 42500 43500 44500 45500 46500 47500 48500 49500 50500 51500 52500 53500 54500 55500 56500 57500 58500 59500 60500 61500 62500 63500 64500 65500 66500 67500
	do 
		CGROUP_LIMIT=$(($JVM_MAX_R + 1000))
		RED_M=$((${CGROUP_LIMIT})) ; 
		#JVM_MAX_R=$(($CGROUP_LIMIT - 2000));
		run ;
	done
done

clush -w dco-node[$CLUSH_SLAVES] 'kill -9 `pgrep cgroups-add`'



