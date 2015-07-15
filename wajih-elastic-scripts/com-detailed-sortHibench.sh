#!/bin/bash	

DFS_R=1
MAP_M=8000
RED_M=8096	
JOB_NAME=sort
INPUT=HiBench/Sort/Input1
JVM_MAX_M=6000
JVM_MAX_R=6000
NO_REDUCERS=1
EXP_NO=1
EXP_NAME=Sort
R_START=1
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
	DIR_NAME="LOG-${EXP_NO}-DFS_R-${DFS_R}-MAP_M-${MAP_M}-RED_M-${RED_M}-RED_JVM-${JVM_MAX_R}-CGLMT-${CGROUP_LIMIT}-RED_start-${R_START}"
	DIR_SUFFIX=$DIR_NAME-`date +"%Y-%m-%d_%H-%M-%S"`
	STATS_DIR=$LOG_PATH_H/Sort-Mar18/
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
		-D mapreduce.reduce.java.opts="-Xms${JVM_MAX_R}m -Xmx${JVM_MAX_R}m" \
		-D mapreduce.job.reduce.slowstart.completedmaps=${R_START} \
                -D mapreduce.reduce.merge.memtomem.enabled=${MEM_TO_MEM} \
                -D mapreduce.reduce.input.buffer.percent=${INPUT_BUFFER} \
                -D mapreduce.reduce.memory.totalbytes=$cal \
                -D mapreduce.task.io.sort.mb=${IO_SORT_MB} \
                -D mapreduce.task.io.sort.factor=${IO_SORT} \
		-outKey org.apache.hadoop.io.Text \
		-outValue org.apache.hadoop.io.Text \
		-r 1 \
		  /$INPUT /teraOutput1 2>&1 | tee $FINAL_DIR/exp.txt
	echo "-----------------------After job ${BLOCK_NO} $DIR_NAME-----------------------------"

	APP_ID=`/HDD-2TB/wajih/bin/hadoop fs -ls /HDD-2TB/wajih/logs/root/logs/ | awk '{print $8}'| awk -F/ '{print $7}'`
	JOB_ID=`echo $APP_ID | sed 's/[a-z]//g'`
	JOB_ID2=`echo job$JOB_ID`
	
	/HDD-2TB/wajih/bin/yarn logs -applicationId $APP_ID > $FINAL_DIR/log_yarn.txt
	
	/HDD-2TB/wajih/bin/mapred job -logs $JOB_ID2 > $FINAL_DIR/log_mapred.txt
	~/total-stats.sh $FINAL_DIR $STATS_DIR $CGROUP_LIMIT
	cp /HDD-2TB/wajih/experiment-scripts-wajih/Final-Experiments/com-detailed-sortHibench.sh $FINAL_DIR
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
        /HDD-2TB/wajih/sbin/hadoop dfsadmin -safemode leave
}

IO_SORT=10
IO_SORT_MB=100
INPUT_BUFFER=1
MEM_TO_MEM=false
BLOCK_NO=4
R_START=1
INPUT=HiBench/Sort/sort24G     # input 20 gb

for EXP_NO in 1 2 3 4 5 
do
cgroup_clean

#for JVM_MAX_R in 1252 2505 3758 5262 11025 17541 22553 27564 37588 47612 49616 52623 67659
#for JVM_MAX_R in 2048 4096 6144 8192 10240 12288 14336 16384 18432 20480 22528 24576 26624 28672 30720 32768 34816 36864 38912 40960 43008 45056 47104 49152 51200 53248 55296 57344 59392 61440 63488 65536 67584 69632
for JVM_MAX_R in 4096 8192 12288 16384 20480 24576 28672 32768 36864 40960 45056 49152 53248 57344 61440 65536 69632
	do 
		CGROUP_LIMIT=$(($JVM_MAX_R + 1000))
		RED_M=$((${CGROUP_LIMIT})) ; 
		#JVM_MAX_R=$(($CGROUP_LIMIT - 2000));
		run ;
	done
done




#

cp /HDD-2TB/wajih/Final-Experiments/com-detailed-sortHibench.sh $LOG_PATH_H/Sort-RW/
clush -w dco-node[$CLUSH_SLAVES] 'kill -9 `pgrep cgroups-add`'
