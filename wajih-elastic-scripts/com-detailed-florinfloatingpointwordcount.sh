#!/bin/bash	


DFS_R=1
MAP_M=8096
RED_M=8096	
JOB_NAME=florinfloatingpointwordcount
INPUT=wiki20.xml
JVM_MAX_M=6000
JVM_MAX_R=6000
NO_REDUCERS=1
EXP_NO=1
EXP_NAME=FPWordCount
R_START=0
LOG_PATH_H=`pwd`/logs
EXAMPLES_JAR=/HDD-2TB/wajih/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.3.0.jar
ALGO=inout
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
	#STATS_DIR=$LOG_PATH_H/pagerank-lastweek/BLOCK${BLOCK_NO}/${EXP_NO}
	STATS_DIR=$LOG_PATH_H/FPWordcount-Mar13
	FINAL_DIR=${STATS_DIR}/$DIR_SUFFIX/

        mkdir -p $FINAL_DIR
        cal=$((${JVM_MAX_R} * 1000000))
        echo $cal
	
	mkdir -p $FINAL_DIR
	
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
		  /$INPUT /teraOutput1 2>&1 | tee $FINAL_DIR/exp.txt
	echo "-----------------------After job ${BLOCK_NO} $DIR_NAME-----------------------------"
           APP_ID=`/HDD-2TB/wajih/bin/hadoop fs -ls /HDD-2TB/wajih/logs/root/logs/ | awk '{print $8}'| awk -F/ '{print $7}'`
          for i in $APP_ID; 
           do JOB_ID=`echo $i | sed 's/[a-z]//g'` ;
                 JOB_ID2=`echo job$JOB_ID`;
		sleep 15 
                /HDD-2TB/wajih/bin/mapred job -logs $JOB_ID2 > $FINAL_DIR/log_mapred_$i.txt;
                /HDD-2TB/wajih/bin/yarn logs -applicationId $i > $FINAL_DIR/log_yarn_$i.txt;
    
              ~/total-stats-2.sh $FINAL_DIR/log_mapred_$i.txt $JOB_ID $CGROUP_LIMIT  $STATS_DIR/total_stats.txt $FINAL_DIR/log_yarn_$i.txt;
           done;


	
	
        }
cgroup_clean(){
        for node in `seq 172 175`; do echo $node; ssh dco-node$node 'echo 400M > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes; echo 400M > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes'; done
    
        for node in `seq 172 175`; do echo $node; ssh dco-node$node 'echo 400M > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes; echo 400M > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes'; done
}
run(){
	experiment
}

stop_start(){
	/HDD-2TB/wajih/sbin/stop-all.sh
        /HDD-2TB/wajih/sbin/start-all.sh
}
####################################################################################


#########################################################################################
BLOCK_NO=1
R_START=1

IO_SORT=10
IO_SORT_MB=100
INPUT_BUFFER=1
MEM_TO_MEM=false

for EXP_NO in `seq 1 5`
do
cgroup_clean
for JVM_MAX_R in 1024 2048 3072 4096 5120 6144 7168 8192 9216 10240 11264 12288 13312 14336 15360 16384 17408 
do  
	CGROUP_LIMIT=$(($JVM_MAX_R + 1000))
	RED_M=$((${CGROUP_LIMIT})) ;
	run
done
done


clush -w dco-node[$CLUSH_SLAVES] 'kill -9 `pgrep cgroups-add`'
