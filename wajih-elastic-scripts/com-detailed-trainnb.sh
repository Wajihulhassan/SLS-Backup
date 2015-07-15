#!/bin/bash	

DFS_R=1
MAP_M=8096
RED_M=8096	
JOB_NAME=terasort
INPUT=wiki
JVM_MAX_M=6000
JVM_MAX_R=6000
NO_REDUCERS=1
EXP_NO=1
EXP_NAME=Sort
R_START=0.05
LOG_PATH_H=`pwd`/logs
EXAMPLES_JAR=/HDD-2TB/wajih/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.3.0.jar
ALGO=inout
CLUSH_SLAVES=`cat $HADOOP_CONF_DIR/slaves | sed 's/dco-node//g' | sed 's/-10g//g' | tr "\n" "," | sed 's/,$//g'`

WORK_DIR=/HDD-2TB/tmp/mahout-work-wiki-root

echo $CLUSH_SLAVES
clush -w dco-node[$CLUSH_SLAVES] '/root/cgroups-add-reducers.sh </dev/null >& ~/cgroup-out-file &'

cleanup (){
/HDD-2TB/wajih/bin/hadoop fs -rm -r temp/summedObservations
/HDD-2TB/wajih/bin/hadoop fs -rm -r /HDD-2TB/wajih/logs/root/logs/*
/HDD-2TB/wajih/bin/hadoop fs -rm -r /iostat/* 
 clush -w dco-node[$CLUSH_SLAVES] 'echo 1 > /proc/sys/vm/drop_caches'
/HDD-2TB/wajih/bin/hadoop fs -rm -r ${WORK_DIR}/model
 /HDD-2TB/wajih/bin/hadoop fs -rm -r temp/weights
}

experiment(){
	cleanup
	clush -w dco-node[$CLUSH_SLAVES] "echo \"Cgroup limit ${CGROUP_LIMIT}M\" > /SSD-200GB/logs/THIS_RUN_CFG"
	clush -w dco-node[$CLUSH_SLAVES] "echo \"${CGROUP_LIMIT}M\" > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes"
	clush -w dco-node[$CLUSH_SLAVES] "echo \"${CGROUP_LIMIT}M\" > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes"

        RED_JVM_FINAL=$(($CGROUP_LIMIT - 100))
	DIR_NAME="LOG-${EXP_NO}-DFS_R-${DFS_R}-MAP_M-${MAP_M}-RED_M-${RED_M}-RED_JVM-${JVM_MAX_R}-CGLMT-${CGROUP_LIMIT}-RED_start-${R_START}"
	DIR_SUFFIX=$DIR_NAME-`date +"%Y-%m-%d_%H-%M-%S"`
	STATS_DIR=$LOG_PATH_H/trainnb-wiki-lastweek/BLOCK${BLOCK_NO}/${EXP_NO}
	FINAL_DIR=${STATS_DIR}/$DIR_SUFFIX/
	
	mkdir -p $FINAL_DIR
        cal=$((${JVM_MAX_R} * 1000000))
        echo $cal	
	echo "-----------------------Before job ${BLOCK_NO} $DIR_NAME-----------------------------"
	 $MAHOUT_HOME/bin/mahout trainnb \
		-Ddfs.replication=$DFS_R \
		-Dmapreduce.reduce.tasks=$NO_REDUCERS \
                -Dmapreduce.map.memory.mb=$MAP_M \
		-Dmapreduce.reduce.memory.mb=$RED_M \
		-Dmapreduce.map.java.opts="-Xms${JVM_MAX_M}m  -Xmx${JVM_MAX_M}m" \
		-Dmapreduce.reduce.java.opts="-Xms${JVM_MAX_R}m -Xmx${JVM_MAX_R}m" \
		-Dmapreduce.job.reduce.slowstart.completedmaps=${R_START} \
		-Dmapreduce.reduce.merge.memtomem.enabled=${MEM_TO_MEM} \
		-Dmapreduce.reduce.input.buffer.percent=${INPUT_BUFFER} \
		-Dmapreduce.reduce.memory.totalbytes=$cal \
		-Dmapreduce.task.io.sort.mb=${IO_SORT_MB} \
		-Dmapreduce.task.io.sort.factor=${IO_SORT} \
      -i ${WORK_DIR}/wikipedidaVecs/tfidf-vectors/ -el -o ${WORK_DIR}/model -li ${WORK_DIR}/labelindex -c 2>&1 | tee $FINAL_DIR/exp.txt
	echo "-----------------------After job ${BLOCK_NO} $DIR_NAME-----------------------------"

           APP_ID=`/HDD-2TB/wajih/bin/hadoop fs -ls /HDD-2TB/wajih/logs/root/logs/ | awk '{print $8}'| awk -F/ '{print $7}'`
          for i in $APP_ID; 
           do JOB_ID=`echo $i | sed 's/[a-z]//g'` ;
                 JOB_ID2=`echo job$JOB_ID`; 
                /HDD-2TB/wajih/bin/mapred job -logs $JOB_ID2 > $FINAL_DIR/log_mapred_$i.txt;
                /HDD-2TB/wajih/bin/yarn logs -applicationId $i > $FINAL_DIR/log_yarn_$i.txt;
    
              ~/total-stats-2.sh $FINAL_DIR/log_mapred_$i.txt $JOB_ID $CGROUP_LIMIT  $STATS_DIR/total_stats.txt $FINAL_DIR/log_yarn_$i.txt;
           done;

}

run(){
	experiment
}


cgroup_clean(){
	for node in `seq 172 175`; do echo $node; ssh dco-node$node 'echo 300M > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes; echo 300M > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes'; done
	
	for node in `seq 172 175`; do echo $node; ssh dco-node$node 'echo 300M > /cgroup/mem_lmt_red/doit/memory.memsw.limit_in_bytes; echo 300M > /cgroup/mem_lmt_red/doit/memory.limit_in_bytes'; done

}
stop_start(){
	/HDD-2TB/wajih/sbin/stop-all.sh
        /HDD-2TB/wajih/sbin/start-all.sh
}
###################################################################################

IO_SORT=10
IO_SORT_MB=100
INPUT_BUFFER=1
MEM_TO_MEM=false

BLOCK_NO=1
R_START=1
cgroup_clean
for CGROUP_LIMIT in 654 808 1116 1270 2041 2812 2966 3274 3600 10000 ; do  EXP_NO=1; RED_M=$(( ${CGROUP_LIMIT} - 300 )) ; JVM_MAX_R=$(($CGROUP_LIMIT - 500));run ;done
cgroup_clean
for CGROUP_LIMIT in 654 808 1116 1270 2041 2812 2966 3274 3600 10000 ; do  EXP_NO=1; RED_M=$(( ${CGROUP_LIMIT} - 300 )) ; JVM_MAX_R=$(($CGROUP_LIMIT - 500));run ;done
cgroup_clean
for CGROUP_LIMIT in 654 808 1116 1270 2041 2812 2966 3274 3600 10000 ; do  EXP_NO=1; RED_M=$(( ${CGROUP_LIMIT} - 300 )) ; JVM_MAX_R=$(($CGROUP_LIMIT - 500));run ;done





#########################################################################################

#IO_SORT=10
#IO_SORT_MB=100
#INPUT_BUFFER=1
#MEM_TO_MEM=true

#BLOCK_NO=1
#R_START=1

#for CGROUP_LIMIT in 800 1000 1500 2000 3000 5000 7000 10000 20000 30000; do  EXP_NO=1; RED_M= $(( ${CGROUP_LIMIT} )) ; JVM_MAX_R=$(($CGROUP_LIMIT - 100));run2 ;done

#for CGROUP_LIMIT in 800 1000 1500 2000 3000 5000 7000 10000 20000 30000; do  EXP_NO=2; RED_M= $(( ${CGROUP_LIMIT} )) ; JVM_MAX_R=$(($CGROUP_LIMIT - 100));run2 ;done

#for CGROUP_LIMIT in 800 1000 1500 2000 3000 5000 7000 10000 20000 30000; do  EXP_NO=3; RED_M= $(( ${CGROUP_LIMIT} )) ; JVM_MAX_R=$(($CGROUP_LIMIT - 100));run2 ;done



#cp /HDD-2TB/wajih/Final-Experiments/com-detailed-trainnb.sh $LOG_PATH_H/trainnb-wiki/


clush -w dco-node[$CLUSH_SLAVES] 'kill -9 `pgrep cgroups-add`'
exit
