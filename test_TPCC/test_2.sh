#! /bin/bash

cc_name=(NO_WAIT WAIT_DIE HEKATON SILO TICTOC SLER)

sed -i '21s/.*/#define WORKLOAD 					TPCC/g' config.h
sed -i '8s/.*/#define THREAD_CNT					20/g' config.h

#sed -i '185s/.*/#define NUM_WH 						1/g' config.h

#sed -i '147s/.*/#define SYNTH_TABLE_SIZE 			10000000/g' config.h
#sed -i '148s/.*/#define ZIPF_THETA 					0/g' config.h
#sed -i '149s/.*/#define READ_PERC 					1/g' config.h
#sed -i '150s/.*/#define WRITE_PERC 					0/g' config.h
#sed -i '159s/.*/#define REQ_PER_QUERY				2/g' config.h
sed -i '139s/.*/#define MAX_TXN_PER_PART 			10000/g' config.h
#sed -i '160s/.*/#define LONG_TXN_RATIO              0/g' config.h
#sed -i '164s/.*/#define SYNTHETIC_YCSB              false/g' config.h


sed -i '60s/.*/#define ABORT_BUFFER_SIZE           1/g' config.h
sed -i '62s/.*/#define ABORT_BUFFER_ENABLE			true/g' config.h
sed -i '57s/.*/#define ABORT_PENALTY               100000/g' config.h

sed -i '304s/.*/#define TEST_NUM                    2/g' config.h


for((a=0;a<${#cc_name[@]};a++))
do
  sed -i '44s/.*/#define CC_ALG 						'${cc_name[$a]}'/g' config.h

  for((i=2;i<=40;i+=2))
  do
    sed -i '185s/.*/#define NUM_WH 						'$i'/g' config.h
    make -j
    ./rundb
  done
done


#for((i=0;i<=40;i+=2))
#do
#  sed -i '7s/.*/#define THREAD_CNT					'$i'/g' config.h
#  make -j
#  ./rundb
#done


#sed -i '7s/.*/#define THREAD_CNT					10/g' config.h
#sed -n '7,7p' config.h
