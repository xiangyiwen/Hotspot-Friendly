#ifndef _CONFIG_H_
#define _CONFIG_H_

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define TERMINATE_BY_COUNT true
#define THREAD_CNT					20
#define PART_CNT					1
// each transaction only accesses 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT			1
#define PAGE_SIZE					4096
#define CL_SIZE						64
// CPU_FREQ is used to get accurate timing info
#define CPU_FREQ 					2.1 // in GHz/s

// # of transactions to run for warmup
#define WARMUP						1000

// YCSB or TPCC
#define WORKLOAD 					YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR				false
#define STATS_ENABLE				true
#define TIME_ENABLE					true

#define MEM_ALLIGN					8

// [THREAD_ALLOC]
#define THREAD_ALLOC				false
#define THREAD_ARENA_SIZE			(1UL << 22)
#define MEM_PAD 					true

// [PART_ALLOC]
#define PART_ALLOC 					false
#define MEM_SIZE					(1UL << 30)
#define NO_FREE						false

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, DL_DETECT, TIMESTAMP, MVCC, HEKATON, HSTORE, OCC, VLL, TICTOC, SILO, HOTSPOT_FRIENDLY , BAMBOO
// TODO TIMESTAMP does not work at this moment
#define CC_ALG 						HOTSPOT_FRIENDLY
#define ISOLATION_LEVEL 			SERIALIZABLE

// latch options
#define LATCH LH_MCSLOCK

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER					false
// transaction roll back changes after abort
#define ROLL_BACK					true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN					false
#define BUCKET_CNT					31
#define ABORT_PENALTY               100000
//#define ABORT_PENALTY               5000
//#define ABORT_BUFFER_SIZE			1
#define ABORT_BUFFER_SIZE           1
// ABORT_BUFFER_ENABLE : All CC need this flag be true except HOTSPOT_FRIENDLY. Otherwise, other CC cannot run TPCC.
#define ABORT_BUFFER_ENABLE			true
// [ INDEX ]
#define ENABLE_LATCH				false
#define CENTRAL_INDEX				false
#define CENTRAL_MANAGER 			false
#define INDEX_STRUCT				IDX_HASH
#define BTREE_ORDER 				16

// [DL_DETECT]
#define DL_LOOP_DETECT				1000 	// 100 us
#define DL_LOOP_TRIAL				100	// 1 us
#define NO_DL						KEY_ORDER
#define TIMEOUT						1000000 // 1ms
// [TIMESTAMP]
#define TS_TWR						false
#define TS_ALLOC					TS_CAS
#define TS_BATCH_ALLOC				false
#define TS_BATCH_NUM				1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
//#define HIS_RECYCLE_LEN		    10
//#define MAX_PRE_REQ				1024
//#define MAX_READ_REQ				1024
#define MIN_TS_INTVL				5000000 //5 ms. In nanoseconds
// [OCC]
#define MAX_WRITE_SET				10
#define PER_ROW_VALID				true
// [TICTOC]
#define WRITE_COPY_FORM				"data" // ptr or data
#define TICTOC_MV					false
#define WR_VALIDATION_SEPARATE		true
#define WRITE_PERMISSION_LOCK		false
#define ATOMIC_TIMESTAMP			"false"
// [TICTOC, SILO]
#define VALIDATION_LOCK				"no-wait" // no-wait or waiting
#define PRE_ABORT					"true"
#define ATOMIC_WORD					true
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions.
#define HSTORE_LOCAL_TS				false
// [VLL]
#define TXN_QUEUE_SIZE_LIMIT		THREAD_CNT
// [BAMBOO]
#define BB_DYNAMIC_TS true
#define BB_OPT_RAW true
#define BB_LAST_RETIRE 0
#define BB_PRECOMMIT false
#define BB_AUTORETIRE false
#define BB_ALWAYS_RETIRE_READ true
// [WW]
#define WW_STARV_FREE false
// [IC3]
#define IC3_EAGER_EXEC              true
#define IC3_RENDEZVOUS              true
#define IC3_FIELD_LOCKING           false // should not be true
#define IC3_MODIFIED_TPCC           false

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND					false
#define LOG_REDO					false
#define LOG_BATCH_TIME				10 // in ms

/***********************************************/
// Benchmark
/***********************************************/
#define INSERT_ENABLED              false
#define THINKTIME				    0
#define MAX_RUNTIME                 10
// max number of rows touched per transaction
// MAX_ROW_PER_TXN: the count of requests in a long transaction of YCSB
#define MAX_ROW_PER_TXN             1000
#define QUERY_INTVL 				1UL
// MAX_TXN_PER_PART: used to calculate the txn count of a thread
#define MAX_TXN_PER_PART 			10000
#define FIRST_PART_LOCAL 			true
#define MAX_TUPLE_SIZE				1024 // in bytes
#define MAX_FIELD_SIZE              50
// ==== [YCSB] ====
#define INIT_PARALLELISM			40
// SYNTH_TABLE_SIZE: tuple count of the YCSB table
//#define SYNTH_TABLE_SIZE 100
#define SYNTH_TABLE_SIZE 			10000000
#define ZIPF_THETA 					0.9
#define READ_PERC 					0.5
#define WRITE_PERC 					0.5
#define SCAN_PERC 					0
#define SCAN_LEN					20
#define PART_PER_TXN 				1
#define PERC_MULTI_PART				1
// Optional optimization for read_only long transaction.
#define READ_ONLY_OPTIMIZATION_ENABLE false

//REQ_PER_QUERY: request count of a txn (short/normal transaction in YCSB)
#define REQ_PER_QUERY				16
#define LONG_TXN_RATIO              0
#define LONG_TXN_READ_RATIO			1
#define FIELD_PER_TUPLE				10
// ==== [YCSB-synthetic] ====
#define SYNTHETIC_YCSB              true
#define POS_HS                      RANDOM
#define SPECIFIED_RATIO             1
#define FLIP_RATIO                  0
#define NUM_HS                      1
#define FIRST_HS                    WR
#define SECOND_HS                   WR
#define FIXED_HS                    0
// Different ratio of operation type for hotspot
#define READ_HOTSPOT_RATIO          0
#define WRITE_HOTSPOT_RATIO         1

// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL					false
// Some of the transactions read the data but never use them.
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions are not modeled.
#define TPCC_ACCESS_ALL 			false
#define WH_UPDATE					true
// NUM_WH: define the warehouse count
#define NUM_WH 						1
//
enum TPCCTxnType {
  TPCC_PAYMENT,
  TPCC_NEW_ORDER,
  TPCC_DELIVERY,
  TPCC_ORDER_STATUS,
  TPCC_STOCK_LEVEL,
  TPCC_ALL};
extern TPCCTxnType 					g_tpcc_txn_type;

//#define TXN_TYPE					TPCC_ALL
#define PERC_PAYMENT 				0.5
#define PERC_DELIVERY               0
#define PERC_ORDERSTATUS            0
#define PERC_STOCKLEVEL             0
// PERC_NEWORDER is (1 - the sum of above).
#define FIRSTNAME_MINLEN 			8
#define FIRSTNAME_LEN 				16
#define LASTNAME_LEN 				16

#define DIST_PER_WARE				10
// enable user-initiated aborts in new-order txn according to TPC-C doc.
#define TPCC_USER_ABORT             true

// Optimizations used in IC3
#define COMMUTATIVE_OPS          false

/***********************************************/
// TODO centralized CC management.
/***********************************************/
#define MAX_LOCK_CNT				(20 * THREAD_CNT)
#define TSTAB_SIZE                  50 * THREAD_CNT
#define TSTAB_FREE                  TSTAB_SIZE
#define TSREQ_FREE                  4 * TSTAB_FREE
#define MVHIS_FREE                  4 * TSTAB_FREE
#define SPIN                        false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL					true
enum TestCases {
  READ_WRITE,
  CONFLICT
};
extern TestCases					g_test_case;

/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB						true
#define IDX_VERB					false
#define VERB_ALLOC					true
#define DEBUG_LOCK					false
#define DEBUG_TIMESTAMP					false
#define DEBUG_SYNTH					false
#define DEBUG_ASSERT                			false
#define DEBUG_CC					false
#define DEBUG_WW                    			false
#define DEBUG_BENCHMARK             			false
#define DEBUG_BAMBOO                			false
#define DEBUG_TMP					false

/***********************************************/
// PROFILING
/***********************************************/
#define PF_BASIC true
#define PF_CS false
#define PF_ABORT false
#define PF_MODEL          false

/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH 					1
#define IDX_BTREE					2
// WORKLOAD
#define YCSB						1
#define TPCC						2
#define TEST						3
// latch options
#define LH_SPINLOCK                   1
#define LH_MUTEX                      2
#define LH_MCSLOCK                    3
// Concurrency Control Algorithm
#define NO_WAIT						1
#define WAIT_DIE					2
#define DL_DETECT					3
#define TIMESTAMP					4
#define MVCC						5
#define HSTORE						6
#define OCC							7
#define TICTOC						8
#define SILO						9
#define VLL							10
#define HEKATON 					11
#define WOUND_WAIT                  12
#define BAMBOO                      13
#define IC3                         14
#define HOTSPOT_FRIENDLY                        15
//Isolation Levels
#define SERIALIZABLE				1
#define SNAPSHOT					2
#define REPEATABLE_READ				3
// TIMESTAMP allocation method.
#define TS_MUTEX					1
#define TS_CAS						2
#define TS_HW						3
#define TS_CLOCK					4
// Synthetic YCSB - HOTSPOT POSITION
#define TOP                         1
#define MID                         2
#define BOT                         3
#define TM                          4
#define MB                          5
#define SPECIFIED                   6

#define TEST_NUM                    2
#define TEST_INDEX                  3

#define ABORT_OPTIMIZATION          true

#define DEADLOCK_DETECTION          true
#define HOTSPOT_FRIENDLY_TIMEOUT                false
#define ABORT_WAIT_TIME             0.4

#define VERSION_CHAIN_CONTROL       false

//#define MAX_CHAIN_LENGTH            18

#define PROJECTED_ELEMENT_COUNT     5
#define FALSE_POSITIVE_PROBABILITY  0.05

#endif
