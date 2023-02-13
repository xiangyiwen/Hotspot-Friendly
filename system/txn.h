#pragma once

#include "global.h"
#include "bloom_filter.h"
#include <unordered_map>
#include "tbb/tbb.h"

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
class INDEX;
class txn_man;
#if CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
struct LockEntry;
#elif CC_ALG == BAMBOO
struct BBLockEntry;
#endif

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

/**
 * SLER: operation type, Version* pointer
 */
class Access {
  public:
    access_t 	type;       // operation type
    row_t * 	data;       // real data of this tuple version[In write operation, it records the new data]

    row_t * 	orig_row;
    row_t * 	orig_data;

#if CC_ALG == SLER
    //Version * tuple_version    [ Compile Error: Version cannot be recognized as a type ]
    void*    tuple_version;              // points to the tuple version accessed by txn
#endif
#if CC_ALG == BAMBOO
    BBLockEntry * lock_entry;
#elif CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
    LockEntry * lock_entry;
#elif CC_ALG == TICTOC
    ts_t 		wts;
    ts_t 		rts;
#elif CC_ALG == SILO
    ts_t 		tid;
    ts_t 		epoch;
#elif CC_ALG == HEKATON
    void * 	history_entry;
#elif CC_ALG == IC3
    ts_t *    tids;
    ts_t      epochs;
    uint64_t  tid;
    uint64_t  rd_accesses;
    uint64_t  wr_accesses;
    uint64_t  lk_accesses;
#endif
#if COMMUTATIVE_OPS
    // support increment-only for now
    uint64_t  com_val;
    int       com_col;
    com_t     com_op;
#endif
    void cleanup();
};

#if CC_ALG == IC3
struct TxnEntry {
    txn_man * txn;
    uint64_t txn_id;
};
#endif

class txn_man
{
  public:
    // **************************************
    // General Data Fields
    // **************************************
    // update per txn
#if LATCH == LH_MCSLOCK
    mcslock::mcs_node * mcs_node;
#endif
    thread_t *          h_thd;
    workload *          h_wl;
    txnid_t 		    txn_id;
    uint64_t            abort_cnt;

    // update per request
    row_t * volatile    cur_row;
    // not used while having no inserts
    uint64_t 		    insert_cnt;
#if INSERT_ENABLED
    row_t * 		    insert_rows[MAX_ROW_PER_TXN];
#else
    row_t *             insert_rows[1];
#endif
    // ideal: one cache line

    // **************************************
    // Individual Data Fields
    // **************************************

    // [DL_DETECT, NO_WAIT, WAIT_DIE, WOUND_WAIT, BAMBOO]
    bool volatile       lock_ready;
    bool volatile       lock_abort;         // forces another waiting txn to abort.
    status_t volatile   status;         // RUNNING, COMMITED, ABORTED, HOLDING
    #if PF_ABORT
    uint64_t            abort_chain;
    uint8_t             padding0[64 - sizeof(bool)*2 - sizeof(status_t)-
    sizeof(uint64_t)];
    #else
    uint8_t             padding0[64 - sizeof(bool)*2 - sizeof(status_t)];
    #endif
    // ideal second cache line

    // [BAMBOO]
//    #if CC_ALG != SLER
    ts_t volatile       timestamp;
//    #endif

    uint8_t             padding1[64 - sizeof(ts_t)];
    // share its own cache line since it happens tooooo frequent.
    // bamboo -- combine both status and barrier into one int.
    // low 2 bits representing status
    uint64_t volatile   commit_barriers;
    uint8_t             padding2[64 - sizeof(uint64_t)];
    //uint64_t volatile   tmp_barriers;
    //volatile uint64_t * volatile addr_barriers;
    int                 retire_threshold;

    // [BAMBOO-AUTORETIRE, OCC]
    uint64_t 		    start_ts; // bamboo: update once per txn
    // [OCC]
    uint64_t 		    end_ts;


    int 			    row_cnt;                // the count of tuples I access
    int	 		        wr_cnt;                 // the count of tuples I modify
//#if CC_ALG == SLER
//    unordered_map<uint64_t, Access*>    sler_accesses;
//#endif
    Access **		    accesses;
    int 			    num_accesses_alloc;

    // [TIMESTAMP, MVCC]
    bool volatile       ts_ready;
    // [HSTORE]
    int volatile 	    ready_part;
    // [TICTOC]
    bool                _write_copy_ptr;
#if CC_ALG == SLER
//    struct dep_element{
//        uint64_t origin_txn_id;             // the txn_id of retire_txn, used in writing phase to avoid wrong semaphore--
//        DepType dep_type;
//    };
    // typedef tbb::concurrent_unordered_map<txn_man*, dep_element> Dependency;

//    int write_row_cnt;

    struct dep_element{
        txn_man* dep_txn;
        uint64_t dep_txn_id;             // the txn_id of retire_txn, used in writing phase to avoid wrong semaphore--
        DepType dep_type;
    };
    typedef  tbb::concurrent_vector<dep_element> Dependency;   //12-12
    uint64_t            sler_txn_id;              // we can directly record txn* in retire field, this txn_id is used in waiting_set
    uint64_t            sler_serial_id;
    atomic<uint64_t>    sler_semaphore;
    //12-6 DEBUG
//    int                 uncommitted_cnt;
//    atomic<int>                 dependency_cnt;
//    vector<dep_element> dep_debug;

    Dependency          sler_dependency;
    bloom_filter        sler_waiting_set;           // Deadlock Detection

    // 12- 6 DEBUG
//    vector<dep_element> wait_list;


    //WriteSet            sler_write_set;           // since no tuple will be accessed twice in YCSB, we don't need it right now

    // make sure that status, dependency, waiting_set, semaphore is accessed exclusively
//    #if LATCH == LH_SPINLOCK
    volatile bool       status_latch;
    volatile bool       dependency_latch;
    volatile bool       waiting_set_latch;
    volatile bool       semaphore_latch;
    volatile bool       serial_id_latch;

//    #elif LATCH == LH_MUTEX
//        pthread_mutex_t * status_latch;
//        pthread_mutex_t * dependency_latch;
//        pthread_mutex_t * waiting_set_latch;
//        pthread_mutex_t * semaphore_latch;
//    #else
//        mcslock * status_latch;
//        mcslock * dependency_latch;
//        mcslock * waiting_set_latch;
//        mcslock * semaphore_latch;
//    #endif
#elif CC_ALG == TICTOC
    bool			    _atomic_timestamp;
    ts_t 			    _max_wts;
    ts_t 			    last_wts;
    ts_t 			    last_rts;
    bool 			    _pre_abort;
    bool 			    _validation_no_wait;
    // [SILO]
#elif CC_ALG == SILO
    ts_t 			    last_tid;
    ts_t 			    _cur_tid;
    bool 			    _pre_abort;
    bool 			    _validation_no_wait;
    // [IC3]
#elif CC_ALG == IC3
    TPCCTxnType         curr_type;
    volatile int        curr_piece;
    int                 access_marker;
    TxnEntry **         depqueue;
    int                 depqueue_sz;
    uint64_t            piece_starttime;
    // [HEKATON]
#elif CC_ALG == HEKATON
//    volatile void * volatile     history_entry;
    void * volatile    history_entry;
#endif

    // **************************************
    // General Main Functions
    // **************************************
    virtual void        init(thread_t * h_thd, workload * h_wl, uint64_t
    part_id);
    void                release();
    virtual RC 		    run_txn(base_query * m_query) = 0;
    RC 			        finish(RC rc);
    void 			    cleanup(RC rc);

    // **************************************
    // General Helper Functions
    // **************************************
    // getters
    uint64_t 		    get_thd_id();
    workload * 		    get_wl();
    txnid_t 		    get_txn_id();
    ts_t 			    get_ts();
    // setters
    void 			    set_txn_id(txnid_t txn_id);
    void 			    set_ts(ts_t timestamp);
    void			    reassign_ts();

    // **************************************
    // Individual Helper Functions
    // **************************************

    // [COMMUTATIVE OPERATIONS]
#if COMMUTATIVE_OPS
    void                inc_value(int col, uint64_t val);
    void                dec_value(int col, uint64_t val);
#endif
    // [WW, BAMBOO]
    // if already abort, no change, return aborted
    // if already commit, no change, return committed
    // if running, set abort, return aborted.  
    status_t   set_abort(bool cascading=false) {
      #if CC_ALG == BAMBOO
        uint64_t local = commit_barriers;
        uint64_t barriers = local >> 2;
        uint64_t s = local & 3UL;
        // cannot use atom_add:
        // (1) what if two txns both atomic add? may change from abort to commit
        // (2) moreover, may exceed two bits
        while (s == RUNNING) {
            ATOM_CAS(commit_barriers, local, (barriers << 2) + ABORTED);
            local = commit_barriers;
            barriers = local >> 2;
            s = local & 3UL;
        } 
        if (s == ABORTED) {
            if (!lock_abort)
                lock_abort = true;
           #if PF_MODEL
            if (cascading)
                INC_STATS(get_thd_id(), cascading_abort_cnt, 1);
           #endif
            return ABORTED;
        } else if (s == COMMITED) {
            return COMMITED;
        } else {
            assert(false);
            return COMMITED;
        }
      #elif CC_ALG == WOUND_WAIT || CC_ALG == IC3
       if (ATOM_CAS(status, RUNNING, ABORTED)) {
            lock_abort = true;
            return ABORTED;
       }
       return status;       // COMMITED or ABORTED
      #elif CC_ALG == SLER
        if(status == ABORTED){
            return ABORTED;
        }
        else if(status == RUNNING){
            if(ATOM_CAS(status, RUNNING, ABORTED))
                return RUNNING;          // COMMITED or ABORTED
            else
                return status;
        }
        else{           // Possible: mis-kill
//            assert(false);
            return status;
        }
      #else
        return ABORTED;
      #endif
    }

    status_t            wound_txn(txn_man * txn);
    void                increment_commit_barriers();
    void                decrement_commit_barriers();
    // dynamically set timestamp
    bool                atomic_set_ts(ts_t ts);
    ts_t			    set_next_ts(int n);
    // auto retire
    ts_t                get_exec_time() {return get_sys_clock() - start_ts;};
#if CC_ALG == BAMBOO
    RC                  retire_row(int access_cnt);
#endif
    // [VLL]
    row_t * 		    get_row(row_t * row, access_t type);
    itemid_t *	        index_read(INDEX * index, idx_key_t key, int part_id);
    void 			    index_read(INDEX * index, idx_key_t key, int part_id,
                                   itemid_t *& item);
    // [IC3]
    void                begin_piece(int piece_id);
    RC                  end_piece(int piece_id);
    void                abort_ic3();
    int                 get_txn_pieces(int tpe);

#if CC_ALG == IC3
    RC                  validate_ic3();
    // [TICTOC]
#elif CC_ALG == TICTOC
    RC				    validate_tictoc();
    ts_t 			    get_max_wts() 	{ return _max_wts; }
    void 			    update_max_wts(ts_t max_wts);
    // [Hekaton]
#elif CC_ALG == HEKATON
    RC 				    validate_hekaton(RC rc);
    // [SILO]
#elif CC_ALG == SILO
    RC				    validate_silo();
#elif CC_ALG == SLER
    RC                  validate_sler(RC rc);
    void                abort_process(txn_man * txn);

    inline uint64_t 	get_sler_txn_id(){return this->sler_txn_id;}

    /* Helper Function for dependency*/
//    bool_dep PushDependency(txn_man *dep_txn, uint64_t txn_id,DepType depType) {
//        if(sler_dependency.find(dep_txn) != sler_dependency.end()){
//            DepType current_type = sler_dependency[dep_txn].dep_type;
//            if((current_type & depType) != 0){
//                return CONTAIN_TXN_AND_TYPE;
//            }
//            else{
//                depType = DepType(depType | current_type);
//                sler_dependency[dep_txn].dep_type = depType;
//                return CONTAIN_TXN;
//            }
//        }
//        else{
//            sler_dependency[dep_txn].origin_txn_id = txn_id;
//            sler_dependency[dep_txn].dep_type = depType;
//            return NOT_CONTAIN;
//        }
//    }

    void PushDependency(txn_man *dep_txn, uint64_t dep_txn_id,DepType depType) {
        //12-6
//        dependency_cnt ++;

        dep_element temp_element = {dep_txn,dep_txn_id,depType};
        sler_dependency.push_back(temp_element);
    }

//    void PushDependency(DepType depType, txn_man * insert_txn , uint64_t insert_txn_id) {
//        dep_element temp_element = {insert_txn,insert_txn_id,depType};
////        sler_dependency.push_back(temp_element);
//
//        //12-6
//        dep_debug.push_back(temp_element);
//    }

//    void PushWaitList(txn_man *wait_txn, uint64_t wait_txn_id,DepType waitType) {
//        dep_element temp_element = {wait_txn,wait_txn_id,waitType};
//        wait_list.push_back(temp_element);
//    }

    /* Helper Functions for waiting_set */
    // Record a txn in waiting_set
    void InsertWaitingSet(uint64_t txn_id) {
        sler_waiting_set.insert(txn_id);
    }

    //judge whether an element is in waiting_set
    bool WaitingSetContains(uint64_t txn_id) {
        return sler_waiting_set.contains(txn_id);
    }

    //update waiting_set
    void UnionWaitingSet(const bloom_filter& wait_set){
        sler_waiting_set |= wait_set;
    }

    void SemaphoreAddOne() {
        //12-6
//        uncommitted_cnt++;

        sler_semaphore++;
        //Equal to semaphore.fetch_add(1);
    }

    void SemaphoreSubOne() {
        sler_semaphore--;
    }
#endif

  protected:
    void 			    insert_row(row_t * row, table_t * table);
    void                index_insert(row_t * row, INDEX * index, idx_key_t key);

  private:
    #if CC_ALG == BAMBOO || CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
    void                assign_lock_entry(Access * access);
    #endif

};


inline status_t txn_man::wound_txn(txn_man * txn)
{
#if CC_ALG == BAMBOO || CC_ALG == WOUND_WAIT
    return txn->set_abort();
#else
    return ABORTED;
#endif
}
