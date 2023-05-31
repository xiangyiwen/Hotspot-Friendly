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


class Access {
  public:
    access_t 	type;       // operation type
    row_t * 	data;       // real data of this tuple version[In write operation, it records the new data]

    row_t * 	orig_row;
    row_t * 	orig_data;

#if CC_ALG == HOTSPOT_FRIENDLY
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
    uint64_t            abort_cnt;          // Actually this attribute is useless because no one accesses it.

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
    ts_t volatile       timestamp;

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

    Access **		    accesses;
    int 			    num_accesses_alloc;

    // [TIMESTAMP, MVCC]
    bool volatile       ts_ready;
    // [HSTORE]
    int volatile 	    ready_part;
    // [TICTOC]
    bool                _write_copy_ptr;
#if CC_ALG == HOTSPOT_FRIENDLY
    struct dep_element{
        txn_man* dep_txn;
        uint64_t dep_txn_id;             // the txn_id of retire_txn, used in writing phase to avoid wrong semaphore--
        DepType dep_type;
    };
    typedef  tbb::concurrent_vector<dep_element> Dependency;
    uint64_t            hotspot_friendly_txn_id;
    uint64_t            hotspot_friendly_serial_id;
    uint64_t            hotspot_friendly_semaphore;

#if READ_ONLY_OPTIMIZATION_ENABLE
    // Optimization for read_only long transaction.
    bool                is_long;
    bool                read_only;
#endif

#if VERSION_CHAIN_CONTROL
    //4-3 Restrict the length of version chain.
    double            priority;           // Only the transaction itself can update the priority, so we can simply declare it a uint64_t.
#endif

    Dependency          hotspot_friendly_dependency;
#if DEADLOCK_DETECTION
    bloom_filter        hotspot_friendly_waiting_set;           // Deadlock Detection
#endif
    volatile bool       status_latch;

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
      #elif CC_ALG == HOTSPOT_FRIENDLY
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
#elif CC_ALG == HOTSPOT_FRIENDLY
    RC                  validate_hotspot_friendly(RC rc);
    void                abort_process(txn_man * txn);

    inline uint64_t 	get_hotspot_friendly_txn_id(){return this->hotspot_friendly_txn_id;}

    void PushDependency(txn_man *dep_txn, uint64_t dep_txn_id,DepType depType) {
        dep_element temp_element = {dep_txn,dep_txn_id,depType};
        hotspot_friendly_dependency.push_back(temp_element);
    }

#if DEADLOCK_DETECTION
    /* Helper Functions for waiting_set */
    // Record a txn in waiting_set
    void InsertWaitingSet(uint64_t txn_id) {
        hotspot_friendly_waiting_set.insert(txn_id);
    }

    // Judge whether an element is in waiting_set
    bool WaitingSetContains(uint64_t txn_id) {
        return hotspot_friendly_waiting_set.contains(txn_id);
    }

    // Update waiting_set
    // This should be a recursive call. Or there may be a deadlock.
    void UnionWaitingSet(const bloom_filter& wait_set){
        hotspot_friendly_waiting_set |= wait_set;

        for (auto &dep_pair: hotspot_friendly_dependency) {
            if (!dep_pair.dep_type) {                    // we may get an element before it being initialized(empty data / wrong data)
                break;
            }
            assert(dep_pair.dep_type != READ_WRITE_);

            // There's already a deadlock.
            if (WaitingSetContains(dep_pair.dep_txn->hotspot_friendly_txn_id) && dep_pair.dep_txn->status == RUNNING) {
                set_abort();
            }else {
                if (dep_pair.dep_txn->get_hotspot_friendly_txn_id() == dep_pair.dep_txn_id && dep_pair.dep_txn->status == RUNNING) {           // Don't inform the txn_manager who is already running a new txn.
                    dep_pair.dep_txn->UnionWaitingSet(hotspot_friendly_waiting_set);
                }
            }
        }
    }
#endif

    void SemaphoreAddOne() {
        ATOM_ADD(hotspot_friendly_semaphore, 1);
    }

    void SemaphoreSubOne() {
        if(hotspot_friendly_semaphore == 0) {
            // This is an aggressive subtraction, this txn_man has already started a new transaction, so semaphore shouldn't be subtracted.
        }
        else {
            auto new_val = ATOM_SUB_FETCH(hotspot_friendly_semaphore,1);
            if(new_val == UINT64_MAX) {
                ATOM_ADD(hotspot_friendly_semaphore, 1);
            }
        }
    }

    #if VERSION_CHAIN_CONTROL
        // Restrict the length of version chain.
        void PriorityAddOne() {
            priority ++;
        }
    #endif
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
