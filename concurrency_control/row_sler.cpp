//
// Created by root on 2022/9/18.
//

#include "manager.h"
#include "row_sler.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == SLER

/**
 * OBSOLETE FUNCTION
 * we can directly use row_t.primary_key.
 * @param _row_id
 */
/*
void Row_sler::set_row_id(uint64_t _row_id){
   this->row_id = _row_id;
}
*/


/**
 * Initialize a row in SLER
 * @param row : pointer of this row(the first version) [Empty: no data]
 */
/*
 * USAGE:
 * 1. init_table() of test_wl,tpcc_wl and ycsb_wl: these functions are only processed
 *    in data generating phase(insert tuples to initialize relevant tables),
 *    so it's safe and correct to set begin_ts = 0.
 * 2. run_payment() of tpcc_txn.cpp: this function runs Payment which needs to insert a new tuple in HISTORY table.
 *    However, it doesn't really insert this new tuple in HISTORY, so the value of begin_ts doesn't matter.
 *    Once this tuple is really inserted, DBx1000 must record it in Accesses and set begin_ts = INF.
 */
void Row_sler::init(row_t *row){
    // initialize version header
    version_header = (Version *) _mm_malloc(sizeof(Version), 64);

    version_header->begin_ts = 0;
    version_header->end_ts = INF;

    // pointer must be initialized
    version_header->prev = NULL;
    version_header->next = NULL;
    version_header->retire = NULL;
    version_header->retire_ID = 0;          //11-17
//    version_header->version_latch = false;

    blatch = false;
}

/**
 * Read/Write a row according to the type.
 * @param txn : the txn which accesses the row
 * @param type : operation type(can only be R_REQ, P_REQ)
 * @param row : the row in the Access Object [Empty: no data]
 * @param access : the Access Object
 * @return
 */
RC Row_sler::access(txn_man * txn, TsType type, Access * access){

    RC rc = RCOK;
    uint64_t txn_id = txn->get_sler_txn_id();
    txn_man* retire_txn;

//    while(!ATOM_CAS(blatch, false, true)){
//        PAUSE
//    }

    if (type == R_REQ) {
        // Todo: Should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets

        // Small optimization for read-intensive workload: if the version_header is a committed version, then read don't have to lock it.
        // Optimization - v1: [BUG]
        /*
//        Version* temp_version = version_header;
//        if(!temp_version->retire){
//            rc = RCOK;
//
//            access->tuple_version = temp_version;
//            return rc;
//        }
//        else {
//            // 11-29 : [BUG] retire my be NULL
//            if(temp_version->retire->status == committing || temp_version->retire->status == COMMITED){
//                access->tuple_version = temp_version;
//                return rc;
//            }
//        }
         */

        // Optimization - v2: [BUG]'else' branch may cause deadlock or other error, because temp_version may be already aborted and temp_retire_txn has already started another txn and committing/committed, we cannot know whether this temp_version is aborted / committed just through the status of temp_retire_txn.
        /*
//        Version* temp_version = version_header;
//        auto temp_retire_txn = temp_version->retire;
//        if(!temp_retire_txn){           // committed version
//            rc = RCOK;
//
//            access->tuple_version = temp_version;
//            return rc;
//        }
//        else{           //uncommitted version
//            if(temp_retire_txn->status == committing || temp_retire_txn->status == COMMITED){
//                access->tuple_version = temp_version;
//                assert(temp_version->begin_ts != UINT64_MAX);   //12-6 Debug
//                return rc;
//            }
//        }
         */

        // Optional Optimization - v3:
        //todo: it sill occasionally causes deadlock(continuous "You should wait") [Wait to Fix]
        //todo: Let this optimization only available to read-only YCSB workload.
        #if WORKLOAD == YCSB
            // This optimization isn't suitable for synthetic YCSB.
            #if !SYNTHETIC_YCSB
        if(g_read_perc == 1){
            Version *temp_version = version_header;
            auto temp_retire_txn = temp_version->retire;
            if (!temp_retire_txn) {           // committed version
                rc = RCOK;

                access->tuple_version = temp_version;
                return rc;
            } else {           //uncommitted version
                if (temp_retire_txn->status == committing || temp_retire_txn->status == COMMITED) {
                    access->tuple_version = temp_version;
                    assert(temp_version->begin_ts != UINT64_MAX);   //12-6 Debug
                    return rc;
                }
            }
        }
            #endif
        #endif


        while(!ATOM_CAS(blatch, false, true)){
            PAUSE
        }

        while (version_header ) {
            assert(version_header->end_ts == INF);

            retire_txn = version_header->retire;

            // committed version
            if (!retire_txn) {
                rc = RCOK;

                access->tuple_version = version_header;
                assert(version_header->begin_ts != UINT64_MAX);   //12-6 Debug
                break;
            }
            // uncommitted version
            else {
                /**
                 * Deadlock Detection
                 */
                assert(retire_txn != txn);
                assert(version_header->retire_ID == retire_txn->sler_txn_id);          //11-18

                // [DeadLock]
                if (retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING) {

                    // 12-5 DEBUG
                    /*
//                    auto dep_size = txn->sler_dependency.size();
//                    uint64_t temp_sem = retire_txn->sler_semaphore;
//                    assert(dep_size != 0 && temp_sem != 0);
                    */

                    version_header = version_header->next;         //11-22

                    assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                    continue;
                }
                // [No Deadlock]
                else {

                    status_t temp_status = retire_txn->status;


                    //[IMPOSSIBLE]
                    /* committing and COMMITTED means version_header.retire == NULL right now, which means retire_txn acquires the blatch.
                     * This is impossible because I acquire the blatch in line 305.
                     * Retire_txn has no chance to acquire the blatch and update the version_header.retire == NULL.
                     */
                    if (temp_status == committing || temp_status == COMMITED) {
                        // Safe: retire_txn cannot commit without acquiring the blatch on this tuple(retire_txn need blatch to modify meta-data),
                        // which means it can commit only after I finish the operation(blatch is owned by me)

                        access->tuple_version = version_header;
                        assert(version_header->begin_ts != UINT64_MAX);         //12-6 Debug
                        assert(false);
                        break;
                    } else if (temp_status == RUNNING || temp_status == validating || temp_status == writing) {       // record dependency

                        // 11-8: Simplize the logic of recording dependency ------------------------
                        txn->SemaphoreAddOne();
//                        //12-6 Debug
//                        txn->PushWaitList(retire_txn,retire_txn->get_sler_txn_id(),DepType::WRITE_READ_);
                        retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_READ_);

                        // 2-26 : Avoid unnecessary data update.
                        if(temp_status == RUNNING) {

                            // Update waiting set
                            txn->UnionWaitingSet(retire_txn->sler_waiting_set);

                            //更新依赖链表中所有事务的 waiting_set
                            auto deps = txn->sler_dependency;
                            for (auto &dep_pair: txn->sler_dependency) {
                                if (!dep_pair.dep_type) {                    // we may get an element before it being initialized(empty data / wrong data)
                                    break;
                                }
                                assert(dep_pair.dep_type != READ_WRITE_);

                                if (dep_pair.dep_txn->get_sler_txn_id() ==
                                    dep_pair.dep_txn_id) {           // Don't inform the txn_manager who is already running a new txn.
                                    dep_pair.dep_txn->UnionWaitingSet(txn->sler_waiting_set);
                                }
                            }
                        }
                        // 11-8 ---------------------------------------------------------------------

                        // Record in Access Object
                        access->tuple_version = version_header;
                        break;

                        // 12-5 Optional Optimization: Twice check the deadlock, alleviate the cost of aborting a txn.(the earlier we detect a deadlock, the less resources we'll waste on aborting a txn)
                        /*
//                        // 12-5 DEBUG
//                        auto txn_dep_size = txn->sler_dependency.size();
//                        uint64_t temp_sem = retire_txn->sler_semaphore;
//                        if(txn_dep_size !=0 || temp_sem != 0){
//                            uint64_t txn_sem = txn->sler_semaphore;
//                            auto retire_dep_size = retire_txn->sler_dependency.size();
//                            auto res = retire_txn->WaitingSetContains(txn_id);
//                            printf("Read ERROR\n");
//                        }

//                        // 12-5 TRY
//                        if(retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING){
//                            version_header = version_header->next;         //11-22
//
//                            assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
//                            continue;
//                        }
//                        else{
//                            // 11-8: Simplize the logic of recording dependency ------------------------
//                            txn->SemaphoreAddOne();
//                            //12-6 Debug
//                            txn->PushWaitList(retire_txn,retire_txn->get_sler_txn_id(),DepType::WRITE_READ_);
//
//                            retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_READ_);
//
//                            // Update waiting set
//                            txn->UnionWaitingSet(retire_txn->sler_waiting_set);
//
////                            // retire txn access a tuple I updated(different from current tuple) amd depend on me now [DEADLOCK]
////                            if(retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING){
////                                txn->SemaphoreSubOne();
////                                version_header = version_header->next;         //11-22
////
////                                assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
////                                continue;
////                            }
//
//                            //更新依赖链表中所有事务的 waiting_set
//                            auto deps = txn->sler_dependency;
//                            for (auto dep_pair: deps) {
//                                txn_man *txn_dep = dep_pair.dep_txn;
//                                uint64_t origin_txn_id = dep_pair.dep_txn_id;
//
//                                if(txn_dep->get_sler_txn_id() == origin_txn_id) {           // Don't inform the txn_manager who is already running a new txn.
//                                    txn_dep->UnionWaitingSet(txn->sler_waiting_set);
//                                }
//                            }
//                            // 11-8 ---------------------------------------------------------------------
//
//                            // retire txn access a tuple I updated(different from current tuple) and depend on me now [DEADLOCK]
//                            if(retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING){
//                                txn->SemaphoreSubOne();
//                                version_header = version_header->next;         //11-22
//
//                                assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
//                                continue;
//                            }
//
//                            // Record in Access Object
//                            access->tuple_version = version_header;
//                            break;
//                        }
                        */

                    } else if (temp_status == ABORTED) {

                        version_header = version_header->next;
                        assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                        continue;
                    }
                }
            }
        }

        // [Impossible]
        if(!version_header ){
            assert(version_header);
            rc = Abort;
        }

        blatch = false;
    }
    else if (type == P_REQ) {
        while(!ATOM_CAS(blatch, false, true)){
            PAUSE
        }

        /*
         * [DELETED ASSERTION]
         * version_header->prev maynot be NULL, because read and write operation will change version_header
         * when there's a deadlock / access an aborted version without collecting the obsolete version and reset the prev pointer.
         */
//        assert(version_header->prev == nullptr);               // this tuple version is the newest version

        while (version_header) {
            assert(version_header->end_ts == INF);

            retire_txn = version_header->retire;

            // [Error Case]: should not happen
            if (version_header->end_ts != INF) {
                assert(false);
                blatch = false;
                rc = Abort;
                break;
            } else {
                // Todo: should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets

                rc = RCOK;

                /**
                 * Deadlock Detection
                 */

                // committed version
                if (!retire_txn) {
                    assert(version_header->begin_ts != UINT64_MAX);
                    rc = RCOK;
                    // create new version & record current row in accesses
                    createNewVersion(txn, access);
                    break;
                }
                // uncommitted version
                else {
//                if(retire_txn == txn){
//                    cout << "retire txn ID: " << version_header->retire_ID << endl;
//                    cout << "current txn ID: " << txn->sler_txn_id << endl;
//                }
                    assert(retire_txn != txn);
                    assert(version_header->retire_ID == retire_txn->sler_txn_id);          //11-18

                    // [DeadLock]
                    if (retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING) {

                        // 12-5 DEBUG
                        /*
    //                    auto dep_size = txn->sler_dependency.size();
    //                    uint64_t temp_sem = retire_txn->sler_semaphore;
    //                    assert(dep_size != 0 && temp_sem != 0);
                        */

                        version_header = version_header->next;
                        assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                        continue;
                    }
                    // [No Deadlock]
                    else {

                        status_t temp_status = retire_txn->status;

                        //[IMPOSSIBLE]
                        /* committing and COMMITTED means version_header.retire == NULL right now, which means retire_txn acquires the blatch.
                         * This is impossible because I acquire the blatch in line 305.
                         * Retire_txn has no chance to acquire the blatch and update the version_header.retire == NULL.
                         */
                        if (temp_status == committing || temp_status == COMMITED) {

                            // create new version & record current row in accesses
                            createNewVersion(txn, access);
                            assert(false);
//                            assert(version_header->begin_ts != UINT64_MAX);         //12-6 Debug
                            break;
                        } else if (temp_status == RUNNING || temp_status == validating || temp_status == writing) {       // record dependency

                            // 12-5 DEBUG
                            /*
//                            auto txn_dep_size = txn->sler_dependency.size();
//                            uint64_t temp_sem = retire_txn->sler_semaphore;
//                            if(txn_dep_size !=0 || temp_sem != 0){
//                                uint64_t txn_sem = txn->sler_semaphore;
//                                auto retire_dep_size = retire_txn->sler_dependency.size();
//                                auto res = retire_txn->WaitingSetContains(txn_id);
//                                printf("Write ERROR\n");
//                            }
                            */

                            // 11-8: Simplize the logic of recording dependency ------------------------
                            txn->SemaphoreAddOne();
                            //12-6 Debug
//                            txn->PushWaitList(retire_txn,retire_txn->get_sler_txn_id(),DepType::WRITE_WRITE_);
                            retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_WRITE_);

                            // 2-26 : Avoid unnecessary data update.
                            if(temp_status == RUNNING) {

                                // Update waiting set
                                txn->UnionWaitingSet(retire_txn->sler_waiting_set);

                                //更新依赖链表中所有事务的 waiting_set
                                auto deps = txn->sler_dependency;
                                for (auto &dep_pair: txn->sler_dependency) {
                                    if (!dep_pair.dep_type) {                    // we may get an element before it being initialized(empty data / wrong data)
                                        break;
                                    }
                                    assert(dep_pair.dep_type != READ_WRITE_);

                                    if (dep_pair.dep_txn->get_sler_txn_id() ==
                                        dep_pair.dep_txn_id) {           // Don't inform the txn_manager who is already running a new txn.
                                        dep_pair.dep_txn->UnionWaitingSet(txn->sler_waiting_set);
                                    }
                                }
                            }
                            // 11-8 ---------------------------------------------------------------------

                            // create new version & record current row in accesses
                            createNewVersion(txn, access);
                            break;

                            // 12-5 Optional Optimization: Twice check the deadlock, alleviate the cost of aborting a txn.(the earlier we detect a deadlock, the less resources we'll waste on aborting a txn)
                            /*
//                            // 12-5 TRY
//                            if(retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING ){
//                                version_header = version_header->next;         //11-22
//
//                                assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
////                                i++;
//                                continue;
//                            }
//                            else {
//                                // 11-8: Simplize the logic of recording dependency ------------------------
//                                txn->SemaphoreAddOne();
//                                //12-6 Debug
//                                txn->PushWaitList(retire_txn,retire_txn->get_sler_txn_id(),DepType::WRITE_WRITE_);
//
//                                retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_WRITE_);
//
//                                // Update waiting set
//                                txn->UnionWaitingSet(retire_txn->sler_waiting_set);
//
////                                // retire txn access a tuple I updated(different from current tuple) amd depend on me now [DEADLOCK]
////                                if(retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING){
////                                    txn->SemaphoreSubOne();
////                                    version_header = version_header->next;         //11-22
////
////                                    assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
////                                    i++;
////                                    continue;
////                                }
//
//                                //更新依赖链表中所有事务的 waiting_set
//                                auto deps = txn->sler_dependency;
//                                for (auto dep_pair: deps) {
//                                    txn_man *txn_dep = dep_pair.dep_txn;
//                                    uint64_t origin_txn_id = dep_pair.dep_txn_id;
//
//                                    if(txn_dep->get_sler_txn_id() == origin_txn_id) {           // Don't inform the txn_manager who is already running a new txn.
//                                        txn_dep->UnionWaitingSet(txn->sler_waiting_set);
//                                    }
//                                }
//                                // 11-8 ---------------------------------------------------------------------
//
//
//                                // retire txn access a tuple I updated(different from current tuple) amd depend on me now [DEADLOCK]
//                                if(retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING){
//                                    txn->SemaphoreSubOne();
//                                    version_header = version_header->next;         //11-22
//
//                                    assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
////                                    i++;
//                                    continue;
//                                }
//
//                                // create new version & record current row in accesses
//                                createNewVersion(txn, access);
//                                break;
//                            }
                            */


                        } else if (temp_status == ABORTED) {
                            version_header = version_header->next;
                            assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                            continue;
                        }
                    }
                }
            }
        }

        // [Impossible]
        if(!version_header){
            assert(version_header);
            rc = Abort;
        }

        blatch = false;
    }
    else
        assert(false);

    return rc;
}

void Row_sler::createNewVersion(txn_man * txn, Access * access){
    // create a new Version Object & row object
    Version* new_version = (Version *) _mm_malloc(sizeof(Version), 64);

//    new_version->row = (row_t *) _mm_malloc(sizeof(row_t), 64);
//    new_version->row->init(MAX_TUPLE_SIZE);

    new_version->prev = NULL;
//    new_version->version_latch = false;

    // set the meta-data of new_version
    new_version->begin_ts = INF;
    new_version->end_ts = INF;
    new_version->retire = txn;

    new_version->retire_ID = txn->get_sler_txn_id();        //11-17

    new_version->next = version_header;
//    new_version->row->copy(version_header->row);
//    new_version->row = version_header->row;


    // update the cur_row of txn, record the object of update operation [new version]
    access->tuple_version = new_version;

    // set the meta-data of old_version
    version_header->prev = new_version;

    // relocate version header
    version_header = new_version;
    assert(version_header->end_ts == INF);
}



#endif


