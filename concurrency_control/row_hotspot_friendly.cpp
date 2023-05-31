//
// Created by root on 2022/9/18.
//

#include "manager.h"
#include "row_hotspot_friendly.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == HOTSPOT_FRIENDLY


/**
 * Initialize a row in HOTSPOT_FRIENDLY
 * @param row : pointer of this row(the first version) [Empty: no data]
 */
void Row_hotspot_friendly::init(row_t *row){
    // initialize version header
    version_header = (Version *) _mm_malloc(sizeof(Version), 64);

    version_header->begin_ts = 0;
    version_header->end_ts = INF;

    // pointer must be initialized
    version_header->prev = NULL;
    version_header->next = NULL;
    version_header->retire = NULL;
    version_header->retire_ID = 0;

#ifdef ABORT_OPTIMIZATION
    version_header->version_number = 0;
#endif

#if VERSION_CHAIN_CONTROL
    // Restrict the length of version chain.
    threshold = 0;
#endif

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
RC Row_hotspot_friendly::access(txn_man * txn, TsType type, Access * access){

    // Optimization for read_only long transaction.
#if READ_ONLY_OPTIMIZATION_ENABLE
    if(txn->is_long && txn->read_only){
        while(!ATOM_CAS(blatch, false, true)){
            PAUSE
        }

        Version* read_only_version = version_header;
        while (read_only_version){
            if(read_only_version->begin_ts != UINT64_MAX){
                assert(read_only_version->retire == NULL);
                access->tuple_version = read_only_version;
                break;
            }
            read_only_version = read_only_version->next;
        }

        blatch = false;
        return RCOK;
    }
#endif

    RC rc = RCOK;
    uint64_t txn_id = txn->get_hotspot_friendly_txn_id();
    txn_man* retire_txn;

    if (type == R_REQ) {

        // This optimization is only available to read-only YCSB workload.
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
                            assert(temp_version->begin_ts != UINT64_MAX);
                            return rc;
                        }
                    }
                }
            #endif
        #endif

#if VERSION_CHAIN_CONTROL
        // Restrict the length of version chain. [Judge the priority of txn and threshold of tuple first.]
        uint64_t starttime_read = get_sys_clock();
        double timeout_span = 0.1*1000000UL;
        while (txn->priority < (double)threshold && (double)(get_sys_clock() - starttime_read) < 0.1*1000000UL){
            PAUSE
        }
#endif

        while(!ATOM_CAS(blatch, false, true)){
            PAUSE
        }

        while (version_header) {
            assert(version_header->end_ts == INF);

            retire_txn = version_header->retire;

            // committed version
            if (!retire_txn) {
                rc = RCOK;

                access->tuple_version = version_header;
                assert(version_header->begin_ts != UINT64_MAX);
                break;
            }
            // uncommitted version
            else {
                assert(retire_txn != txn);
                assert(version_header->retire_ID == retire_txn->hotspot_friendly_txn_id);

#if DEADLOCK_DETECTION
                // [DeadLock]
                if (retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING) {
                    version_header = version_header->next;

    #ifdef ABORT_OPTIMIZATION
                    // Recursively update the chain_number of uncommitted old version and the first committed verison.
                    Version* version_retrieve = version_header;

                    while(version_retrieve->begin_ts == UINT64_MAX){
                        assert(version_retrieve->retire != NULL);

                        version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                        version_retrieve = version_retrieve->next;
                    }

                    // Update the chain-number of the first committed version.
                    assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL && version_retrieve->retire_ID == 0);
                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
    #endif

                    assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                    continue;
                }
                // [No Deadlock]
                else {
#endif

                    status_t temp_status = retire_txn->status;

                    //[IMPOSSIBLE]
                    /* committing and COMMITTED means version_header.retire == NULL right now, which means retire_txn acquires the blatch.
                     * This is impossible because I acquire the blatch in line 305.
                     * Retire_txn has no chance to acquire the blatch and update the version_header.retire == NULL.
                     */
                    if (temp_status == committing || temp_status == COMMITED) {
                        access->tuple_version = version_header;
                        assert(version_header->begin_ts != UINT64_MAX);
                        assert(false);
                        break;
                    } else if (temp_status == RUNNING || temp_status == validating || temp_status == writing) {       // record dependency

                        txn->SemaphoreAddOne();
                        retire_txn->PushDependency(txn, txn->get_hotspot_friendly_txn_id(), DepType::WRITE_READ_);

                        if(temp_status == RUNNING) {

#if DEADLOCK_DETECTION
                            // Update waiting set
                            txn->UnionWaitingSet(retire_txn->hotspot_friendly_waiting_set);
#endif

                            if(txn->status == ABORTED){
                                rc = Abort;
                                break;
                            }
                        }

                        // Record in Access Object
                        access->tuple_version = version_header;
                        break;

                    } else if (temp_status == ABORTED) {

                        version_header = version_header->next;

#ifdef ABORT_OPTIMIZATION
                        // Recursively update the chain_number of uncommitted old version and the first committed verison.
                        Version* version_retrieve = version_header;

                        while(version_retrieve->begin_ts == UINT64_MAX){
                            assert(version_retrieve->retire != NULL);

                            version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                            version_retrieve = version_retrieve->next;
                        }

                        // Update the chain-number of the first committed version.
                        assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL && version_retrieve->retire_ID == 0);
                        version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif

                        assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                        continue;
                    }
#if DEADLOCK_DETECTION
                }
#endif
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
    #if VERSION_CHAIN_CONTROL
        // Restrict the length of version chain. [Judge the priority of txn and threshold of tuple first.]
        uint64_t starttime_write = get_sys_clock();
        double timeout_span = 0.1*1000000UL;
        while (txn->priority < (double)threshold && (double)(get_sys_clock() - starttime_write) < 0.1*1000000UL){
            PAUSE
        }
    #endif

        while(!ATOM_CAS(blatch, false, true)){
            PAUSE
        }

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
                    assert(retire_txn != txn);
                    assert(version_header->retire_ID == retire_txn->hotspot_friendly_txn_id);

#if DEADLOCK_DETECTION

                    // [DeadLock]
                    if (retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING) {
                        version_header = version_header->next;

#ifdef ABORT_OPTIMIZATION
                        // Recursively update the chain_number of uncommitted old version and the first committed verison.
                        Version* version_retrieve = version_header;

                        while(version_retrieve->begin_ts == UINT64_MAX){
                            assert(version_retrieve->retire != NULL);

                            version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                            version_retrieve = version_retrieve->next;
                        }

                        // Update the chain-number of the first committed version.
                        assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL && version_retrieve->retire_ID == 0);
                        version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif

                        assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                        continue;
                    }
                    // [No Deadlock]
                    else {
#endif

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
                            break;
                        } else if (temp_status == RUNNING || temp_status == validating || temp_status == writing) {       // record dependency
                            txn->SemaphoreAddOne();
                            retire_txn->PushDependency(txn, txn->get_hotspot_friendly_txn_id(), DepType::WRITE_WRITE_);

                            // Avoid unnecessary data update.
                            if(temp_status == RUNNING) {

#if DEADLOCK_DETECTION

                                // Update waiting set
                                txn->UnionWaitingSet(retire_txn->hotspot_friendly_waiting_set);
#endif

                                if(txn->status == ABORTED){
                                    rc = Abort;
                                    break;
                                }
                            }

                            // create new version & record current row in accesses
                            createNewVersion(txn, access);
                            break;

                        } else if (temp_status == ABORTED) {
                            version_header = version_header->next;

#ifdef ABORT_OPTIMIZATION
                            // Recursively update the chain_number of uncommitted old version and the first committed verison.
                            Version* version_retrieve = version_header;

                            while(version_retrieve->begin_ts == UINT64_MAX){
                                assert(version_retrieve->retire != NULL);

                                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                                version_retrieve = version_retrieve->next;
                            }

                            // Update the chain-number of the first committed version.
                            assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL && version_retrieve->retire_ID == 0);
                            version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
#endif

                            assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                            continue;
                        }
#if DEADLOCK_DETECTION
                    }
#endif
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

void Row_hotspot_friendly::createNewVersion(txn_man * txn, Access * access){
    // create a new Version Object & row object
    Version* new_version = (Version *) _mm_malloc(sizeof(Version), 64);
    new_version->prev = NULL;
    new_version->begin_ts = INF;
    new_version->end_ts = INF;
    new_version->retire = txn;

    new_version->retire_ID = txn->get_hotspot_friendly_txn_id();

#ifdef ABORT_OPTIMIZATION
    new_version->version_number = version_header->version_number + 1;
#endif

#if VERSION_CHAIN_CONTROL
    // Restrict the length of version chain. [Update the count of uncommitted versions.]
    IncreaseThreshold();
    txn->PriorityAddOne();
#endif

    new_version->next = version_header;

    // update the cur_row of txn, record the object of update operation [new version]
    access->tuple_version = new_version;

    // set the meta-data of old_version
    version_header->prev = new_version;

    // relocate version header
    version_header = new_version;
    assert(version_header->end_ts == INF);
}


#endif


