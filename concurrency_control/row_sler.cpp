//
// Created by root on 2022/9/18.
//

#include "manager.h"
#include "row_sler.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == SLER

/**
 * DELETED FUNCTION
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
    version_header->version_latch = false;

    blatch = false;
}

/**
 * Read/Write a row and according to the type.
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
        // Note: should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets

        // Small optimization for read-intensive workload: if the version_header is a committed version, then read don't have to lock it.
        Version* temp_version = version_header;
        if(!temp_version->retire){
            rc = RCOK;

            access->tuple_version = temp_version;
            return rc;
        }
        else {
            if(temp_version->retire->status == committing || temp_version->retire->status == COMMITED){
                access->tuple_version = temp_version;
                return rc;
            }
        }

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
                    version_header = version_header->next;         //11-22

                    assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                    continue;
                }
                // [No Deadlock]
                else {

                    status_t temp_status = retire_txn->status;

                    // [IMPOSSIBLE]: read without recording dependency
                    if (temp_status == committing || temp_status == COMMITED) {
                        // safe: retire_txn cannot commit without acquiring the blatch on this tuple(retire_txn need blatch to modify meta-data),
                        // which means it can commit only after I finish the operation(blatch is owned by me)

                        access->tuple_version = version_header;
                        break;
                    } else if (temp_status == RUNNING || temp_status == validating || temp_status == writing) {       // record dependency

                        // 11-8: Simplize the logic of recording dependency ------------------------
                        txn->SemaphoreAddOne();
                        retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_READ_);

                        // Update waiting set
                        txn->UnionWaitingSet(retire_txn->sler_waiting_set);

                        //更新依赖链表中所有事务的 waiting_set
                        auto deps = txn->sler_dependency;
                        for (auto dep_pair: deps) {
                            txn_man *txn_dep = dep_pair.dep_txn;

                            txn_dep->UnionWaitingSet(txn->sler_waiting_set);
                        }
                        // 11-8 ---------------------------------------------------------------------


                        // Record in Access Object
                        access->tuple_version = version_header;
                        break;
                    } else if (temp_status == ABORTED) {

                        version_header = version_header->next;
                        assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                        continue;
                    }
                }
            }
        }

        if(!version_header){
            assert(false);
            rc = Abort;
        }

        blatch = false;
    }
    else if (type == P_REQ) {
        while(!ATOM_CAS(blatch, false, true)){
            PAUSE
        }

//        assert(version_header->prev == nullptr);               // this tuple version is the newest version

        while (version_header) {

            assert(version_header->end_ts == INF);

            retire_txn = version_header->retire;

            //Error Case, should not happen
            if (version_header->end_ts != INF) {
                assert(false);
                blatch = false;
                rc = Abort;
                break;
            } else {
                // Note: should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets

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

                        version_header = version_header->next;
                        assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                        continue;
                    }
                    // [No Deadlock]
                    else {

                        status_t temp_status = retire_txn->status;

                        //[IMPOSSIBLE]
                        if (temp_status == committing || temp_status == COMMITED) {

                            // create new version & record current row in accesses
                            createNewVersion(txn, access);
                            break;
                        } else if (temp_status == RUNNING || temp_status == validating ||
                                   temp_status == writing) {       // record dependency

                            // 11-8: Simplize the logic of recording dependency ------------------------
                            txn->SemaphoreAddOne();
                            retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_WRITE_);

                            // Update waiting set
                            txn->UnionWaitingSet(retire_txn->sler_waiting_set);

                            //更新依赖链表中所有事务的 waiting_set
                            auto deps = txn->sler_dependency;
                            for (auto dep_pair: deps) {
                                txn_man *txn_dep = dep_pair.dep_txn;

                                txn_dep->UnionWaitingSet(txn->sler_waiting_set);
                            }
                            // 11-8 ---------------------------------------------------------------------

                            // create new version & record current row in accesses
                            createNewVersion(txn, access);
                            break;
                        } else if (temp_status == ABORTED) {

                            version_header = version_header->next;
                            assert(version_header->end_ts == INF && retire_txn->status == ABORTED);
                            continue;
                        }
                    }
                }
            }
        }

        if(!version_header){
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
    new_version->version_latch = false;

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
}



#endif


