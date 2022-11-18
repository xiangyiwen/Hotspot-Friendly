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
    version_header->row = row;

    version_header->begin_ts = 0;
    version_header->end_ts = INF;
    // pointer must be initialized
    version_header->prev = NULL;
    version_header->next = NULL;
    version_header->retire = NULL;
    version_header->version_latch = false;

    blatch = false;

    // assign Row_sler.row_id = row._primary_key
    // set_row_id(row->get_primary_key());
}

/**
 * Read/Write a row and according to the type.
 * @param txn : the txn which accesses the row
 * @param type : operation type(can only be R_REQ, P_REQ)
 * @param row : the row in the Access Object [Empty: no data]
 * @param access : the Access Object
 * @return
 */
RC Row_sler::access(txn_man * txn, TsType type, row_t * row, Access * access){

    RC rc = RCOK;
    uint64_t txn_id = txn->get_sler_txn_id();

    while(!ATOM_CAS(blatch, false, true)){
        PAUSE
    }

    if (type == R_REQ) {
        // Note: should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets

        txn_man* retire_txn = version_header->retire;

        // committed version
        if(!retire_txn){
            rc = RCOK;
            txn->cur_row = version_header->row;             // assign current version to cur_row for future recording
            access->tuple_version = version_header;
        }
        // uncommitted version
        else{
            /**
             * Deadlock Detection
             */
            assert(retire_txn != txn);
            assert(version_header->retire_ID == retire_txn->sler_txn_id);          //11-18


            // [DeadLock]
            if(retire_txn->WaitingSetContains(txn_id)){
//                blatch = false;
                retire_txn->set_abort();

                rc = access_helper(txn,row,access,version_header);
            }
            // [No Deadlock]
            else{
                status_t temp_status = retire_txn->status;

                // [IMPOSSIBLE]: read without recording dependency
                if(temp_status == committing || temp_status == COMMITED){
                    // safe: retire_txn cannot commit without acquiring the blatch on this tuple(retire_txn need blatch to modify meta-data),
                    // which means it can commit only after I finish the operation(blatch is owned by me)

                    txn->cur_row = version_header->row;
                    access->tuple_version = version_header;
                }
                else if(temp_status == RUNNING || temp_status == validating || temp_status == writing){       // record dependency
//                    bool_dep res_dep = retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_READ_);
//
//                    if(res_dep == NOT_CONTAIN){
//                        txn->SemaphoreAddOne();
//
//                        // Update waiting set
//                        txn->UnionWaitingSet(retire_txn->sler_waiting_set);
//
//                        //更新依赖链表中所有事务的 waiting_set
//                        auto deps = txn->sler_dependency;
//                        for(auto dep_pair :deps){
//                            txn_man* txn_dep = dep_pair.first;
//
//                            txn_dep->UnionWaitingSet(txn->sler_waiting_set);
//                        }
//                    }
//                    else if(res_dep == CONTAIN_TXN){
//                        txn->SemaphoreAddOne();
//                    }
//                    else{
//                        assert(res_dep == CONTAIN_TXN_AND_TYPE);
//                    }

                    // 11-8: Simplize the logic of recording dependency ------------------------
                    txn->SemaphoreAddOne();
                    retire_txn->PushDependency(txn,txn->get_sler_txn_id(),DepType::WRITE_READ_);

                    // Update waiting set
                    txn->UnionWaitingSet(retire_txn->sler_waiting_set);

                    //更新依赖链表中所有事务的 waiting_set
                    auto deps = txn->sler_dependency;
                    for(auto dep_pair :deps){
                        txn_man* txn_dep = dep_pair.dep_txn;

                        txn_dep->UnionWaitingSet(txn->sler_waiting_set);
                    }
                    // 11-8 ---------------------------------------------------------------------


                    // Record in Access Object
                    txn->cur_row = version_header->row;
                    access->tuple_version = version_header;
                }
                else if(temp_status == ABORTED){
                    rc = access_helper(txn,row,access,version_header);
                }
            }
        }
        blatch = false;
    }
    else if (type == P_REQ) {
        assert(version_header->prev == nullptr);               // this tuple version is the newest version

        //Error Case, should not happen
        if(version_header->end_ts != INF){
            assert(false);
            blatch = false;
            rc = Abort;
        }
        else{
            // Note: should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets

            rc = RCOK;

            /**
             * Deadlock Detection
             */
            txn_man* retire_txn = version_header->retire;

            // committed version
            if(!retire_txn){
                assert(version_header->begin_ts != UINT64_MAX);
                rc = RCOK;
                // create new version & record current row in accesses
                createNewVersion(txn,access);
            }
            // uncommitted version
            else{
                if(retire_txn == txn){
                    cout << "retire txn ID: " << version_header->retire_ID << endl;
                    cout << "current txn ID: " << txn->sler_txn_id << endl;

//                    auto te = glob_manager->_all_txns;
//                    for(int i=0;i<g_thread_cnt;i++){
//                        cout << te[i] <<endl;
//                    }

                }
                assert(retire_txn != txn);
                assert(version_header->retire_ID == retire_txn->sler_txn_id);          //11-18

                // [DeadLock]
                if(retire_txn->WaitingSetContains(txn_id) && retire_txn->set_abort() == RUNNING){
                    rc = WAIT;
                    blatch = false;
//                    retire_txn->set_abort();

//                    COMPILER_BARRIER

                    return rc;
                }
                // [No Deadlock]
                else{

                    status_t temp_status = retire_txn->status;

                    //[IMPOSSIBLE]
                    if(temp_status == committing || temp_status == COMMITED){

                        // create new version & record current row in accesses
                        createNewVersion(txn,access);
                    }
                    else if(temp_status == RUNNING || temp_status == validating || temp_status == writing){       // record dependency

                        // 11-8: Simplize the logic of recording dependency ------------------------
                        txn->SemaphoreAddOne();
                        retire_txn->PushDependency(txn,txn->get_sler_txn_id(),DepType::WRITE_WRITE_);

                        // Update waiting set
                        txn->UnionWaitingSet(retire_txn->sler_waiting_set);

                        //更新依赖链表中所有事务的 waiting_set
                        auto deps = txn->sler_dependency;
                        for(auto dep_pair :deps){
                            txn_man* txn_dep = dep_pair.dep_txn;

                            txn_dep->UnionWaitingSet(txn->sler_waiting_set);
                        }
                        // 11-8 ---------------------------------------------------------------------


                        // create new version & record current row in accesses
                        createNewVersion(txn,access);
                    }
                    else if(temp_status == ABORTED){
                        rc = WAIT;
                        blatch = false;

                        return rc;
                    }
                }
            }
            blatch = false;
        }
    }
    else
        assert(false);

    return rc;
}

//RC Row_sler::access(txn_man * txn, TsType type, row_t * row, Access * access){
//
//    RC rc = RCOK;
//    uint64_t txn_id = txn->get_sler_txn_id();
//
//    if (type == R_REQ) {
//        // Note: should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets
//
//        /**
//         * Notice:
//         * there won't be any incomplete data, since all meta-data of new version are set before reallocating version_header.
//         * we can directly access the version_header(must be recorded in temp_version first).
//         */
//
//        Version* temp_version = version_header;
//
//        while(!ATOM_CAS(temp_version->version_latch, false, true)){
//            PAUSE
//        }
//
//        txn_man* retire_txn = temp_version->retire;
//
//        // committed version
//        if(!retire_txn){
//            rc = RCOK;
//            txn->cur_row = temp_version->row;             // assign current version to cur_row for future recording
//            access->tuple_version = temp_version;
//
//            temp_version->version_latch = false;
//        }
//        // uncommitted version
//        else{
//            /**
//             * Deadlock Detection
//             */
//            assert(retire_txn != txn);
//
//            // [DeadLock]
//            if(retire_txn->WaitingSetContains(txn_id)){
//                temp_version->version_latch = false;
//                retire_txn->set_abort();
//
//                rc = access_helper(txn,row,access,temp_version);
//            }
//            // [No Deadlock]
//            else{
//                status_t temp_status = retire_txn->status;
//
//                // [IMPOSSIBLE]: read without recording dependency
//                if(temp_status == committing || temp_status == COMMITED){
//                    // safe: retire_txn cannot commit without acquiring the blatch on this tuple(retire_txn need blatch to modify meta-data),
//                    // which means it can commit only after I finish the operation(blatch is owned by me)
//
//                    txn->cur_row = temp_version->row;
//                    access->tuple_version = temp_version;
//
//                    temp_version->version_latch = false;
//                }
//                else if(temp_status == RUNNING || temp_status == validating || temp_status == writing){       // record dependency
//                    bool_dep res_dep = retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_READ_);
//
//                    if(res_dep == NOT_CONTAIN){
//                        txn->SemaphoreAddOne();
//
//                        // Update waiting set
//                        txn->UnionWaitingSet(retire_txn->sler_waiting_set);
//
//                        //更新依赖链表中所有事务的 waiting_set
//                        auto deps = txn->sler_dependency;
//                        for(auto dep_pair :deps){
//                            txn_man* txn_dep = dep_pair.first;
//
//                            txn_dep->UnionWaitingSet(txn->sler_waiting_set);
//                        }
//                    }
//                    else if(res_dep == CONTAIN_TXN){
//                        txn->SemaphoreAddOne();
//                    }
//                    else{
//                        assert(res_dep == CONTAIN_TXN_AND_TYPE);
//                    }
//
//                    // Record in Access Object
//                    txn->cur_row = temp_version->row;
//                    access->tuple_version = temp_version;
//
//                    temp_version->version_latch = false;
//                }
//                else if(temp_status == ABORTED){
//                    temp_version->version_latch = false;
//                    rc = access_helper(txn,row,access,temp_version);
//                }
//            }
//
//        }
//    }
//    else if (type == P_REQ) {
//        while (!ATOM_CAS(version_header->version_latch, false, true))
//            PAUSE
//
//        assert(version_header->prev == NULL);               // this tuple version is the newest version
//
//        //Error Case, should not happen
//        if(version_header->end_ts != INF){
//            version_header->version_latch = false;
//            rc = Abort;
//        }
//        else{
//            // Note: should search write set and read set. However, since no record will be accessed twice, we don't have to search these two sets
//
//            rc = RCOK;
//
//            /**
//             * Deadlock Detection
//             */
//            txn_man* retire_txn = version_header->retire;
//
//            // committed version
//            if(!retire_txn){
//                assert(version_header->begin_ts != UINT64_MAX);
//                rc = RCOK;
//                // create new version & record current row in accesses
//                createNewVersion(txn,access);
//
////                version_header->version_latch = false;
//            }
//            // uncommitted version
//            else{
//                assert(retire_txn != txn);
//
//                // [DeadLock]
//                if(retire_txn->WaitingSetContains(txn_id)){
//                    rc = WAIT;
//                    version_header->version_latch = false;
//                    retire_txn->set_abort();
//
//                    COMPILER_BARRIER
//
//                    return rc;
//                }
//                // [No Deadlock]
//                else{
//
//                    status_t temp_status = retire_txn->status;
//
//                    //[IMPOSSIBLE]
//                    if(temp_status == committing || temp_status == COMMITED){
//
//                        // create new version & record current row in accesses
//                        createNewVersion(txn,access);
//
////                        version_header->version_latch = false;
//                    }
//                    else if(temp_status == RUNNING || temp_status == validating || temp_status == writing){       // record dependency
//                        bool_dep res_dep = retire_txn->PushDependency(txn, txn->get_sler_txn_id(),DepType::WRITE_WRITE_);
//
//                        if(res_dep == NOT_CONTAIN){
//                            // Add semaphore
//                            txn->SemaphoreAddOne();
//
//                            txn->UnionWaitingSet(retire_txn->sler_waiting_set);
//
//                            //更新依赖链表中所有事务的 waiting_set
//                            auto deps = txn->sler_dependency;
//                            for(auto dep_pair :deps){
//                                txn_man* txn_dep = dep_pair.first;
//
//                                txn_dep->UnionWaitingSet(txn->sler_waiting_set);
//                            }
//                        }
//                        else if(res_dep == CONTAIN_TXN){
//                            // Add semaphore
//                            txn->SemaphoreAddOne();
//                        }
//                        else{
//                            assert(res_dep == CONTAIN_TXN_AND_TYPE);
//                        }
//
//                        // create new version & record current row in accesses
//                        createNewVersion(txn,access);
//
////                        version_header->version_latch = false;
//                    }
//                    else if(temp_status == ABORTED){
//                        rc = WAIT;
//                        version_header->version_latch = false;
//
//                        return rc;
//                    }
//                }
//
//            }
//        }
//    }
//    else
//        assert(false);
//
//    return rc;
//}

void Row_sler::createNewVersion(txn_man * txn, Access * access){
    // create a new Version Object & row object
    Version* new_version = (Version *) _mm_malloc(sizeof(Version), 64);
    new_version->row = (row_t *) _mm_malloc(sizeof(row_t), 64);
    new_version->row->init(MAX_TUPLE_SIZE);
    new_version->prev = NULL;
    new_version->version_latch = false;

    // set the meta-data of new_version
    new_version->begin_ts = INF;
    new_version->end_ts = INF;
    new_version->retire = txn;

    new_version->retire_ID = txn->get_sler_txn_id();        //11-17

    new_version->next = version_header;
    new_version->row->copy(version_header->row);

    // update the cur_row of txn, record the object of update operation [old version]
    txn->cur_row = new_version->row;
    access->tuple_version = version_header;

    // set the meta-data of old_version
    version_header->prev = new_version;

    COMPILER_BARRIER

//    // recover the version_latch of old version
//    version_header->version_latch = false;

    // relocate version header
    version_header = new_version;
}

/**
 * Process the WAIT situation, retrieve all versions until find a visible version.
 * @param txn
 * @param type
 * @param row
 * @param access
 * @return
 */
RC Row_sler::access_helper(txn_man * txn, row_t * row, Access * access, Version* temp_version_){
    RC rc = RCOK;
    uint64_t txn_id = txn->get_sler_txn_id();
    Version* temp_version = temp_version_->next;
    txn_man* retire_txn;

    while (temp_version){
//        while(!ATOM_CAS(temp_version->version_latch, false, true)){
//            PAUSE
//        }

        retire_txn = temp_version->retire;

        // committed version
        if(!retire_txn){
            rc = RCOK;
            txn->cur_row = temp_version->row;
            access->tuple_version = temp_version;

//            temp_version->version_latch = false;
            break;
        }
        // uncommitted version
        else{
            assert(retire_txn != txn);
            assert(temp_version->retire_ID == retire_txn->sler_txn_id);          //11-18


            // [DeadLock]
            if(retire_txn->WaitingSetContains(txn_id)){
                // retrieve all versions until there is a visible version[WAIT]
//                temp_version->version_latch = false;
                retire_txn->set_abort();
                temp_version = temp_version->next;
                continue;
            }
            // [No Deadlock]
            else{

                status_t temp_status = retire_txn->status;

                // [IMPOSSIBLE]: read without recording dependency
                if(temp_status == committing || temp_status == COMMITED){
                    txn->cur_row = temp_version->row;
                    access->tuple_version = temp_version;

//                    temp_version->version_latch = false;
                    break;
                }
                else if(temp_status == RUNNING || temp_status == validating || temp_status == writing){       // record dependency
//                    bool_dep res_dep = retire_txn->PushDependency(txn, txn->get_sler_txn_id(), DepType::WRITE_READ_);
//
//                    if(res_dep == NOT_CONTAIN){
//                        txn->SemaphoreAddOne();
//
//                        // Update waiting set
//                        txn->UnionWaitingSet(retire_txn->sler_waiting_set);
//
//                        //更新依赖链表中所有事务的 waiting_set
//                        auto deps = txn->sler_dependency;
//                        for(auto dep_pair :deps){
//                            txn_man* txn_dep = dep_pair.first;
//
//                            txn_dep->UnionWaitingSet(txn->sler_waiting_set);
//                        }
//                    }
//                    else if(res_dep == CONTAIN_TXN){
//                        txn->SemaphoreAddOne();
//                    }
//                    else{
//                        assert(res_dep == CONTAIN_TXN_AND_TYPE);
//                    }

                    // 11-8: Simplize the logic of recording dependency ------------------------
                    txn->SemaphoreAddOne();
                    retire_txn->PushDependency(txn,txn->get_sler_txn_id(),DepType::WRITE_READ_);

                    // Update waiting set
                    txn->UnionWaitingSet(retire_txn->sler_waiting_set);

                    //更新依赖链表中所有事务的 waiting_set
                    auto deps = txn->sler_dependency;
                    for(auto dep_pair :deps){
                        txn_man* txn_dep = dep_pair.dep_txn;

                        txn_dep->UnionWaitingSet(txn->sler_waiting_set);
                    }
                    // 11-8 ---------------------------------------------------------------------


                    // Record in Access Object
                    txn->cur_row = temp_version->row;
                    access->tuple_version = temp_version;

//                    temp_version->version_latch = false;
                    break;
                }
                else if(temp_status == ABORTED){
                    // retrieve all versions until there is a visible version[WAIT]
//                    temp_version->version_latch = false;

                    temp_version = temp_version->next;
                    continue;
                }
            }
        }
    }

    if(!temp_version){
        rc = Abort;
    }

    return rc;
}


#endif


