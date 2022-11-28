//
// Created by root on 2022/9/22.
//
#include "txn.h"
#include "row.h"
#include "row_sler.h"
#include "manager.h"
#include <mm_malloc.h>


#if CC_ALG == SLER

RC txn_man::validate_sler(RC rc) {
    uint64_t starttime = get_sys_clock();
    while(true){
        uint64_t span = get_sys_clock() - starttime;
        if(span > 1000000){
            printf("txn_id:%lu,validate_time: %lu\n",sler_txn_id,span);
        }

        // Abort myself actively
        if(status == ABORTED || rc == Abort){
            abort_process(this);
            return Abort;
        }
        if(sler_semaphore == 0){
            break;
        }
    }

    /**
     * Update status.
     */
//    while(!ATOM_CAS(status_latch, false, true))
//        PAUSE
//    // Abort myself actively  [this shouldn't happen]
//    if(status == ABORTED){
//        status_latch = false;
//        abort_process(this);
//        return Abort;
//    }
//    else if(status == RUNNING){
//        status = validating;
//    }
//    else
//        assert(false);
//    status_latch = false;

    // Abort myself actively
    if(status == ABORTED){
        abort_process(this);
        return Abort;
    }
    else if(status == RUNNING){
        int i = 0;
        if(!ATOM_CAS(status, RUNNING, validating)) {
            abort_process(this);
            return Abort;
        }
    }
    else {
        assert(status == ABORTED);
        abort_process(this);
        return Abort;
    }


    /**
     * Validate the read & write set
     */
    uint64_t min_next_begin = UINT64_MAX;
    uint64_t serial_id = 0;

    // separate write set from accesses       11-28
    int write_set[wr_cnt];
    int cur_wr_idx = 0;

    for(int rid = 0; rid < row_cnt; rid++){

        // Caculate serial_ID
        Version* current_version = (Version*)accesses[rid]->tuple_version;

        if(accesses[rid]->type == WR) {          // we record the new version in read_write_set 11-22
            current_version = current_version->next;
        }

        if(serial_id <= current_version->begin_ts) {

            while(current_version->begin_ts == UINT64_MAX){
                cout << sler_txn_id << " You should wait!" << endl;
            }

            serial_id = current_version->begin_ts + 1;
        }

        if(accesses[rid]->type == WR){
            write_set[cur_wr_idx ++] = rid;         // 11-28
            continue;
        }

        // Check RW dependency
        Version* newer_version = current_version->prev;
        if(newer_version){

            txn_man* newer_version_txn = newer_version->retire;
            // New version is uncommitted
            if(newer_version_txn){

                while(!ATOM_CAS(newer_version_txn->status_latch, false, true)){
                    PAUSE
                }
                status_t temp_status = newer_version_txn->status;

                if(temp_status == RUNNING){
                    if(newer_version->begin_ts != UINT64_MAX){
                        assert(newer_version->retire == nullptr);
                        min_next_begin = std::min(min_next_begin,newer_version->begin_ts);
                    }
                    else{
                        assert(newer_version->begin_ts == UINT64_MAX);

                        // Record RW dependency
                        newer_version_txn->SemaphoreAddOne();
                        PushDependency(newer_version_txn,newer_version_txn->get_sler_txn_id(),DepType::READ_WRITE_);

//                    // Update waiting set
//                    newer_version_txn->UnionWaitingSet(sler_waiting_set);
//
//                    //更新依赖链表中所有事务的 waiting_set
//                    auto deps = newer_version_txn->sler_dependency;
//                    for(auto dep_pair :deps){
//                        txn_man* txn_dep = dep_pair.dep_txn;
//
//                        txn_dep->UnionWaitingSet(newer_version_txn->sler_waiting_set);
//                    }
                        // 11-8 ---------------------------------------------------------------------
                    }
                }
                else if(temp_status == writing || temp_status == committing || temp_status == COMMITED){
                    // Treat next tuple version as committed(Do nothing here)
                    min_next_begin = std::min(min_next_begin,newer_version_txn->sler_serial_id);
                }
                else if(temp_status == validating){
                    newer_version_txn->status_latch = false;
                    abort_process(this);
                    return Abort;

                    //Todo: maybe we can wait until newer_version_txn commit/abort,but how can it inform me before that thread start next txn
                    /*
                    // abort in advance
                    if(serial_id >= std::min(min_next_begin,newer_version_txn->sler_serial_id)){
                        abort_process(this);
                        return Abort;
                    }
                     // wait until newer_version_txn commit / abort
                     */
                }
                else if(temp_status == ABORTED){
                    newer_version_txn->status_latch = false;
                    continue;
                }

                newer_version_txn->status_latch = false;
            }
            // new version is committed
            else{
                min_next_begin = std::min(min_next_begin,newer_version->begin_ts);
            }

            if(serial_id >= min_next_begin){
                abort_process(this);
                return Abort;
            }
        }
    }


    /**
     * Writing phase
     */
     this->sler_serial_id = serial_id;

     // Update status.
     while(!ATOM_CAS(status_latch, false, true))
         PAUSE
     assert(status == validating);
     ATOM_CAS(status, validating, writing);
     status_latch = false;

     for(int rid = 0; rid < wr_cnt; rid++){
//         if(accesses[rid]->type == RD){
//             continue;
//         }

         // 11-22: We record new version in read_write_set.
         Version* new_version = (Version*)accesses[write_set[rid]]->tuple_version;
         Version* old_version = new_version->next;


         while (!ATOM_CAS(accesses[write_set[rid]]->orig_row->manager->blatch, false, true)){
             PAUSE
         }

         assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);

         old_version->end_ts = this->sler_serial_id;
         new_version->begin_ts = this->sler_serial_id;
         new_version->retire = nullptr;
         new_version->retire_ID = 0;                //11-17

         accesses[write_set[rid]]->orig_row->manager->blatch = false;
     }


     /**
      * Releasing Dependency
      */
     // Update status.
     while(!ATOM_CAS(status_latch, false, true))
         PAUSE

     assert(status == writing);
     ATOM_CAS(status, writing, committing);
     status_latch = false;

     auto deps = sler_dependency;

     for(auto dep_pair :deps){
         txn_man *txn_dep = dep_pair.dep_txn;
         uint64_t origin_txn_id = dep_pair.dep_txn_id;
         DepType type = dep_pair.dep_type;

         // only inform the txn which wasn't aborted and really depend on me[status == RUNNING]
//         while(!ATOM_CAS(txn_dep->status_latch, false, true))
//            PAUSE

         if(txn_dep->status == RUNNING && txn_dep->get_sler_txn_id() == origin_txn_id ){

             // if there is a RW dependency
             if(type == READ_WRITE_){
                 uint64_t origin_serial_ID;
                 uint64_t new_serial_ID;
                 do {
                     origin_serial_ID = txn_dep->sler_serial_id;
                     new_serial_ID = this->sler_serial_id + 1;
                 } while (origin_serial_ID < new_serial_ID && !ATOM_CAS(txn_dep->sler_serial_id, origin_serial_ID, new_serial_ID));

//                 // serialize other threads to concurrently modify the serial_ID
//                 while(!ATOM_CAS(txn_dep->serial_id_latch, false, true))
//                     PAUSE
//                     txn_dep->sler_serial_id = max(txn_dep->sler_serial_id, this->sler_serial_id + 1);
//                 txn_dep->serial_id_latch = false;
             }

             txn_dep->SemaphoreSubOne();
         }

//         txn_dep->status_latch = false;
     }


    // Update status.
    while(!ATOM_CAS(status_latch, false, true))
        PAUSE

    assert(status == committing);
    ATOM_CAS(status, committing, COMMITED);
    status_latch = false;

    return rc;
}



void txn_man::abort_process(txn_man * txn){
    while(!ATOM_CAS(status_latch, false, true))
        PAUSE
    assert(status == RUNNING || status == ABORTED || status == validating);
    status = ABORTED;
    status_latch = false;

    // 11-28
    if(wr_cnt != 0){
        for(int rid = 0; rid < row_cnt; rid++){
            if(accesses[rid]->type == RD){
                continue;
            }

            // 11-22: We record new version in read_write_set.
            Version* new_version = (Version*)accesses[rid]->tuple_version;
            Version* old_version = new_version->next;

            while (!ATOM_CAS(accesses[rid]->orig_row->manager->blatch, false, true)){
                PAUSE
            }

            assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);

            Version* row_header = accesses[rid]->orig_row->manager->get_version_header();

            // new version is the newest version
            if(new_version == row_header) {
                if (new_version->prev == NULL) {
                    accesses[rid]->orig_row->manager->version_header = old_version;
                    assert(accesses[rid]->orig_row->manager->version_header->end_ts == INF);

                    assert(old_version->prev == new_version);
                    old_version->prev = NULL;
                    new_version->next = NULL;
                }
                else{
                    accesses[rid]->orig_row->manager->version_header = old_version;
                    assert(accesses[rid]->orig_row->manager->version_header->end_ts == INF);

                    assert(old_version->prev == new_version);
                    old_version->prev = new_version->prev;
                    new_version->prev->next = old_version;

                    new_version->prev = NULL;
                    new_version->next = NULL;
                }
            }
            else{
                Version* pre_new = new_version->prev;
                if(pre_new){
                    pre_new->next = old_version;
                }
                if(old_version->prev == new_version){
                    old_version->prev = pre_new;
                }
                new_version->prev = NULL;
                new_version->next = NULL;
            }


//        new_version->row->free_row();
//        _mm_free(new_version->row);
//        new_version->row = NULL;
//        _mm_free(new_version->data);


            new_version->retire = nullptr;
            new_version->retire_ID = 0;                //11-17


            _mm_free(new_version);
            new_version = NULL;

            accesses[rid]->orig_row->manager->blatch = false;

        }
    }


    /**
     * Cascading abort
     */
    auto deps = sler_dependency;

    for(auto dep_pair :deps){
        txn_man *txn_dep = dep_pair.dep_txn;
        uint64_t origin_txn_id = dep_pair.dep_txn_id;
        DepType type = dep_pair.dep_type;

        // only inform the txn which wasn't aborted
        if(txn_dep->get_sler_txn_id() == origin_txn_id){
            if((type == WRITE_WRITE_) || (type == WRITE_READ_)){
//                assert(txn_dep->status == RUNNING || txn_dep->status == ABORTED);
                txn_dep->set_abort(true);
            }
        }
    }

//    while(!ATOM_CAS(status_latch, false, true))
//        PAUSE
//        status = ABORTED;
//    status_latch = false;
}


#endif

