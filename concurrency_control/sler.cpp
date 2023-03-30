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

    //12-5 Debug
//    int temp_sem = sler_semaphore;

    /**
     * Wait to validate. && Check deadlock again. || Abort myself.
     */
    while(true){
        // Abort myself actively
        if(status == ABORTED || rc == Abort){
            abort_process(this);
            return Abort;
        }
        if(sler_semaphore == 0){
            break;
        }

/*
        int dp_size = dependency_cnt;
        //12-6 make sure the workload can finish
        for(int i = 0; i < sler_dependency.size(); i++) {
            txn_man *txn_dep = sler_dependency[i].dep_txn;
            uint64_t origin_txn_id = sler_dependency[i].dep_txn_id;
            auto type = sler_dependency[i].dep_type;
            auto j = i;

            auto dep_size= sler_dependency.size();

//            bool a = sler_dependency[i].dep_type == READ_WRITE_;

            if(sler_dependency[i].dep_type == READ_WRITE_){
                cout << "now_i : " << i << endl;

                cout << endl << "Use for" << endl;

                cout << "a = " << a << endl;

                cout << sler_txn_id <<  "dep_txn: " << sler_dependency[i].dep_txn << endl;
                cout << sler_txn_id <<  "dep_txn_id: " << sler_dependency[i].dep_txn_id << endl;
                cout << sler_txn_id <<  "dep_type: " << sler_dependency[i].dep_type << endl;
                cout << endl << "Use auto" << endl;
                for(auto & j : sler_dependency){
                    if(j.dep_type == READ_WRITE_){
                        cout << endl << "Auto wrong too." << endl;
                    }
                    cout << sler_txn_id << "dep_txn: " << j.dep_txn << endl;
                    cout << sler_txn_id <<  "dep_txn_id: " << j.dep_txn_id << endl;
                    cout << sler_txn_id <<  "dep_type: " << j.dep_type << endl;
                }

                auto debug_RW_size = dep_debug.size();

                cout << endl;
                cout << endl;
//                assert(false);
            }

            // DEADLOCK
            if (WaitingSetContains(txn_dep->sler_txn_id) && txn_dep->WaitingSetContains(sler_txn_id) && txn_dep->status == RUNNING) {
                if (origin_txn_id == txn_dep->sler_txn_id) {
                    txn_dep->set_abort();
                }
                abort_process(this);
                return Abort;
            }
        }*/

//        int dp_size = dependency_cnt;
//        int debug_i =0 ;

        // 2-28 Reduce percentage of wrong killing in single hotspot scene..
//        uint64_t span = get_sys_clock() - starttime;
//        if(span > 10000){

        //12-6 [Check Deadlock again]: Make sure the workload can finish.
        for(auto & dep_pair : sler_dependency) {
            if(status == ABORTED){
                abort_process(this);
                return Abort;
            }

            if(!dep_pair.dep_type){               // we may get an element before it being initialized(empty data / wrong data)
                break;
            }

            // 12-12 Debug: There shouldn't appear READ_WRITE_ dependency.
            /*
            txn_man *txn_dep = dep_pair.dep_txn;
            uint64_t origin_txn_id = dep_pair.dep_txn_id;
            auto type = dep_pair.dep_type;

            auto dep_size= sler_dependency.size();

            if(dep_pair.dep_type == READ_WRITE_){
                if(dep_pair.dep_type == READ_WRITE_){
                    cout << endl << "Auto wrong too." << endl;
                }
                cout << "First Data." << endl;
                cout << txn_dep << endl;
                cout << origin_txn_id << endl;
                cout << endl;

//                cout << sler_txn_id <<  "    1: dep_type: " << dep_pair.dep_type << endl;
//                cout << sler_txn_id <<  "    2: dep_type: " << dep_pair.dep_type << endl;

                cout << "Use auto" << endl;
//                cout << sler_txn_id <<  "    3: dep_type: " << dep_pair.dep_type << endl;

                for(auto & i : sler_dependency){
//                    cout << sler_txn_id <<  "    4: dep_type: " << dep_pair.dep_type << endl;

                    if(i.dep_type == READ_WRITE_){
                        cout << endl << "Auto wrong too." << endl;
                    }
                    cout << sler_txn_id << "dep_txn: " << i.dep_txn << endl;
                    cout << sler_txn_id <<  "dep_txn_id: " << i.dep_txn_id << endl;
                    cout << sler_txn_id <<  "dep_type: " << i.dep_type << endl;
                }

//                cout << sler_txn_id <<  "    5: dep_type: " << dep_pair.dep_type << endl;

                cout << endl;
                cout << endl;
            }
            */

            // DEADLOCK
            if (WaitingSetContains(dep_pair.dep_txn->sler_txn_id) && dep_pair.dep_txn->WaitingSetContains(sler_txn_id) && dep_pair.dep_txn->status == RUNNING) {
                if (dep_pair.dep_txn_id == dep_pair.dep_txn->sler_txn_id) {
                    dep_pair.dep_txn->set_abort();
                }
                abort_process(this);
                return Abort;
            }
        }

        //        }


//         [Timeout]: Make sure the workload can finish.[1 ms(a transaction's average execution time is 0.16ms)]
        uint64_t span = get_sys_clock() - starttime;
        if(span > 0.5*1000000UL){

//            printf("txn_id: %lu, semaphore: %ld, status: %d, validate_time: %lu\n", sler_txn_id,sler_semaphore,status,span);
//            printf("semaphore: %ld;\n",sler_semaphore);

//            printf("waiting_list size = %zu, release_cnt = %zu\n",wait_list.size(),dep_debug.size());


            abort_process(this);
            return Abort;
        }
    }


    /**
     * Update status.
     */
    /*
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
   */

    /*
     * Abort myself actively.
     * [Theory Impossible]: Shouldn't enter the branch of status == ABORTED.
     *    These branches can only be entered when semaphore == 0 && status != ABORTED in line 24, which is impossible.
     *    Because we never aggressively subtract semaphore, which means t.semaphore == 0 only when transactions t depends on are committed(WR/WW/RW) or aborted(RW).
     * [Actually Possible]: Wrong deadlock detection.
     *    When t1 accesses an uncommitted tuple version(created by t2) and find that this access will cause deadlock,
     *    t1 will set t2.status = ABORTED to abort t2. However, the deadlock detection has certain possibility to kill t2 wrongly.
     *    In the situation where t2 doesn't depend on t1, but t2 is wrongly detected to cause deadlock, t2 may normally process and first reach line 28 with semaphore == 0,
     *    and then set_abort() by t1 and then reach the following status == ABORTED branches, which make semaphore == 0 && status == ABORTED to be possible.
     */
    if(status == ABORTED){
        //2-15 DEBUG
//        uint64_t temp_semaphore = this->sler_semaphore;         // semaphore == 0

        abort_process(this);
        return Abort;
    }
    else if(status == RUNNING){
        if(!ATOM_CAS(status, RUNNING, validating)) {
            assert(status == ABORTED);
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

        if(accesses[rid]->type == WR) {          // we record the new version in read_write_set   11-22
            current_version = current_version->next;
        }

        if(serial_id <= current_version->begin_ts) {
            while(current_version->begin_ts == UINT64_MAX){
                cout << sler_txn_id << " You should wait!  " << endl;

                // 12-12 [DEADLOCK FIX]: Since we may subtract the semaphore of a txn too aggressively, the txn may be validated too early. So we have to do one more deadlock check here.
                for(auto & dep_pair : sler_dependency) {
                    if(!dep_pair.dep_type){               // we may get an element before it being initialized(empty data / wrong data)
                        break;
                    }

                    // 12-12 Debug: There shouldn't appear READ_WRITE_ dependency.
                    /*
                    txn_man *txn_dep = dep_pair.dep_txn;
                    uint64_t origin_txn_id = dep_pair.dep_txn_id;
                    auto type = dep_pair.dep_type;

                    auto dep_size= sler_dependency.size();

                    if(dep_pair.dep_type == READ_WRITE_){
                        if(dep_pair.dep_type == READ_WRITE_){
                            cout << endl << "Auto wrong too." << endl;
                        }
                        cout << "First Data." << endl;
                        cout << txn_dep << endl;
                        cout << origin_txn_id << endl;
                        cout << endl;

        //                cout << sler_txn_id <<  "    1: dep_type: " << dep_pair.dep_type << endl;
        //                cout << sler_txn_id <<  "    2: dep_type: " << dep_pair.dep_type << endl;

                        cout << "Use auto" << endl;
        //                cout << sler_txn_id <<  "    3: dep_type: " << dep_pair.dep_type << endl;

                        for(auto & i : sler_dependency){
        //                    cout << sler_txn_id <<  "    4: dep_type: " << dep_pair.dep_type << endl;

                            if(i.dep_type == READ_WRITE_){
                                cout << endl << "Auto wrong too." << endl;
                            }
                            cout << sler_txn_id << "dep_txn: " << i.dep_txn << endl;
                            cout << sler_txn_id <<  "dep_txn_id: " << i.dep_txn_id << endl;
                            cout << sler_txn_id <<  "dep_type: " << i.dep_type << endl;
                        }

        //                cout << sler_txn_id <<  "    5: dep_type: " << dep_pair.dep_type << endl;

                        cout << endl;
                        cout << endl;
                    }
                    */

                    // DEADLOCK
                    if (WaitingSetContains(dep_pair.dep_txn->sler_txn_id) && dep_pair.dep_txn->WaitingSetContains(sler_txn_id) && dep_pair.dep_txn->status == RUNNING) {
                        if (dep_pair.dep_txn_id == dep_pair.dep_txn->sler_txn_id) {
                            dep_pair.dep_txn->set_abort();
                        }
                        abort_process(this);
                        return Abort;
                    }
                }
            }

            serial_id = current_version->begin_ts + 1;
            assert(serial_id > 0);
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
                        //12-6 Debug
                        /*
//                        newer_version_txn->PushWaitList(this,sler_txn_id,DepType::READ_WRITE_);

//                        PushDependency(DepType::READ_WRITE_,this,sler_txn_id);      //12-6 Debug

//                        cout << endl << "!!!!! insert_txn: " << this << endl;
//                        printf("!!!!! insert_txn_id:=%ld\n",sler_txn_id);
//                        cout << "!!!!!! dep_txn: " << newer_version_txn << endl;
//                        printf("!!!!! sler_txn_id:=%ld\n",newer_version_txn->get_sler_txn_id());
//                        cout << "!!!!! sler_txn_id: " << newer_version_txn->get_sler_txn_id() << endl;

                      //  cout << endl << "!!!!!! dep_txn: " << newer_version_txn << "!!!!! sler_txn_id: " << newer_version_txn->get_sler_txn_id() << endl;

                      */
                        PushDependency(newer_version_txn,newer_version_txn->get_sler_txn_id(),DepType::READ_WRITE_);

                    // Update waiting set [We don't have to do that, meaningless]
                    /*
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
                    */
                    }
                }
//                else if(temp_status == writing || temp_status == committing || temp_status == COMMITED){
//                    // Treat next tuple version as committed(Do nothing here)
//
//                    // 2-13: Make sure "COMMITTED" txn won't process too fast that it has already reset newer_version_txn->sler_serial_id = 0.
//                    assert(newer_version_txn->sler_serial_id != 0);
//                    min_next_begin = std::min(min_next_begin,newer_version_txn->sler_serial_id);
//                }
                else if(temp_status == writing){
                    // newer_version->begin_ts may not be set, but newer_version_txn->sler_serial_id is already calculated.
                    min_next_begin = std::min(min_next_begin,newer_version_txn->sler_serial_id);
                }
                else if(temp_status == committing || temp_status == COMMITED){
                    // Treat next tuple version as committed(Do nothing here)
                    assert(newer_version->begin_ts != UINT64_MAX);
                    min_next_begin = std::min(min_next_begin,newer_version->begin_ts);
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

            // [BUG FIX] : sler_serial_ID may be updated because of RW dependency.
            if(serial_id >= min_next_begin || this->sler_serial_id >= min_next_begin){
                abort_process(this);
                return Abort;
            }
        }
    }


    /**
     * Writing phase
     */
    // [BUG FIX] : sler_serial_ID may be updated because of RW dependency.
     this->sler_serial_id = max(this->sler_serial_id , serial_id);
     assert(this->sler_serial_id != 0);

     // Update status.
     while(!ATOM_CAS(status_latch, false, true))
         PAUSE

     assert(status == validating);
     ATOM_CAS(status, validating, writing);
     status_latch = false;

     for(int rid = 0; rid < wr_cnt; rid++){

         // 11-22: We record new version in read_write_set. [Bug: we should first take the lock, otherwise old_version may be a wrong version]
//         Version* new_version = (Version*)accesses[write_set[rid]]->tuple_version;
//         Version* old_version = new_version->next;

         while (!ATOM_CAS(accesses[write_set[rid]]->orig_row->manager->blatch, false, true)){
             PAUSE
         }

         Version* new_version = (Version*)accesses[write_set[rid]]->tuple_version;
         Version* old_version = new_version->next;

         assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);

         assert(this->sler_serial_id > old_version->begin_ts);
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

     for(auto & dep_pair :sler_dependency){

         // only inform the txn which wasn't aborted and really depend on me[status == RUNNING]
//         while(!ATOM_CAS(txn_dep->status_latch, false, true))
//            PAUSE

        if(dep_pair.dep_txn->status == RUNNING && dep_pair.dep_txn->get_sler_txn_id() == dep_pair.dep_txn_id ){

            // if there is a RW dependency
            if(dep_pair.dep_type == READ_WRITE_){
                uint64_t origin_serial_ID;
                uint64_t new_serial_ID;
                do {
                    origin_serial_ID = dep_pair.dep_txn->sler_serial_id;
                    new_serial_ID = this->sler_serial_id + 1;
                } while (origin_serial_ID < new_serial_ID && !ATOM_CAS(dep_pair.dep_txn->sler_serial_id, origin_serial_ID, new_serial_ID));

                //                 // serialize other threads to concurrently modify the serial_ID
                //                 while(!ATOM_CAS(txn_dep->serial_id_latch, false, true))
                //                     PAUSE
                //                     txn_dep->sler_serial_id = max(txn_dep->sler_serial_id, this->sler_serial_id + 1);
                //                 txn_dep->serial_id_latch = false;
            }

            dep_pair.dep_txn->SemaphoreSubOne();
        }

//         txn_dep->status_latch = false;

         //12-12 [BUG Fixed] Making concurrent_vector correct
         dep_pair.dep_type = INVALID;
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

#ifdef ABORT_OPTIMIZATION
    if(wr_cnt != 0){
        for(int rid = 0; rid < row_cnt; rid++){
            if(accesses[rid]->type == RD){
                continue;
            }

            // We record new version in read_write_set.
            Version* new_version = (Version*)accesses[rid]->tuple_version;
            Version* old_version;
            Version* row_header;

            assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);

            /* BUG : Don't get the value of new_version.version_number at first. This value changes all the time, so we should get the value just before we use it or right after we acquire the blatch.
            uint64_t my_version_chain_number = (new_version->version_number & CHAIN_NUMBER);
            uint64_t my_version_deep_length = new_version->version_number & DEEP_LENGTH;
             */

            row_header = accesses[rid]->orig_row->manager->get_version_header();
            uint64_t vh_chain = (row_header->version_number & CHAIN_NUMBER) >> 40;
            uint64_t my_chain = ((new_version->version_number & CHAIN_NUMBER) >> 40) + 1;
            // [No Latch]: Free directly.
            if(vh_chain > my_chain){
                assert(row_header->version_number > new_version->version_number);

                new_version->retire = nullptr;
                new_version->retire_ID = 0;

                new_version->prev = NULL;
                new_version->next = NULL;

                //TODO: can we just free this object without reset retire and retire_ID?

                _mm_free(new_version);
                new_version = NULL;
            }
            else{       // In the same chain.
                while (!ATOM_CAS(accesses[rid]->orig_row->manager->blatch, false, true)){
                    PAUSE
                }

                // update the version_header if there's a new header.
                row_header = accesses[rid]->orig_row->manager->get_version_header();
                vh_chain = (row_header->version_number & CHAIN_NUMBER) >> 40;
                my_chain = ((new_version->version_number & CHAIN_NUMBER) >> 40);

                assert(vh_chain >= my_chain);

                // No longer in the same chain. [No Latch]: Free directly.
                if(vh_chain != my_chain) {

                    assert(row_header->version_number > new_version->version_number);

                    accesses[rid]->orig_row->manager->blatch = false;

                    new_version->retire = nullptr;
                    new_version->retire_ID = 0;

                    new_version->prev = NULL;
                    new_version->next = NULL;

                    //TODO: can we just free this object without reset retire and retire_ID?

                    _mm_free(new_version);
                    new_version = NULL;
                }
                    // [Need Latch]: We are in the same chain.
                else{
                    // Get the old_version and depth of version_header and I.
                    old_version = new_version->next;
                    uint64_t vh_depth = (row_header->version_number & DEEP_LENGTH);
                    uint64_t my_depth = (new_version->version_number & DEEP_LENGTH);

                    // Check again to avoid acquiring unnecessary latch. [Only need latch when I'm in the front of version_header]
                    if(vh_depth >= my_depth) {

                        // new version is the newest version
                        if (new_version == row_header) {
                            if (new_version->prev == NULL) {
                                assert(old_version != NULL);
                                accesses[rid]->orig_row->manager->version_header = old_version;
                                assert(accesses[rid]->orig_row->manager->version_header->end_ts == UINT64_MAX);

                                assert(old_version->prev == new_version);
                                old_version->prev = NULL;
                                new_version->next = NULL;

                                //3-27 Solution-1:Recursively update the chain_number of uncommitted old version and the first committed verison.
                                Version* version_retrieve = accesses[rid]->orig_row->manager->version_header;

                                while(version_retrieve->begin_ts == UINT64_MAX){
                                    assert(version_retrieve->retire != NULL);

                                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                                    version_retrieve = version_retrieve->next;

                                    /*
                                    version_retrieve->chain_num += 1;
                                    assert(version_retrieve->chain_num != 0);
                                    if((version_retrieve->version_number & CHAIN_NUMBER) == (pow(2,24)-1)){
                                        printf("Overflow!\n");
                                    }
                                    */
                                }

                                // Update the chain-number of the first committed version.
                                assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL && version_retrieve->retire_ID == 0);
                                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                            } else {
                                accesses[rid]->orig_row->manager->version_header = old_version;

                                assert(accesses[rid]->orig_row->manager->version_header->end_ts == INF);
                                assert(old_version->prev == new_version);

                                // Should link these two versions.
                                Version *pre_new = new_version->prev;
                                if (pre_new && pre_new->next == new_version) {
                                    pre_new->next = old_version;
                                }
                                if (old_version->prev == new_version) {
                                    // Possible: old_version is already pointing to new version.
                                    old_version->prev = pre_new;
                                }

                                new_version->prev = NULL;
                                new_version->next = NULL;


                                //3-27 Solution-1:Recursively update the chain_number of uncommitted old version and the first committed verison.
                                Version* version_retrieve = accesses[rid]->orig_row->manager->version_header;

                                while(version_retrieve->begin_ts == UINT64_MAX){
                                    assert(version_retrieve->retire != NULL);

                                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                                    version_retrieve = version_retrieve->next;
                                }

                                // Update the chain-number of the first committed version.
                                assert(version_retrieve->begin_ts != UINT64_MAX && version_retrieve->retire == NULL && version_retrieve->retire_ID == 0);
                                version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                            }
                        } else {
                            // Should link these two versions.
                            Version *pre_new = new_version->prev;
                            if (pre_new && pre_new->next == new_version) {
                                pre_new->next = old_version;
                            }
                            if (old_version->prev == new_version) {
                                // Possible: old_version is already pointing to new version.
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

                        //TODO: Notice that begin_ts and end_ts of new_version both equal to MAX

                        _mm_free(new_version);
                        new_version = NULL;

                        accesses[rid]->orig_row->manager->blatch = false;
                    }
                    else{
                        // Possible: May be I'm the version_header at first, but when I wait for blatch, someone changes the version_header to a new version.
                        assert(vh_chain == my_chain && vh_depth < my_depth);

                        accesses[rid]->orig_row->manager->blatch = false;

                        new_version->retire = nullptr;
                        new_version->retire_ID = 0;

                        new_version->prev = NULL;
                        new_version->next = NULL;

                        //TODO: can we just free this object without reset retire and retire_ID?

                        _mm_free(new_version);
                        new_version = NULL;
                    }
                }
            }

        }
    }
#else
    // 11-28
    if(wr_cnt != 0){
        for(int rid = 0; rid < row_cnt; rid++){
            if(accesses[rid]->type == RD){
                continue;
            }

            // 11-22: We record new version in read_write_set. [Bug: we should first take the lock, otherwise old_version may be a wrong version]
//            Version* new_version = (Version*)accesses[rid]->tuple_version;
//            Version* old_version = new_version->next;

            while (!ATOM_CAS(accesses[rid]->orig_row->manager->blatch, false, true)){
                PAUSE
            }

            Version* new_version = (Version*)accesses[rid]->tuple_version;
            Version* old_version = new_version->next;

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
                    // I think (old_version->prev == new_version) is always true.
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

            //TODO: Notice that begin_ts and end_ts of new_version both equal to MAX


            _mm_free(new_version);
            new_version = NULL;

            accesses[rid]->orig_row->manager->blatch = false;

        }
    }
#endif


    /**
     * Cascading abort
     */
    for(auto & dep_pair :sler_dependency){

        // only inform the txn which wasn't aborted
        if(dep_pair.dep_txn->get_sler_txn_id() == dep_pair.dep_txn_id && dep_pair.dep_txn->status == RUNNING){
            if((dep_pair.dep_type == WRITE_WRITE_) || (dep_pair.dep_type == WRITE_READ_)){
                dep_pair.dep_txn->set_abort(true);
            }
            else{           //[Fix BUG: 2 threads + 15 ops] Have to release the semaphore of txns who READ_WRITE_ depend on me, otherwise they can never commit and causes deadlock.
                assert(dep_pair.dep_type == READ_WRITE_);
                if(dep_pair.dep_txn->get_sler_txn_id() == dep_pair.dep_txn_id && dep_pair.dep_txn->status == RUNNING) {         // Recheck: Don't inform txn_manger who is already running another txn. Otherwise, the semaphore of that txn will be decreased too much.
                    dep_pair.dep_txn->SemaphoreSubOne();
                }
            }
        }

        //12-12 [BUG Fixed] Making concurrent_vector correct
        dep_pair.dep_type = INVALID;
    }

//    while(!ATOM_CAS(status_latch, false, true))
//        PAUSE
//        status = ABORTED;
//    status_latch = false;
}


#endif

