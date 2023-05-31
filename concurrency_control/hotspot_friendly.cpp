//
// Created by root on 2022/9/22.
//
#include "txn.h"
#include "row.h"
#include "row_hotspot_friendly.h"
#include "manager.h"
#include <mm_malloc.h>


#if CC_ALG == HOTSPOT_FRIENDLY

RC txn_man::validate_hotspot_friendly(RC rc) {
    uint64_t starttime = get_sys_clock();

    /**
     * Wait to validate. && Check deadlock again. || Abort myself.
     */
    while(true){
        // Abort myself actively
        if(status == ABORTED || rc == Abort){
            abort_process(this);
            return Abort;
        }
        if(hotspot_friendly_semaphore == 0){
            break;
        }

#if DEADLOCK_DETECTION
        //[Check Deadlock again]: Make sure the workload can finish.
        for(auto & dep_pair : hotspot_friendly_dependency) {
            if(status == ABORTED){
                abort_process(this);
                return Abort;
            }

            if(!dep_pair.dep_type){               // we may get an element before it being initialized(empty data / wrong data)
                break;
            }

            // DEADLOCK
            if (WaitingSetContains(dep_pair.dep_txn->hotspot_friendly_txn_id) && dep_pair.dep_txn->WaitingSetContains(hotspot_friendly_txn_id) && dep_pair.dep_txn->status == RUNNING) {
                if (dep_pair.dep_txn_id == dep_pair.dep_txn->hotspot_friendly_txn_id) {
                    dep_pair.dep_txn->set_abort();
                }
                abort_process(this);
                return Abort;
            }
        }
#endif

#if HOTSPOT_FRIENDLY_TIMEOUT
//         [Timeout]: Make sure the workload can finish.[1 ms(a transaction's average execution time is 0.16ms)]
        uint64_t span = get_sys_clock() - starttime;
        if(span > ABORT_WAIT_TIME*1000000UL){
            abort_process(this);
            return Abort;
        }
#endif
    }


    /**
     * Update status.
     */
    if(status == ABORTED){
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

    // separate write set from accesses
    int write_set[wr_cnt];
    int cur_wr_idx = 0;

    for(int rid = 0; rid < row_cnt; rid++){
        // Caculate serial_ID
        Version* current_version = (Version*)accesses[rid]->tuple_version;

        if(accesses[rid]->type == WR) {          // we record the new version in read_write_set
            current_version = current_version->next;
        }

        if(serial_id <= current_version->begin_ts) {
            while(current_version->begin_ts == UINT64_MAX){
                cout << hotspot_friendly_txn_id << " You should wait!  " << endl;

#if DEADLOCK_DETECTION
                //[DEADLOCK FIX]: Since we may subtract the semaphore of a txn too aggressively, the txn may be validated too early. So we have to do one more deadlock check here.
                for(auto & dep_pair : hotspot_friendly_dependency) {
                    if(!dep_pair.dep_type){               // we may get an element before it being initialized(empty data / wrong data)
                        break;
                    }

                    // DEADLOCK
                    if (WaitingSetContains(dep_pair.dep_txn->hotspot_friendly_txn_id) && dep_pair.dep_txn->WaitingSetContains(hotspot_friendly_txn_id) && dep_pair.dep_txn->status == RUNNING) {
                        if (dep_pair.dep_txn_id == dep_pair.dep_txn->hotspot_friendly_txn_id) {
                            dep_pair.dep_txn->set_abort();
                        }
                        abort_process(this);
                        return Abort;
                    }
                }
#endif
            }

            serial_id = current_version->begin_ts + 1;
            assert(serial_id > 0);
        }

        if(accesses[rid]->type == WR){
            write_set[cur_wr_idx ++] = rid;
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
                        PushDependency(newer_version_txn,newer_version_txn->get_hotspot_friendly_txn_id(),DepType::READ_WRITE_);
                        // Update waiting set [We don't have to do that, meaningless]
                    }
                }
                else if(temp_status == writing){
                    // newer_version->begin_ts may not be set, but newer_version_txn->hotspot_friendly_serial_id is already calculated.
                    min_next_begin = std::min(min_next_begin,newer_version_txn->hotspot_friendly_serial_id);
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
                    if(serial_id >= std::min(min_next_begin,newer_version_txn->hotspot_friendly_serial_id)){
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

            // hotspot_friendly_serial_id may be updated because of RW dependency.
            if(serial_id >= min_next_begin || this->hotspot_friendly_serial_id >= min_next_begin){
                abort_process(this);
                return Abort;
            }
        }
    }


    /**
     * Writing phase
     */
    // hotspot_friendly_serial_id may be updated because of RW dependency.
     this->hotspot_friendly_serial_id = max(this->hotspot_friendly_serial_id , serial_id);
     assert(this->hotspot_friendly_serial_id != 0);

     // Update status.
     while(!ATOM_CAS(status_latch, false, true))
         PAUSE

     assert(status == validating);
     ATOM_CAS(status, validating, writing);
     status_latch = false;

     for(int rid = 0; rid < wr_cnt; rid++){
         while (!ATOM_CAS(accesses[write_set[rid]]->orig_row->manager->blatch, false, true)){
             PAUSE
         }

         Version* new_version = (Version*)accesses[write_set[rid]]->tuple_version;
         Version* old_version = new_version->next;

         assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);

         assert(this->hotspot_friendly_serial_id > old_version->begin_ts);
         old_version->end_ts = this->hotspot_friendly_serial_id;
         new_version->begin_ts = this->hotspot_friendly_serial_id;
         new_version->retire = nullptr;
         new_version->retire_ID = 0;

#if VERSION_CHAIN_CONTROL
         //4-3 Restrict the length of version chain. [Subtract the count of uncommitted versions.]
         accesses[write_set[rid]]->orig_row->manager->DecreaseThreshold();
#endif

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

     for(auto & dep_pair :hotspot_friendly_dependency){
        if(dep_pair.dep_txn->status == RUNNING && dep_pair.dep_txn->get_hotspot_friendly_txn_id() == dep_pair.dep_txn_id ){

            // if there is a RW dependency
            if(dep_pair.dep_type == READ_WRITE_){
                uint64_t origin_serial_ID;
                uint64_t new_serial_ID;
                do {
                    origin_serial_ID = dep_pair.dep_txn->hotspot_friendly_serial_id;
                    new_serial_ID = this->hotspot_friendly_serial_id + 1;
                } while (origin_serial_ID < new_serial_ID && !ATOM_CAS(dep_pair.dep_txn->hotspot_friendly_serial_id, origin_serial_ID, new_serial_ID));
            }

            dep_pair.dep_txn->SemaphoreSubOne();
        }

         // Making concurrent_vector correct
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

#if VERSION_CHAIN_CONTROL
            //4-3 Restrict the length of version chain. [Subtract the count of uncommitted versions.]
            accesses[rid]->orig_row->manager->DecreaseThreshold();
#endif

            // We record new version in read_write_set.
            Version* new_version = (Version*)accesses[rid]->tuple_version;
            Version* old_version;
            Version* row_header;

            assert(new_version->begin_ts == UINT64_MAX && new_version->retire == this);

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

                                // Recursively update the chain_number of uncommitted old version and the first committed verison.
                                Version* version_retrieve = accesses[rid]->orig_row->manager->version_header;

                                while(version_retrieve->begin_ts == UINT64_MAX){
                                    assert(version_retrieve->retire != NULL);

                                    version_retrieve->version_number += CHAIN_NUMBER_ADD_ONE;
                                    version_retrieve = version_retrieve->next;
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

                                // Recursively update the chain_number of uncommitted old version and the first committed verison.
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
    if(wr_cnt != 0){
        for(int rid = 0; rid < row_cnt; rid++){
            if(accesses[rid]->type == RD){
                continue;
            }

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

            new_version->retire = nullptr;
            new_version->retire_ID = 0;

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
    for(auto & dep_pair :hotspot_friendly_dependency){

        // only inform the txn which wasn't aborted
        if(dep_pair.dep_txn->get_hotspot_friendly_txn_id() == dep_pair.dep_txn_id && dep_pair.dep_txn->status == RUNNING){
            if((dep_pair.dep_type == WRITE_WRITE_) || (dep_pair.dep_type == WRITE_READ_)){
                dep_pair.dep_txn->set_abort(true);
            }
            else{           // Have to release the semaphore of txns who READ_WRITE_ depend on me, otherwise they can never commit and causes deadlock.
                assert(dep_pair.dep_type == READ_WRITE_);
                if(dep_pair.dep_txn->get_hotspot_friendly_txn_id() == dep_pair.dep_txn_id && dep_pair.dep_txn->status == RUNNING) {         // Recheck: Don't inform txn_manger who is already running another txn. Otherwise, the semaphore of that txn will be decreased too much.
                    dep_pair.dep_txn->SemaphoreSubOne();
                }
            }
        }

        // Making concurrent_vector correct
        dep_pair.dep_type = INVALID;
    }
}


#endif

