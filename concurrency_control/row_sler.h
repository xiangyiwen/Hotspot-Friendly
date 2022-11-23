//
// Created by root on 2022/9/18.
//

#ifndef DBX1000_ROW_SLER_H
#define DBX1000_ROW_SLER_H

#pragma once
#include "row.h"
#include "txn.h"
#include "global.h"

class table_t;
class Catalog;
class txn_man;

#if CC_ALG == SLER

#define INF UINT64_MAX

/**
 * Version Format in SLER
 */
struct Version {
    ts_t begin_ts;
    ts_t end_ts;
    Version* prev;
    Version* next;
    txn_man* retire;      // the txn_man of the uncommitted txn which updates the tuple version

    uint64_t retire_ID;     //11-17

//    row_t* data;

    volatile bool 	version_latch;

//    Version(): begin_ts(0), end_ts(INF),
//    prev(NULL),next(NULL),
//    retire(NULL),
//    row(NULL){};
};


class Row_sler {
public:
    void 			init(row_t * row);
    RC 				access(txn_man * txn, TsType type, Access *& access);
    Version*        get_version_header(){return this->version_header;}
//    RC              access_helper(txn_man * txn, Access * access,Version* temp_version_);

//    void            abort_process(txn_man * txn);
//    void 			write_process(txn_man * txn, uint64_t serial_id, RC rc);
    //void          set_row_id(uint64_t _row_id);

    volatile bool 	blatch;
    Version *       version_header;          // version header of a row's version chain (N2O)


private:
    void 		createNewVersion(txn_man * txn, Access * access);
    //void 			doubleHistory();

    /**
     * blatch: the write lock of a row
     * we can acquire a write lock with a CAS operation.
     * bool type is enough for DBx1000,since DBx1000 doesn't need to know which txn locks the tuple
     */
//    Version *       version_header;          // version header of a row's version chain (N2O)

    //uint64_t 		row_id;                 // a unique identifier of a row(equal to row_t::_primary_key)

};

#endif


#endif //DBX1000_ROW_SLER_H
