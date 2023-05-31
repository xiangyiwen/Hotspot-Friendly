//
// Created by root on 2022/9/18.
//

#ifndef DBX1000_ROW_HOTSPOT_FRIENDLY_H
#define DBX1000_ROW_HOTSPOT_FRIENDLY_H

#pragma once
#include "row.h"
#include "txn.h"
#include "global.h"

class table_t;
class Catalog;
class txn_man;

#if CC_ALG == HOTSPOT_FRIENDLY

#define INF UINT64_MAX


/**
 * Version Format in HOTSPOT_FRIENDLY
 */

#ifdef ABORT_OPTIMIZATION
// 24 bit chain_number + 40 bit deep_length
#define CHAIN_NUMBER (((1ULL << 24)-1) << 40)
#define DEEP_LENGTH ((1ULL << 40)-1)
#define CHAIN_NUMBER_ADD_ONE (1ULL << 40)
#endif


struct Version {
    ts_t begin_ts;
    ts_t end_ts;
    Version* prev;
    Version* next;
    txn_man* retire;      // the txn_man of the uncommitted txn which updates the tuple version
    uint64_t retire_ID;

    row_t* data;

#ifdef ABORT_OPTIMIZATION
    uint64_t version_number;
#endif
};


class Row_hotspot_friendly {
public:
    void 			init(row_t * row);
    RC 				access(txn_man * txn, TsType type, Access * access);
    Version*        get_version_header(){return this->version_header;}

    volatile bool 	blatch;
    Version *       version_header;          // version header of a row's version chain (N2O)

#if VERSION_CHAIN_CONTROL
    // Restrict the length of version chain.
    uint64_t threshold;

    void IncreaseThreshold(){
        ATOM_ADD(threshold,1);
    }

    void DecreaseThreshold(){
        ATOM_SUB(threshold,1);
    }
#endif


private:
    void 		createNewVersion(txn_man * txn, Access * access);
};

#endif


#endif //DBX1000_ROW_HOTSPOT_FRIENDLY_H
