
#ifndef WORKER_H
#define WORKER_H

#include <cstdio>
#include <vector>
#include <map>
#include <algorithm>
#include "extmem.h"
using namespace std;

typedef tuple<int, int> Tuple2;
typedef tuple<int, int, int, int> Tuple4;

int randomInt(int lower, int upper);

int readIntFromBlkWithOffset(unsigned char *blk, int offset);

struct HashLoader
{
    vector<unsigned char *> blocks;
    Buffer *buf;
    int allocatedBlkNum;
    map<int, vector<unsigned char *>> hashTable;
    int tupleSize;
    int initialAddr;
    int currentAddr;
    bool hasRemaining;

    HashLoader(Buffer *buf, int allocatedBlkNum, int initialAddr, int tupleSize);

    void freeAllBlocks();

    void buildHash();

    vector<unsigned char *> getTuples(int key);

    bool hasKey(int key);
};

struct Worker
{
    unsigned char *blk;
    Buffer *buf;
    int initialAddr;
    int currentAddr;
    int offset;
    int tupleSize;
    bool write_mode;

    Worker(Buffer *buf, int initialAddr, bool write_mode);

    int readIntFromBlkWithOffset(int offset);

    void setTupleSize(int tupleSize);

    int pushTuple(int val1, int val2);

    int pushTuple(int val1, int val2, int val3, int val4);

    int pushTuple(int val);

    int readTuple1();

    Tuple2 readTuple2();

    Tuple4 readTuple4();

    bool hasNext();

    void finish();

    void printAll();
};

Worker sortRelation2(Buffer *buf, int tupleSize, int initialAddr, int finalAddr);
Worker mergeJoin(Buffer *buf, int addr1, int addr2, int finalAddr);

#endif