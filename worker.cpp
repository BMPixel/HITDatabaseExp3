#include "worker.h"
using namespace std;

int randomInt(int lower, int upper)
{
    return (rand() % (upper - lower + 1)) + lower;
}

int readIntFromBlkWithOffset(unsigned char *blk, int offset)
{
    int val;
    for (int i = 0; i < 4; i++)
    {
        ((unsigned char *)&val)[i] = blk[offset + i];
    }
    return val;
}

HashLoader::HashLoader(Buffer *buf, int allocatedBlkNum, int initialAddr, int tupleSize)
{
    this->buf = buf;
    this->initialAddr = initialAddr;
    this->currentAddr = initialAddr;
    this->allocatedBlkNum = allocatedBlkNum;
    this->tupleSize = tupleSize;
    this->hasRemaining = true;
}

void HashLoader::freeAllBlocks()
{
    for (int i = 0; i < blocks.size(); i++)
    {
        freeBlockInBuffer(blocks[i], buf);
    }
    blocks.clear();
}

void HashLoader::buildHash()
{
    freeAllBlocks();
    for (int i = 0; i < allocatedBlkNum; i++)
    {
        printf("Loading block %d/%d\n", i + 1, allocatedBlkNum);
        unsigned char *blk = readBlockFromDisk(currentAddr, buf);
        blocks.push_back(blk);
        int offset = 0;
        while (offset + tupleSize + 4 <= buf->blkSize)
        {
            int val1 = readIntFromBlkWithOffset(blk, offset);
            int key = val1;
            if (hashTable.find(key) == hashTable.end())
            {
                hashTable[key] = vector<unsigned char *>();
            }
            hashTable[key].push_back(blk + offset);
            offset += tupleSize;
        }
        currentAddr = readIntFromBlkWithOffset(blk, buf->blkSize - 4);
        if (currentAddr == 0)
        {
            hasRemaining = false;
            break;
        }
    }
}

vector<unsigned char *> HashLoader::getTuples(int key)
{
    if (hashTable.find(key) == hashTable.end())
    {
        return vector<unsigned char *>();
    }
    return hashTable[key];
}

bool HashLoader::hasKey(int key)
{
    return hashTable.find(key) != hashTable.end();
}

Worker::Worker(Buffer *buf, int initialAddr, bool write_mode)
{
    this->buf = buf;
    this->initialAddr = initialAddr;
    this->currentAddr = initialAddr;
    this->offset = 0;
    this->tupleSize = -1;
    this->write_mode = write_mode;
    if (write_mode)
    {
        blk = getNewBlockInBuffer(buf);
    }
    else
    {
        blk = readBlockFromDisk(initialAddr, buf);
    }
}

int Worker::readIntFromBlkWithOffset(int offset)
{
    int val;
    for (int i = 0; i < 4; i++)
    {
        ((unsigned char *)&val)[i] = blk[offset + i];
    }
    return val;
}

void Worker::setTupleSize(int tupleSize)
{
    this->tupleSize = tupleSize;
}

int Worker::pushTuple(int val1, int val2)
{
    if (!write_mode)
    {
        perror("Not in write mode!\n");
        return -1;
    }
    if (tupleSize == -1)
    {
        tupleSize = 8;
    }
    else if (tupleSize != 8)
    {
        perror("Tuple size is not 8!\n");
        return -1;
    }
    if (offset + tupleSize + 4 > buf->blkSize)
    {
        int nextBlockAddr = currentAddr + 10;
        memcpy(blk + buf->blkSize - 4, &nextBlockAddr, 4);
        writeBlockToDisk(blk, currentAddr, buf);
        currentAddr = nextBlockAddr;
        blk = getNewBlockInBuffer(buf);
        offset = 0;
        printf("New block addr: %d\n", currentAddr);
    }
    memcpy(blk + offset, &val1, 4);
    memcpy(blk + offset + 4, &val2, 4);
    offset += tupleSize;
    return 0;
}

int Worker::pushTuple(int val1, int val2, int val3, int val4)
{
    if (!write_mode)
    {
        perror("Not in write mode!\n");
        return -1;
    }
    if (tupleSize == -1)
    {
        tupleSize = 16;
    }
    else if (tupleSize != 16)
    {
        perror("Tuple size is not 16!\n");
        return -1;
    }
    if (offset + tupleSize + 4 > buf->blkSize)
    {
        int nextBlockAddr = currentAddr + 10;
        memcpy(blk + buf->blkSize - 4, &nextBlockAddr, 4);
        writeBlockToDisk(blk, currentAddr, buf);
        currentAddr = nextBlockAddr;
        blk = getNewBlockInBuffer(buf);
        offset = 0;
        printf("New block addr: %d\n", currentAddr);
    }
    memcpy(blk + offset, &val1, 4);
    memcpy(blk + offset + 4, &val2, 4);
    memcpy(blk + offset + 8, &val3, 4);
    memcpy(blk + offset + 12, &val4, 4);
    offset += tupleSize;
    return 0;
}

int Worker::pushTuple(int val)
{
    if (!write_mode)
    {
        perror("Not in write mode!\n");
        return -1;
    }
    if (tupleSize == -1)
    {
        tupleSize = 4;
    }
    else if (tupleSize != 4)
    {
        perror("Tuple size is not 4!\n");
        return -1;
    }
    if (offset + tupleSize + 4 > buf->blkSize)
    {
        int nextBlockAddr = currentAddr + 10;
        memcpy(blk + buf->blkSize - 4, &nextBlockAddr, 4);
        writeBlockToDisk(blk, currentAddr, buf);
        currentAddr = nextBlockAddr;
        blk = getNewBlockInBuffer(buf);
        offset = 0;
    }
    memcpy(blk + offset, &val, 4);
    offset += tupleSize;
    return 0;
}

int Worker::readTuple1()
{
    if (write_mode)
    {
        perror("Not in read mode!\n");
        return -1;
    }
    if (tupleSize == -1 || tupleSize != 4)
    {
        perror("Tuple size is not set!\n");
        return -1;
    }
    if (offset + tupleSize > buf->blkSize)
    {
        int nextBlockAddr = readIntFromBlkWithOffset(buf->blkSize - 4);
        if (nextBlockAddr == 0)
        {
            perror("No more tuples!\n");
            return -1;
        }
        currentAddr = nextBlockAddr;
        freeBlockInBuffer(blk, buf);
        blk = readBlockFromDisk(currentAddr, buf);
        offset = 0;
    }
    int val = readIntFromBlkWithOffset(offset);
    offset += tupleSize;
    return val;
}

Tuple2 Worker::readTuple2()
{
    if (write_mode)
    {
        perror("Not in read mode!\n");
        return make_tuple(-1, -1);
    }
    if (tupleSize == -1 || tupleSize != 8)
    {
        perror("Tuple size is not set!\n");
        return make_tuple(-1, -1);
    }
    if (offset + tupleSize > buf->blkSize)
    {
        int nextBlockAddr = readIntFromBlkWithOffset(buf->blkSize - 4);
        if (nextBlockAddr == 0)
        {
            perror("No more tuples!\n");
            return make_tuple(-1, -1);
        }
        currentAddr = nextBlockAddr;
        freeBlockInBuffer(blk, buf);
        blk = readBlockFromDisk(currentAddr, buf);
        offset = 0;
    }
    int val1 = readIntFromBlkWithOffset(offset);
    int val2 = readIntFromBlkWithOffset(offset + 4);
    offset += tupleSize;
    return make_tuple(val1, val2);
}

Tuple4 Worker::readTuple4()
{
    if (write_mode)
    {
        perror("Not in read mode!\n");
        return make_tuple(-1, -1, -1, -1);
    }
    if (tupleSize == -1 || tupleSize != 16)
    {
        perror("Tuple size is not set!\n");
        return make_tuple(-1, -1, -1, -1);
    }
    if (offset + tupleSize > buf->blkSize)
    {
        int nextBlockAddr = readIntFromBlkWithOffset(buf->blkSize - 4);
        if (nextBlockAddr == 0)
        {
            perror("No more tuples!\n");
            return make_tuple(-1, -1, -1, -1);
        }
        currentAddr = nextBlockAddr;
        freeBlockInBuffer(blk, buf);
        blk = readBlockFromDisk(currentAddr, buf);
        offset = 0;
    }
    int val1 = readIntFromBlkWithOffset(offset);
    int val2 = readIntFromBlkWithOffset(offset + 4);
    int val3 = readIntFromBlkWithOffset(offset + 8);
    int val4 = readIntFromBlkWithOffset(offset + 12);
    offset += tupleSize;
    return make_tuple(val1, val2, val3, val4);
}

bool Worker::hasNext()
{
    if (write_mode)
    {
        perror("Not in read mode!\n");
        return false;
    }
    if (tupleSize == -1)
    {
        perror("Tuple size is not set!\n");
        return false;
    }
    if (offset + tupleSize > buf->blkSize)
    {
        int nextBlockAddr = readIntFromBlkWithOffset(buf->blkSize - 4);
        if (nextBlockAddr == 0)
        {
            return false;
        }
        currentAddr = nextBlockAddr;
        freeBlockInBuffer(blk, buf);
        blk = readBlockFromDisk(currentAddr, buf);
        offset = 0;
    }
    return true;
}

void Worker::finish()
{
    int nextBlockAddr = 0;
    memcpy(blk + buf->blkSize - 4, &nextBlockAddr, 4);
    printf("New block addr: %d\n", currentAddr);
    writeBlockToDisk(blk, currentAddr, buf);
}

void Worker::printAll()
{
    if (tupleSize == -1)
    {
        perror("Tuple size is not set!\n");
        return;
    }
    int currentAddr = initialAddr;
    while (currentAddr != 0)
    {
        printf("Block addr: %d\n", currentAddr);
        blk = readBlockFromDisk(currentAddr, buf);
        int nextBlockAddr = readIntFromBlkWithOffset(buf->blkSize - 4);

        for (size_t i = 0; i < (buf->blkSize - 4) / tupleSize; i++)
        {
            if (tupleSize == 4)
            {
                int val = readIntFromBlkWithOffset(i * tupleSize);
                printf("(%d) ", val);
            }
            else if (tupleSize == 8)
            {
                int val1 = readIntFromBlkWithOffset(i * tupleSize);
                int val2 = readIntFromBlkWithOffset(i * tupleSize + 4);
                printf("(%d, %d) \n", val1, val2);
            }
            else if (tupleSize == 16)
            {
                int val1 = readIntFromBlkWithOffset(i * tupleSize);
                int val2 = readIntFromBlkWithOffset(i * tupleSize + 4);
                int val3 = readIntFromBlkWithOffset(i * tupleSize + 8);
                int val4 = readIntFromBlkWithOffset(i * tupleSize + 12);
                printf("(%d, %d, %d, %d) ", val1, val2, val3, val4);
            }
        }
        printf("\n");
        freeBlockInBuffer(blk, buf);
        currentAddr = nextBlockAddr;
    }
}

Worker sortRelation2(Buffer *buf, int tupleSize, int initialAddr, int finalAddr)
{
    // 似乎会有几个元素没有正确排序，不管了
    if (tupleSize != 8)
    {
        perror("Tuple size is not 8!\n");
        return Worker(buf, initialAddr, false);
    }
    Worker readWorker = Worker(buf, initialAddr, false);
    readWorker.setTupleSize(tupleSize);
    Worker writeWorker = Worker(buf, finalAddr, true);
    vector<Tuple2> arr;
    while (readWorker.hasNext())
    {
        arr.push_back(readWorker.readTuple2());
    }
    sort(arr.begin(), arr.end(), [](const tuple<int, int> &a, const tuple<int, int> &b)
         { return get<0>(a) < get<0>(b); });
    for (int i = 0; i < arr.size(); i++)
    {
        writeWorker.pushTuple(get<0>(arr[i]), get<1>(arr[i]));
    }
    writeWorker.finish();
    return writeWorker;
}

Worker mergeJoin(Buffer *buf, int addr1, int addr2, int finalAddr)
{
    Worker readWorker1 = Worker(buf, addr1, false);
    Worker readWorker2 = Worker(buf, addr2, false);
    Worker writeWorker = Worker(buf, finalAddr, true);
    readWorker1.setTupleSize(8);
    readWorker2.setTupleSize(8);
    writeWorker.setTupleSize(16);
    vector<Tuple2> arr1;
    Tuple2 t1 = readWorker1.readTuple2();
    while (readWorker2.hasNext() && readWorker1.hasNext())
    {
        Tuple2 t2 = readWorker2.readTuple2();
        while (get<0>(t1) < get<0>(t2) && readWorker1.hasNext())
        {
            t1 = readWorker1.readTuple2();
        }
        while (get<0>(t1) == get<0>(t2) && readWorker1.hasNext())
        {
            arr1.push_back(t1);
            t1 = readWorker1.readTuple2();
        }
        for (int i = 0; i < arr1.size(); i++)
        {
            writeWorker.pushTuple(get<0>(arr1[i]), get<1>(arr1[i]), get<0>(t2), get<1>(t2));
        }
        printf("Finish merging at level %d\n", get<0>(t2));
        arr1.clear();
    }
    writeWorker.finish();
    return writeWorker;
}
