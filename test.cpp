// 关系 R 具有两个属性 A 和 B，其中 A 和 B 的属性值均为 int 型（4 个字节）， A 的值域为[1, 40]，B 的值域为[1, 1000]。关系 S 具有两个属性 C 和 D，其中 C 和 D 的属性值均为 int 型（4 个字节）。 C 的值域为[20, 60]，D 的值域为[1, 1000]。
// 使用 ExtMem 程序库建立两个关系 R 和 S 的物理存储。关系的物理存储形式 为磁盘块序列 B1 , B2 , …, Bn ，其中 B i 的最后 4 个字节存放 B i+1 的地址。 即 R 和 S 的每个元组的大小均为 8 个字节。 块的大小设置为 64 个字节，缓冲区大小设置为 512+8=520 个字节。这样， 每块可存放 7 个元组和 1 个后继磁盘块地址，缓冲区内可最多存放 8 个块。编写程序，随机生成关系 R 和 S，使得 R 中包含 16 * 7 = 112 个元组，S 中 包含 32 * 7 = 224 个元组。

#include <cstdlib>
#include <cstdio>
#include <string>
#include "extmem.h"
#include <tuple>
#include <vector>
#include <map>
#include <algorithm>
using namespace std;

typedef tuple<int, int> Tuple2;
typedef tuple<int, int, int, int> Tuple4;

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

    HashLoader(Buffer *buf, int allocatedBlkNum, int initialAddr, int tupleSize)
    {
        this->buf = buf;
        this->initialAddr = initialAddr;
        this->currentAddr = initialAddr;
        this->allocatedBlkNum = allocatedBlkNum;
        this->tupleSize = tupleSize;
        this->hasRemaining = true;
    }

    void freeAllBlocks()
    {
        for (int i = 0; i < blocks.size(); i++)
        {
            freeBlockInBuffer(blocks[i], buf);
        }
        blocks.clear();
    }

    void buildHash()
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

    vector<unsigned char *> getTuples(int key)
    {
        if (hashTable.find(key) == hashTable.end())
        {
            return vector<unsigned char *>();
        }
        return hashTable[key];
    }

    bool hasKey(int key)
    {
        return hashTable.find(key) != hashTable.end();
    }
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

    Worker(Buffer *buf, int initialAddr, bool write_mode)
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

    int readIntFromBlkWithOffset(int offset)
    {
        int val;
        for (int i = 0; i < 4; i++)
        {
            ((unsigned char *)&val)[i] = blk[offset + i];
        }
        return val;
    }

    void setTupleSize(int tupleSize)
    {
        this->tupleSize = tupleSize;
    }

    int pushTuple(int val1, int val2)
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

    int pushTuple(int val1, int val2, int val3, int val4)
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

    int pushTuple(int val)
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

    int readTuple1()
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

    Tuple2 readTuple2()
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

    Tuple4 readTuple4()
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

    bool hasNext()
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

    void finish()
    {
        int nextBlockAddr = 0;
        memcpy(blk + buf->blkSize - 4, &nextBlockAddr, 4);
        printf("New block addr: %d\n", currentAddr);
        writeBlockToDisk(blk, currentAddr, buf);
    }

    void printAll()
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
};

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

Worker taskInitializeRelationR(Buffer buf)
{
    Worker writeWorker(&buf, 10000, true);
    for (int i = 0; i < 16; i++)
    {
        for (int j = 0; j < 7; j++)
        {
            writeWorker.pushTuple(randomInt(1, 40), randomInt(1, 1000));
        }
    }
    writeWorker.finish();
    return writeWorker;
}

Worker taskInitializeRelationS(Buffer buf)
{
    Worker writeWorker(&buf, 20000, true);
    for (int i = 0; i < 32; i++)
    {
        for (int j = 0; j < 7; j++)
        {
            writeWorker.pushTuple(randomInt(20, 60), randomInt(1, 1000));
        }
    }
    writeWorker.finish();
    return writeWorker;
}

Worker taskFilterRA40(Buffer *buf, int raddr)
{
    Worker reader(buf, raddr, false);
    reader.setTupleSize(8);
    Worker writer(buf, 30000, true);
    writer.setTupleSize(8);
    while (reader.hasNext())
    {
        Tuple2 tuple = reader.readTuple2();
        if (get<0>(tuple) == 40)
        {
            writer.pushTuple(get<0>(tuple), get<1>(tuple));
        }
    }
    writer.finish();
    return writer;
}

Worker taskFilterSC60(Buffer *buf, int saddr)
{
    Worker reader(buf, saddr, false);
    reader.setTupleSize(8);
    Worker writer(buf, 40000, true);
    writer.setTupleSize(8);
    while (reader.hasNext())
    {
        Tuple2 tuple = reader.readTuple2();
        if (get<0>(tuple) == 60)
        {
            writer.pushTuple(get<0>(tuple), get<1>(tuple));
        }
    }
    writer.finish();
    return writer;
}

Worker taskProjectRA(Buffer *buf, int saddr)
{
    Worker reader(buf, saddr, false);
    reader.setTupleSize(8);
    Worker writer(buf, 50000, true);
    writer.setTupleSize(4);
    while (reader.hasNext())
    {
        Tuple2 tuple = reader.readTuple2();
        writer.pushTuple(get<0>(tuple));
    }
    writer.finish();
    return writer;
}

Worker taskLoopJoin(Buffer *buf, int saddr, int raddr)
{
    Worker readerS(buf, saddr, false);
    readerS.setTupleSize(8);
    Worker readerR(buf, raddr, false);
    readerR.setTupleSize(8);
    Worker writer(buf, 60000, true);
    writer.setTupleSize(16);
    while (readerS.hasNext())
    {
        Tuple2 tupleS = readerS.readTuple2();
        while (readerR.hasNext())
        {
            Tuple2 tupleR = readerR.readTuple2();
            if (get<0>(tupleS) == get<0>(tupleR))
            {
                writer.pushTuple(get<0>(tupleS), get<1>(tupleS), get<0>(tupleR), get<1>(tupleR));
            }
        }
        freeBlockInBuffer(readerR.blk, buf);
        readerR = Worker(buf, raddr, false);
        readerR.setTupleSize(8);
    }
    writer.finish();
    return writer;
}

Worker taskHashJoin(Buffer buf, int saddr, int raddr)
{
    Worker readerS(&buf, saddr, false);
    readerS.setTupleSize(8);
    HashLoader hashLoader = HashLoader(&buf, 6, raddr, 8);
    Worker writer(&buf, 70000, true);
    writer.setTupleSize(16);
    while (hashLoader.hasRemaining)
    {
        hashLoader.buildHash();
        while (readerS.hasNext())
        {
            Tuple2 tupleS = readerS.readTuple2();
            if (hashLoader.hasKey(get<0>(tupleS)))
            {
                vector<unsigned char *> rowR = hashLoader.getTuples(get<0>(tupleS));
                for (int i = 0; i < rowR.size(); i++)
                {
                    int var1 = readIntFromBlkWithOffset(rowR[i], 0);
                    int var2 = readIntFromBlkWithOffset(rowR[i], 4);
                    writer.pushTuple(get<0>(tupleS), get<1>(tupleS), var1, var2);
                }
            }
        }
        freeBlockInBuffer(readerS.blk, &buf);
        readerS = Worker(&buf, saddr, false);
        readerS.setTupleSize(8);
    }
    writer.finish();
    return writer;
}

int main(int argc, char **argv)
{
    Buffer buf;         /* A buffer */
    unsigned char *blk; /* A pointer to a block */
    int i = 0;

    /* Initialize the buffer */
    if (!initBuffer(520, 64, &buf))
    {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    /* argv[1] can be buildR, buildS */
    if (argc != 2)
    {
        perror("Please input the correct number of parameters!\n");
        return -1;
    }
    if (string(argv[1]) == "buildR")
    {
        Worker worker = taskInitializeRelationR(buf);
        worker.printAll();
    }
    else if (string(argv[1]) == "select40")
    {
        Worker worker = taskInitializeRelationR(buf);
        Worker result = taskFilterRA40(&buf, worker.initialAddr);
        result.printAll();
    }
    else if (string(argv[1]) == "select60")
    {
        Worker worker = taskInitializeRelationS(buf);
        Worker result = taskFilterSC60(&buf, worker.initialAddr);
        result.printAll();
    }
    else if (string(argv[1]) == "project")
    {
        Worker worker = taskInitializeRelationR(buf);
        Worker result = taskProjectRA(&buf, worker.initialAddr);
        result.printAll();
    }
    else if (string(argv[1]) == "loopjoin")
    {
        Worker workerR = taskInitializeRelationR(buf);
        Worker workerS = taskInitializeRelationS(buf);
        Worker result = taskLoopJoin(&buf, workerS.initialAddr, workerR.initialAddr);
        result.printAll();
    }
    else if (string(argv[1]) == "hashjoin")
    {
        Worker workerR = taskInitializeRelationR(buf);
        Worker workerS = taskInitializeRelationS(buf);
        Worker result = taskHashJoin(buf, workerS.initialAddr, workerR.initialAddr);
        result.printAll();
    }
    else if (string(argv[1]) == "sortR")
    {
        Worker worker = taskInitializeRelationR(buf);
        Worker result = sortRelation2(&buf, 8, worker.initialAddr, 95000);
        result.printAll();
    }
    else if (string(argv[1]) == "merge")
    {
        Worker workerR = taskInitializeRelationR(buf);
        Worker workerS = taskInitializeRelationS(buf);
        Worker sortedR = sortRelation2(&buf, 8, workerR.initialAddr, 115000);
        Worker sortedS = sortRelation2(&buf, 8, workerS.initialAddr, 105000);
        // sortedR.printAll();
        Worker result = mergeJoin(&buf, sortedR.initialAddr, sortedS.initialAddr, 125000);
        result.printAll();
    }
    else
    {
        perror("Please input the correct parameters!\n");
        return -1;
    }

    printf("\n");
    printf("# of IO's is %lu\n", buf.numIO); /* Check the number of IO's */

    return 0;
}
