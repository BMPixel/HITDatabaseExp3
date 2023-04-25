// 关系 R 具有两个属性 A 和 B，其中 A 和 B 的属性值均为 int 型（4 个字节）， A 的值域为[1, 40]，B 的值域为[1, 1000]。关系 S 具有两个属性 C 和 D，其中 C 和 D 的属性值均为 int 型（4 个字节）。 C 的值域为[20, 60]，D 的值域为[1, 1000]。
// 使用 ExtMem 程序库建立两个关系 R 和 S 的物理存储。关系的物理存储形式 为磁盘块序列 B1 , B2 , …, Bn ，其中 B i 的最后 4 个字节存放 B i+1 的地址。 即 R 和 S 的每个元组的大小均为 8 个字节。 块的大小设置为 64 个字节，缓冲区大小设置为 512+8=520 个字节。这样， 每块可存放 7 个元组和 1 个后继磁盘块地址，缓冲区内可最多存放 8 个块。编写程序，随机生成关系 R 和 S，使得 R 中包含 16 * 7 = 112 个元组，S 中 包含 32 * 7 = 224 个元组。

#include <string>
#include "worker.h"
#include "extmem.h"
using namespace std;

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
