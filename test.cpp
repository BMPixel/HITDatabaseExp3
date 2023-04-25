// 关系 R 具有两个属性 A 和 B，其中 A 和 B 的属性值均为 int 型（4 个字节）， A 的值域为[1, 40]，B 的值域为[1, 1000]。关系 S 具有两个属性 C 和 D，其中 C 和 D 的属性值均为 int 型（4 个字节）。 C 的值域为[20, 60]，D 的值域为[1, 1000]。
// 使用 ExtMem 程序库建立两个关系 R 和 S 的物理存储。关系的物理存储形式 为磁盘块序列 B1 , B2 , …, Bn ，其中 B i 的最后 4 个字节存放 B i+1 的地址。 即 R 和 S 的每个元组的大小均为 8 个字节。 块的大小设置为 64 个字节，缓冲区大小设置为 512+8=520 个字节。这样， 每块可存放 7 个元组和 1 个后继磁盘块地址，缓冲区内可最多存放 8 个块。编写程序，随机生成关系 R 和 S，使得 R 中包含 16 * 7 = 112 个元组，S 中 包含 32 * 7 = 224 个元组。

#include <cstdlib>
#include <cstdio>
#include <string>
#include "extmem.h"
using namespace std;

int randomInt(int lower, int upper)
{
    return (rand() % (upper - lower + 1)) + lower;
}

struct WorkingBlk
{
    unsigned char *blk;
    Buffer *buf;
    int initialAddr;
    int currentAddr;
    int offset;
    int tupleSize;
    WorkingBlk(Buffer *buf, int initialAddr)
    {
        this->buf = buf;
        this->initialAddr = initialAddr;
        this->currentAddr = initialAddr;
        this->offset = 0;
        this->tupleSize = -1;
        blk = getNewBlockInBuffer(buf);
    }

    int readInt(int offset)
    {
        int val;
        memcpy(&val, blk + offset, 4);
        return val;
    }

    int pushTuple(int val1, int val2)
    {
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
            int nextBlockAddr = currentAddr + 1;
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
            int nextBlockAddr = rand() % 100000000 + 100000000;
            memcpy(blk + buf->blkSize - 4, &nextBlockAddr, 4);
            writeBlockToDisk(blk, currentAddr, buf);
            currentAddr = nextBlockAddr;
            blk = getNewBlockInBuffer(buf);
            offset = 0;
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
            int nextBlockAddr = rand() % 100000000 + 100000000;
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

    void finish()
    {
        int nextBlockAddr = 0;
        memcpy(blk + buf->blkSize - 4, &nextBlockAddr, 4);
        printf("New block addr: %d\n", currentAddr);
        writeBlockToDisk(blk, currentAddr, buf);
    }

    int printAll()
    {
        if (tupleSize == -1)
        {
            perror("Tuple size is not set!\n");
            return -1;
        }
        int currentAddr = initialAddr;
        while (currentAddr != 0)
        {
            printf("Block addr: %d\n", currentAddr);
            blk = readBlockFromDisk(currentAddr, buf);
            int nextBlockAddr = readInt(buf->blkSize - 4);

            for (size_t i = 0; i < (buf->blkSize - 4) / tupleSize; i++)
            {
                if (tupleSize == 4)
                {
                    int val = readInt(i * tupleSize);
                    printf("%d ", val);
                }
                else if (tupleSize == 8)
                {
                    int val1 = readInt(i * tupleSize);
                    int val2 = readInt(i * tupleSize + 4);
                    printf("(%d, %d) \n", val1, val2);
                }
                else if (tupleSize == 16)
                {
                    int val1 = readInt(i * tupleSize);
                    int val2 = readInt(i * tupleSize + 4);
                    int val3 = readInt(i * tupleSize + 8);
                    int val4 = readInt(i * tupleSize + 12);
                    printf("(%d, %d, %d, %d) ", val1, val2, val3, val4);
                }
            }
            printf("\n");
            freeBlockInBuffer(blk, buf);
            currentAddr = nextBlockAddr;
        }
    }
};

WorkingBlk initializeRelationR(Buffer buf)
{
    WorkingBlk workingBlk(&buf, 1000000);
    for (int i = 0; i < 16; i++)
    {
        for (int j = 0; j < 7; j++)
        {
            workingBlk.pushTuple(randomInt(1, 40), randomInt(1, 1000));
        }
    }
    workingBlk.finish();
    return workingBlk;
}

WorkingBlk initializeRelationS(Buffer buf)
{
    WorkingBlk workingBlk(&buf, 1000400);
    for (int i = 0; i < 32; i++)
    {
        for (int j = 0; j < 7; j++)
        {
            workingBlk.pushTuple(randomInt(20, 60), randomInt(1, 1000));
        }
    }
    workingBlk.finish();
    return workingBlk;
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
        initializeRelationR(buf);
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
