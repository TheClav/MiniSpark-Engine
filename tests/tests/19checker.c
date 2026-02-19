#include <stdio.h>
#include <stdlib.h>
#include "lib.h"

int checker(struct row **file, int filesize, int lineperfile, int npartitions)
{
    int prevpart = 0;

    struct colpart_ctx pctx;
    pctx.keynum = 0;
    for (int i = 0; i < filesize * lineperfile; i++)
    {
        int hash = ColumnHashPartitioner(file[i], npartitions, &pctx);
        if (hash < prevpart)
            return i;
        else if (hash >= npartitions)
            return i;
        else
        {
            prevpart = hash;
        }
    }
    return 0;
}

void printfile(struct row **file, int wrongline, int npartitions)
{
    if (wrongline < 1)
        return;

    struct colpart_ctx pctx;
    pctx.keynum = 0;
    printf("wrong partition order, prev partition smaller than cur or having too large partition number\n");
    printf("prev line : (partition %ld)\n", ColumnHashPartitioner(file[wrongline - 1], npartitions, &pctx));
    RowPrinter(file[wrongline - 1]);

    printf("cur line : (partition %ld)\n", ColumnHashPartitioner(file[wrongline], npartitions, &pctx));
    RowPrinter(file[wrongline]);
}

int *populate_partitions(char **filename, int numfiles, int numlines, int npartitions)
{
    int *ret = calloc(npartitions, sizeof(int));

    struct colpart_ctx pctx;
    pctx.keynum = 0;

    for (int i = 0; i < numfiles; i++)
    {
        FILE *f = fopen(filename[i], "r");
        if (!f) {
            printf("failed to open file %s\n", filename[i]);
            exit(1);
        }
        for (int j = 0; j < numlines; j++)
        {
            void * line = GetLines(f);
            if (!line) {
                printf("not enough line in file %s\n", filename[i]);
                exit(1);
            }
            int hash = ColumnHashPartitioner(SplitCols(line), npartitions, &pctx);
            ret[hash]++;
        }
    }
    return ret;
}

int check_counts(int *refcnt, struct row **file, int filesize, int lineperfile, int npartitions)
{

    int *curcnt = calloc(npartitions, sizeof(int));
    struct colpart_ctx pctx;
    pctx.keynum = 0;
    for (int i = 0; i < filesize * lineperfile; i++)
    {
        int hash = ColumnHashPartitioner(file[i], npartitions, &pctx);
        curcnt[hash]++;
    }

    for (int i = 0; i < npartitions; i++)
    {
        if (curcnt[i] != refcnt[i])
        {
            free(curcnt);
            return 1;
        }
    }
    free(curcnt);
    return 0;
}

#define NUMFILES 1024

int main()
{

    char *filenames[NUMFILES];


    for (int i = 0; i < NUMFILES / 2; i++)
    {
        filenames[i] = calloc(50, 1);
        sprintf(filenames[i], "./test_files/largevals%d.txt", i);
    }
    int *counter = populate_partitions(filenames, NUMFILES / 2, 1024, 53);

    struct row **file1 = calloc(1024 * 512, sizeof(struct row *));
    for (int i = 0; i < 512 * 1024; i++)
    {
        void * line = GetLines(stdin);
        if (!line) {
            printf("not enough output from 19.tmp\n");
            exit(1);
        }
        file1[i] = SplitCols(line);
    }
    int file1res = checker(file1, 512, 1024, 53);
    if (file1res)
    {
        printfile(file1, file1res, 53);
        return 1;
    }
    if (check_counts(counter, file1, NUMFILES/2, 1024, 53)){
        
        fprintf(stderr, "file 1 partition count mismatch\n");
        return 1;
    }
    free(file1);

    printf("ok");
    return 0;
}