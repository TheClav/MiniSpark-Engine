#include <stdio.h>
#include <stdlib.h>
#include "lib.h"
#include "minispark.h"

// - join on column n, sum column m for each joined key
// - if there are only 2 files, we don't use partitionby
// - else, divide the input files in half and use partitionby before joining
// we assume there are no duplicate keys within a file (i.e., no reduce needed)

int main(int argc, char* argv[]) {
  if (argc < 4) {
    printf("usage: sum-join n m files ...\n");
    exit(1);
  }

  MS_Run();
  int numfiles = argc - 3;
  char** files = argv + 3;
  
  struct sumjoin_ctx sctx;
  sctx.keynum = atoi(argv[1]);
  sctx.target = atoi(argv[2]);

  if (numfiles == 2) {
    RDD* data1 = map(map(RDDFromFiles(files, 1), GetLines), SplitCols);
    RDD* data2 = map(map(RDDFromFiles(files + 1, 1), GetLines), SplitCols);
    print(join(data1, data2, SumJoin, (void*)&sctx), RowPrinter);
  } else {
    int group1 = numfiles / 2;
    int group2 = numfiles - group1;
    
    RDD* data1 = map(map(RDDFromFiles(files, group1), GetLines), SplitCols);
    RDD* data2 = map(map(RDDFromFiles(files + group1, group2), GetLines), SplitCols);

    struct colpart_ctx pctx;
    pctx.keynum = 0;
    RDD* repart1 = partitionBy(data1, ColumnHashPartitioner, 4, &pctx);
    RDD* repart2 = partitionBy(data2, ColumnHashPartitioner, 4, &pctx);

    print(join(repart1, repart2, SumJoin, (void*)&sctx), RowPrinter);
  }
  MS_TearDown();
  

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }
  return 0;
}
