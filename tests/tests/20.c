#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sched.h>
#include <stdarg.h>
#include "lib.h"
#include "minispark.h"

#define ROUNDS 5
#define NUMFILES (1<<ROUNDS)
#define FILENAMESIZE 100

int main() {

  char *filenames[NUMFILES];
  RDD* files[2];
  
  struct colpart_ctx pctx;
  pctx.keynum = 0;

  
  struct sumjoin_ctx sctx;
  sctx.keynum = 0;
  sctx.target = 1;

  for (int i=0; i< NUMFILES/2; i++) {
    filenames[i] = calloc(FILENAMESIZE,1);
    sprintf(filenames[i], "./test_files/largevals%d.txt", i);
     }
  for (int i=NUMFILES/2; i< NUMFILES; i++) {
    filenames[i] = calloc(FILENAMESIZE,1);
    sprintf(filenames[i], "./test_files/largevals%d.txt", i);
     }

     MS_Run();

  files[0] = partitionBy(map(map(RDDFromFiles(filenames, NUMFILES/2), GetLines), SplitCols), ColumnHashPartitioner, 64, &pctx);
  files[1] = partitionBy(map(map(RDDFromFiles((filenames + NUMFILES/2), NUMFILES/2), GetLines), SplitCols), ColumnHashPartitioner, 64, &pctx);
 
  print(join(files[0], files[1], SumJoin, &sctx), RowPrinter);
  for (int i=0; i< NUMFILES; i++) {
    free(filenames[i]);
     }
  MS_TearDown();
  

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }
  return 0;
}
