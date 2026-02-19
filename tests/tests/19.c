#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sched.h>
#include <stdarg.h>
#include "lib.h"
#include "minispark.h"

#define ROUNDS 10
#define NUMFILES (1<<ROUNDS)
#define FILENAMESIZE 100

int main() {

  MS_Run();
  char *filenames[NUMFILES];
  RDD* files[2];
  
  struct colpart_ctx pctx;
  pctx.keynum = 0;

  for (int i=0; i< NUMFILES/2; i++) {
    filenames[i] = calloc(FILENAMESIZE,1);
    sprintf(filenames[i], "./test_files/largevals%d.txt", i);
     }

  files[0] = partitionBy(map(map(RDDFromFiles(filenames, NUMFILES/2), GetLines), SplitCols), ColumnHashPartitioner, 128, &pctx);
  files[0] = partitionBy(files[0], ColumnHashPartitioner, 53, &pctx);

  print(files[0], RowPrinter);
  
  MS_TearDown();
  for (int i=0; i< NUMFILES/2; i++) {
    free(filenames[i]);
     }
     

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }
  return 0;
}
