#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sched.h>
#include <stdarg.h>
#include "lib.h"
#include "minispark.h"

#define ROUNDS 9
#define NUMFILES (1<<ROUNDS)

int main() {
  
  MS_Run();

  RDD* files[10][NUMFILES];
  struct sumjoin_ctx sctx;
  sctx.keynum = 0;
  sctx.target = 1;
  for (int i=0; i< NUMFILES; i++) {
    char *buffer = calloc(20,1);
    sprintf(buffer, "./test_files/%d", i);
    files[0][i] = map(map(RDDFromFiles(&buffer, 1), GetLines), SplitCols);
    free(buffer);
  }

  
  for (int round=0; round < ROUNDS; round++) {
    for (int i =0; i< (1<<(ROUNDS-1-round)); i++) {
        files[(round+1)][i] = join(files[round][2*i], files[round][2*i+1], SumJoin, &sctx); 
    }
  }

  print(files[ROUNDS][0], RowPrinter);

  MS_TearDown();

  

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }
  return 0;
}
