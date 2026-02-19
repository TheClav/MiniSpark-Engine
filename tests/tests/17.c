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
  RDD* files[NUMFILES];
  struct sumjoin_ctx sctx;
  sctx.keynum = 0;
  sctx.target = 1;
  for (int i=0; i< NUMFILES; i++) {
    char *buffer = calloc(50,1);
    sprintf(buffer, "./test_files/%d", i);
    files[i] = map(map(RDDFromFiles(&buffer, 1), GetLines), SplitCols);
    free(buffer);
  }
  
  for (int i =0; i< (1<<(ROUNDS))-1; i++) {
    RDD* tmp = join(files[i], files[i+1], SumJoin, &sctx);

    files[i+1] = tmp;
  }

  print(files[(1<<ROUNDS)-1], RowPrinter);

  MS_TearDown();

  return 0;
}
