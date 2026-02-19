#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sched.h>
#include <stdarg.h>
#include "lib.h"
#include "minispark.h"
#include <dirent.h>

#define ROUNDS 6
#define NUMFILES (1<<ROUNDS)

int main() {
  struct timeval start, end;
  measureNumNops();
  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(1);
  }
  
  int cpu_cnt = CPU_COUNT(&set);
  // Get starting time


  MS_Run();
  RDD* files[NUMFILES];
  struct sumjoin_ctx sctx;
  sctx.keynum = 0;
  sctx.target = 1;
  for (int i=0; i< NUMFILES; i++) {
    char *buffer = calloc(50,1);
    sprintf(buffer, "./test_files/%d", i);
    files[i] = map(map(map(RDDFromFiles(&buffer, 1), GetLines), SleepSecMap), SplitCols);
    free(buffer);
  }

  gettimeofday(&start, NULL);
  RDD* tmp = files[0];
  for (int i =0; i< (1<<(ROUNDS))-1; i++) {
    tmp = join(tmp, files[i+1], SumJoin, &sctx);
  }

  print(tmp, RowPrinter);

  // Get ending time
  gettimeofday(&end, NULL);

  MS_TearDown();
  // Calculate elapsed time in microseconds
  long seconds = end.tv_sec - start.tv_sec;
  long microseconds = end.tv_usec - start.tv_usec;
  double elapsed = seconds + microseconds * 1e-6;
  

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }

  double predict = (2*1.920/ (cpu_cnt-1))+3;
   if (elapsed < 0.1) {
      printf("Too fast! Are you evaluating before count()?");
    } else if (elapsed < predict)
    printf("ok");
    else
    printf("Too slow. elapsed %.2f predict %.2f\n", elapsed, predict);

  return 0;
}
