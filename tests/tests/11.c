#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sched.h>
#include <stdarg.h>
#include "lib.h"
#include "minispark.h"

int main() {

  measureNumNops();

  struct timeval start, end;

  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(1);
  }
  
  int cpu_cnt = CPU_COUNT(&set);
  // Get starting time
  char *filenames[1000];
  for (int i=0; i< 1000; i++) {
    char *buffer = calloc(30,1);
    sprintf(buffer, "./test_files/%d", i);
    filenames[i] = buffer;
  }

  MS_Run();
  RDD* files = RDDFromFiles(filenames, 1000);
  gettimeofday(&start, NULL);
  count(map(map(files, GetLines), SleepSecMap));
  // Call the function to measure

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


  // Check if it scales. 
  double predict = ((3*2*10.0)/ (cpu_cnt-1))+3;
  if (elapsed < 1) {
    printf("Too fast! Are you evaluating before count()?");
  }else if (elapsed < predict)
    printf("ok");
  else 
    printf("Too slow. elapsed %.2f predict %.2f\n", elapsed, predict);

  for (int i=0; i< 1000; i++) {
    free(filenames[i]);
  }
  
  return 0;
}
