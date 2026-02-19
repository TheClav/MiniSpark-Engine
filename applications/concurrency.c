#include <stdio.h>
#include <sys/time.h>
#include "lib.h"
#include "minispark.h"

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("need at least one file\n");
    return -1;
  }
  struct timeval start, end;

  MS_Run();
  // Get starting time
  gettimeofday(&start, NULL);

  RDD* files = RDDFromFiles(argv + 1, argc - 1);
  count(map(map(files, GetLines), SleepSecMap));
  // Call the function to measure

  MS_TearDown();
  
  // Get ending time
  gettimeofday(&end, NULL);

  // Calculate elapsed time in microseconds
  long seconds = end.tv_sec - start.tv_sec;
  long microseconds = end.tv_usec - start.tv_usec;
  double elapsed = seconds + microseconds * 1e-6;

  // Print elapsed time
  printf("Time elapsed: %.6f seconds\n", elapsed);

  return 0;
}
