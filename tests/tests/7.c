#include <stdio.h>
#include "lib.h"
#include "minispark.h"

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("need at least one file\n");
    return -1;
  }

  MS_Run();
  RDD* files = RDDFromFiles(argv + 1, argc - 1);
  print(map(files, GetLines), StringPrinter);

  MS_TearDown();
  

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }
  return 0;
}
