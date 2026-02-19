#include <stdio.h>
#include "lib.h"
#include "minispark.h"

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("usage: ./grep <query> file1 ...\n");
    return -1;
  }

  MS_Run();

  RDD* files = RDDFromFiles(argv + 2, argc - 2);
  int matches = count(filter(map(files, GetLines), StringContains, argv[1]));

  printf("found %d matches\n", matches);

  MS_TearDown();
  

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }
  
  return 0;
}
