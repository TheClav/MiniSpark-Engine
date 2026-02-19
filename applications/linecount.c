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
  int totalnumlines = count(map(files, GetLines));

  MS_TearDown();

  printf("total number of lines in all files: %d\n", totalnumlines);
  return 0;
}
