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
  RDD* lines = map(files, GetLines);

  print(lines, StringPrinter);
  //  printf("lines found: %d\n", count(lines));

  MS_TearDown();

  return 0;
}
