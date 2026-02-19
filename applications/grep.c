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
  print(filter(map(files, GetLines), StringContains, argv[1]), StringPrinter);

  MS_TearDown();

  return 0;
}
