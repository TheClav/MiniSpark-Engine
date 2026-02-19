#include <stdio.h>
#include <stdlib.h>
#include "lib.h"
#include "minispark.h"


void* CountZero(void* arg) {
  FILE *fp = (FILE*)arg;
 
  char buffer[1000];
  int total_read = 0;
  int *cnt = calloc(1, sizeof(int));
  int tmp;
  while ((tmp = fread(buffer, 1, 999, fp))){
    for (int i =0; i< tmp; i++){
      if (buffer[i] == '0') *cnt+=1;
    }
    total_read += tmp;
  }
  if (total_read)
    return cnt;
  free(cnt);
  return NULL;
}

void IntPrinter(void* arg) {
  int* v = (int *)arg;
  printf("%d\n", *v);
}



int main() {

  
  char *filenames[1000];
  for (int i=0; i< 1000; i++) {
    filenames[i] = calloc(100,1);
    sprintf(filenames[i], "./test_files/largevals%d.txt", i);
  }

  MS_Run();

  RDD* files = RDDFromFiles(filenames, 1000);
  print(map(files, CountZero), IntPrinter);

  MS_TearDown();

  for (int i=0; i< 1000; i++) {
    free(filenames[i]);
  }
  

  int num_threads = getNumThreads();
  if (num_threads > 1) {
    printf("Worker threads didn't terminate\n");
    return 0;
  }
  return 0;
}
