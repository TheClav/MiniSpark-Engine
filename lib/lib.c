#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>
#include "lib.h"

#define SLEEPNSEC 1E8 // 100 ms

float numnops = 0;

void measureNumNops(){
  
 struct timeval start, current;
 long elapsed = 0;
 gettimeofday(&start, NULL);

 for (int i = 0; i < 1E8; i++) {
    asm volatile ("nop\nnop\nnop\nnop\nnop\nnop\nnop\nnop\nnop\nnop\n");
 }

 gettimeofday(&current, NULL);
    elapsed = (current.tv_sec - start.tv_sec) * 1000 +
               (current.tv_usec - start.tv_usec) / 1000;
  numnops = 1E9 * (1.0/elapsed);
}

int getNumThreads(){
  struct timespec req = {.tv_sec = 0, .tv_nsec = SLEEPNSEC}; // 100 ms
  nanosleep(&req, NULL);
  int count = 0;
  DIR *dir = opendir("/proc/self/task");
  if (!dir) {
      perror("opendir");
      return -1;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
      // Skip the special entries "." and ".."
      if (entry->d_name[0] == '.')
          continue;
      count++;
  }
  closedir(dir);
  return count;
}

void* GetLines(void* arg) {
  FILE *fp = (FILE*)arg;

  char *line = NULL;
  size_t size = 0;
  
  if (getline(&line, &size, fp) < 0) {
    free(line);
    return NULL;
  }

  return line;
}

void SleepSec() {
  if (numnops == 0) { printf("error on the tester!!!! \n"); assert(0);};
  for (int i = 0; i < numnops*0.7; i++) {
    asm volatile ("nop\nnop\nnop\nnop\nnop\nnop\nnop\nnop\nnop\nnop\n");
 }
}

void *SleepSecMap(void *arg) {
  SleepSec();
  return arg;
}

int SleepSecFilter(void *arg, void* ctx) {
  (void)arg;
  (void)ctx;
  
  SleepSec();
  return 1;
}

void *SleepSec2(void *arg, void *arg2, void* ctx) {
  (void)arg2;
  (void)ctx;

  struct row* argcpy = malloc(sizeof(struct row));
  memcpy(argcpy, arg, sizeof(struct row));

  SleepSec();
  return argcpy;
}

// accepts a line of text as an argument, returns an array of columns
void* SplitCols(void* arg) {
  char *line = (char*)arg;

  struct row* row = malloc(sizeof(struct row));
  int nc = 0;
  char* ret;
  char* delim = " \t\n";
  char* saveptr;
  
  ret = strtok_r(line, delim, &saveptr);
  while (ret != NULL) {
    strncpy(row->cols[nc], ret, MAXLEN);
    row->cols[nc++][MAXLEN-1] = '\0';
    ret = strtok_r(NULL, delim, &saveptr);
  }
  row->ncols = nc;
  
  free(line);
  return (void*)row;
}

int StringContains(void* arg, void* needle) {
  if (strstr((char*)arg, (char*)needle)) {
    return 1;
  }
  free(arg);
  return 0;
}

// for row1 and row2, where each row has been split into columns
// if the key on column n matches, create a new row with two columns,
// the key and the sum of column m in the input rows.
// assume column n is a string and column m are digits
void* SumJoin(void* row1, void* row2, void* ctx) {
  struct sumjoin_ctx* c = (struct sumjoin_ctx*)ctx;
  struct row* data1 = (struct row*)row1;
  struct row* data2 = (struct row*)row2;
  struct row* row = NULL;

  if (!strcmp(data1->cols[c->keynum], data2->cols[c->keynum])) {
    row = malloc(sizeof(struct row));
    int res = atoi(data1->cols[c->target]) + atoi(data2->cols[c->target]);

    strncpy(row->cols[0], data1->cols[c->keynum], MAXLEN);
    snprintf(row->cols[1], MAXLEN, "%d", res);
    row->ncols = 2;
  }

  return (void*)row;
}


void *SumJoinSleep(void* row1, void* row2, void* ctx) {
  SleepSec();
  return SumJoin(row1, row2, ctx);
}

// assign row to a partition based on the hash of column n
unsigned long ColumnHashPartitioner(void* arg, int numpartitions, void* ctx) {
  struct colpart_ctx* c = (struct colpart_ctx*)ctx;
  
  unsigned long hash = 5381;
  char ch;
  struct row* row = (struct row*)arg;
  char* key = row->cols[c->keynum];
  while ((ch = *key++) != '\0')
    hash = hash * 33 + ch;
  
  return hash % numpartitions;
}

// assign string to a partition based on its hash
unsigned long StringHashPartitioner(void* arg, int numpartitions, void* ctx) {
  (void)ctx;

  unsigned long hash = 5381;
  char ch;
  char* str = (char*)arg;
  while ((ch = *str++) != '\0')
    hash = hash * 33 + ch;
  
  return hash % numpartitions;
}

void StringPrinter(void* arg) {
  char* str = (char*)arg;
  printf("%s", str);
}

void RowPrinter(void* arg) {
  struct row* data = (struct row*)arg;
  assert(data->ncols > 0);

  printf("%s", data->cols[0]);
  for (int i = 1; i < data->ncols; i++) {
    printf("\t%s", data->cols[i]);
  }
  printf("\n");
}
