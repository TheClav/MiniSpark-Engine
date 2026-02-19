#define MAXCOLS (10)
#define MAXLEN (32)
#include <dirent.h>

void measureNumNops();

int getNumThreads();
// we statically allocate the number and length of columns to simplify
// memory management.
struct row {
  char cols[MAXCOLS][MAXLEN];
  int ncols;
};

struct sumjoin_ctx {
  int keynum;
  int target;
};

struct colpart_ctx {
  int keynum;
};

// Mappers
// arg: a char*. Assume whitespace-delimited text.
// returns: `struct row`
void* SplitCols(void* arg);

// arg: an opened FILE*
// returns: a char* or NULL if EOFW
void* GetLines(void* arg);

// A function to test concurrency
void* SleepSecMap(void *arg);
int SleepSecFilter(void *arg, void* ctx);
void *SleepSec2(void *arg, void *arg2, void* ctx);
void *SumJoinSleep(void* row1, void* row2, void* ctx);

// Filters
// arg: char* string
// needle: char* string
// returns: 1 if arg contains needle, or 0.
int StringContains(void* arg, void* needle);

// Joiners
// row1, row2: `struct row` to be joined
// ctx: key (column number) for inner join, and target column to sum
// returns: new `struct row` containing the key and sum
void* SumJoin(void* row1, void* row2, void* ctx);

// Partitioners
// arg: `struct row`
// ctx: column number to hash, and number of output partitions
// returns: output partition
unsigned long ColumnHashPartitioner(void* arg, int numpartitions, void* ctx);

// arg: char*
// ctx: number of output partitions
// returns: output partition
unsigned long StringHashPartitioner(void* arg, int numpartitions, void* ctx);

// Printers
// arg: thing to print
void StringPrinter(void* arg);
void RowPrinter(void* arg);
