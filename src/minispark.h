#ifndef __minispark_h__
#define __minispark_h__

#include <pthread.h>

#define MAXDEPS (2)
#define TIME_DIFF_MICROS(start, end) \
  (((end.tv_sec - start.tv_sec) * 1000000L) + ((end.tv_nsec - start.tv_nsec) / 1000L))

struct RDD;
struct List;

typedef struct RDD RDD;   // forward decl. of struct RDD
typedef struct List List; // forward decl. of List.
// Minimally, we assume "list_add_elem(List *l, void*)"

// Different function pointer types used by minispark
typedef void *(*Mapper)(void *arg);
typedef int (*Filter)(void *arg, void *pred);
typedef void *(*Joiner)(void *arg1, void *arg2, void *arg);
typedef unsigned long (*Partitioner)(void *arg, int numpartitions, void *ctx);
typedef void (*Printer)(void *arg);

typedef enum
{
  MAP,
  FILTER,
  JOIN,
  PARTITIONBY,
  FILE_BACKED
} Transform;

// represent all partitions in RDD as a list
// Needs to store pointers to data elements (dynamic allocation)
/*
struct List
{
    int size;  // number of elements stored
    void** data; // array of pointers to elements
    int capacity; // max number of elements in data
    int track_index; // current index to start iterating through
};
*/

// create new list with intial capacity
// List* list_init(int capacity);
// add an element to list
// void add_list(List* list, void* element);
// get element at index
// void* get_element(List* list, int index);
// reset to start for iteration
// void reset(List* list);
// next element in iteration
// void* list_next(List* list);
// free all memory associated with list
// void free_list(List* list);

struct RDD
{
  Transform trans;  // transform type, see enum
  void *fn;         // transformation function
  void *ctx;        // used by minispark lib functions
  List *partitions; // list of partitions
  int numpartitions;
  RDD *dependencies[MAXDEPS];
  int numdependencies; // 0, 1, or 2

  // you may want extra data members here
  int materialized;
};

typedef struct
{
  struct timespec created;
  struct timespec scheduled;
  size_t duration; // in usec
  RDD *rdd;
  int pnum;
} TaskMetric;

typedef struct
{
  RDD *rdd;
  int pnum;
  TaskMetric *metric;
} Task;

//////// actions ////////

// Return the total number of elements in "dataset"
int count(RDD *dataset);

// Print each element in "dataset" using "p".
// For example, p(element) for all elements.
void print(RDD *dataset, Printer p);

//////// transformations ////////

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation.
RDD *map(RDD *rdd, Mapper fn);

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation. "ctx" should be passed to "fn"
// when it is called as a Filter
RDD *filter(RDD *rdd, Filter fn, void *ctx);

// Create an RDD with two dependencies, "rdd1" and "rdd2"
// "ctx" should be passed to "fn" when it is called as a
// Joiner.
RDD *join(RDD *rdd1, RDD *rdd2, Joiner fn, void *ctx);

// Create an RDD with "rdd" as a dependency. The new RDD
// will have "numpartitions" number of partitions, which
// may be different than its dependency. "ctx" should be
// passed to "fn" when it is called as a Partitioner.
RDD *partitionBy(RDD *rdd, Partitioner fn, int numpartitions, void *ctx);

// Create an RDD which opens a list of files, one per
// partition. The number of partitions in the RDD will be
// equivalent to "numfiles."
RDD *RDDFromFiles(char *filenames[], int numfiles);

//////// MiniSpark ////////
// Submits work to the thread pool to materialize "rdd".
void execute(RDD *rdd);

// Creates the thread pool and monitoring thread.
void MS_Run();

// Waits for work to be complete, destroys the thread pool, and frees
// all RDDs allocated during runtime.
void MS_TearDown();

#endif // __minispark_h__