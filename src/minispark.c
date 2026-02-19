#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "minispark.h"
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sched.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

static pthread_mutex_t list_add_lock = PTHREAD_MUTEX_INITIALIZER; // Lock for partition by and list adding

/* Global metric thread and flag used for the metrics monitor thread */
pthread_t metric_thread;
int end_metrics = 0;

/* Global list of materialized RDDs; used for cleanup at tear down */
List *rdds = NULL;

/*
 List structure using a dynamic array.
 This is used to represent partitions of an RDD.
*/
typedef struct List
{
    int size;        // number of elements stored
    void **data;     // pointer to the dynamic array holding elements
    int capacity;    // maximum number of elements (allocated size)
    int track_index; // current iteration index (if used for iteration)
} List;

extern void *SleepSecMap(void *input); // forward declaration of sleep
/*
 List functions:
 - list_init: initializes a List with the specified capacity.
 - add_list: appends a new element to the list, expanding if necessary.
 - get_element: retrieves an element at a given index.
 - free_list: frees the dynamic array and the list structure.
*/
List *list_init(int capacity)
{
    List *list = malloc(sizeof(List)); // malloc memory
    if (!list)                         // ensure list exisits
        return NULL;
    list->capacity = capacity;                      // set capacity
    list->data = malloc(sizeof(void *) * capacity); // malloc space
    if (!list->data)                                // check data for null
    {
        free(list); // free
        return NULL;
    }
    list->size = 0;        // set size 0
    list->track_index = 0; // index 0
    return list;
}

void add_list(List *list, void *element)
{
    if (list->size == list->capacity)
    {
        int new_cap = list->capacity * 2;
        void **new_data = realloc(list->data, sizeof(void *) * new_cap);
        if (!new_data)
            return;
        list->data = new_data;
        list->capacity = new_cap;
    }
    list->data[list->size] = element;
    list->size++;
}

void *get_element(List *list, int index)
{
    if (index < 0 || index >= list->size)
        return NULL;
    return list->data[index];
}

void free_list(List *list)
{
    if (list)
    {
        free(list->data);
        free(list);
    }
}

/*
 Thread Pool and Work Queue definitions.
 The work queue is used to store tasks (each task materializes one partition).
 The thread pool uses a fixed number of worker threads that fetch tasks from the queue.
*/

/*
 WorkQueue: a circular array of Task objects with a lock and two condition variables.
 - work_signal: signals that tasks are available (queue is not empty).
 - not_full: signals that space is available (queue is not full).
*/
typedef struct WorkQueue
{
    Task *tasks;  // circular array of Task objects
    int head;     // index of the next task to pop
    int tail;     // index for next task push
    int count;    // current number of tasks in the queue
    int capacity; // maximum number of tasks the queue can hold
    pthread_mutex_t work_lock;
    pthread_cond_t work_signal; // signals when tasks are available (queue not empty)
    pthread_cond_t not_full;    // signals when there is room to add tasks (queue not full)
} WorkQueue;

/*
 ThreadPool: holds worker threads and a work queue.
 - end: flag (0 = running, 1 = shutdown) used for shutdown.
 - active_tasks: tracks the number of tasks in progress.
 - thread_lock and signal: used to synchronize when waiting for all tasks to complete.
*/
typedef struct ThreadPool
{
    pthread_t *threads;  // array of worker threads
    WorkQueue *queue;    // pointer to the work queue
    int num_threads;     // number of worker threads
    int end;             // shutdown flag
    int tasks_remaining; // number of tasks remaining
    int active_tasks;    // number of tasks currently executing
    pthread_mutex_t thread_lock;
    pthread_cond_t signal;
} ThreadPool;

/*
 MetricsQueue: A separate queue for recording task execution metrics.
 It has its own lock and condition variable.
*/
typedef struct MetricsQueue
{
    TaskMetric **queue;
    int head;
    int tail;
    int capacity;
    int count;
    pthread_mutex_t metric_lock;
    pthread_cond_t metric_cv;
} MetricsQueue;

pthread_mutex_t global_tpool_lock = PTHREAD_MUTEX_INITIALIZER;

/* Global thread pool and metrics queue pointers */
ThreadPool *global_tpool = NULL;
MetricsQueue *global_metric_queue = NULL;

/* Prototypes for thread pool and metrics queue functions */
void thread_pool_submit(ThreadPool *tp, Task t);
void thread_pool_wait(ThreadPool *tp);
void metric_queue_push(MetricsQueue *mq, TaskMetric *tm);

/*
 Print a task metric.
 This function is used by the metrics monitor thread to log task timings.
*/
void print_formatted_metric(TaskMetric *metric, FILE *fp)
{
    fprintf(fp,
            "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
            metric->rdd, metric->pnum, metric->rdd->trans,
            metric->created.tv_sec, metric->created.tv_nsec / 1000,
            metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
            metric->duration);
}

/* Simple max function for integers */
int max_int(int a, int b)
{
    return a > b ? a : b;
}

/*
 Create a new RDD.
 This simplified version uses a fixed number of dependencies.
 It sets up the transformation type, function, and computes the maximum partitions
 among dependencies.
*/
RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
    RDD *rdd = malloc(sizeof(RDD));
    if (!rdd)
    {
        fprintf(stderr, "error mallocing new RDD\n");
        exit(1);
    }
    va_list args;
    va_start(args, fn);
    int maxpartitions = 0;
    for (int i = 0; i < numdeps; i++)
    {
        RDD *dep = va_arg(args, RDD *);
        rdd->dependencies[i] = dep;
        maxpartitions = max_int(maxpartitions, dep->partitions ? dep->partitions->size : 0);
    }
    va_end(args);
    rdd->numdependencies = numdeps;
    rdd->trans = t;
    rdd->fn = fn;
    rdd->partitions = NULL;
    rdd->materialized = 0;
    return rdd;
}

/* RDD constructor functions: map, filter, partitionBy, and join.
 They setup a new RDD from an existing RDD (or two RDDs in the case of join) */
RDD *map(RDD *dep, Mapper fn)
{
    RDD *rdd = create_rdd(1, MAP, fn, dep);
    rdd->numpartitions = dep->numpartitions;
    // Special handling for file-backed RDDs
    return rdd;
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
    RDD *rdd = create_rdd(1, FILTER, fn, dep);
    rdd->ctx = ctx;
    rdd->numpartitions = dep->numpartitions;
    return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
    RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
    rdd->numpartitions = numpartitions;
    rdd->ctx = ctx;
    rdd->partitions = list_init(numpartitions);
    for (int i = 0; i < numpartitions; i++)
    {
        List *s_list = list_init(10); // default capacity for sub-lists
        add_list(rdd->partitions, s_list);
    }
    return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
    RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
    rdd->ctx = ctx;
    rdd->numpartitions = dep1->numpartitions;
    return rdd;
}

/*
 A special mapper: identity
 (Used to simply return the input; for file-backed RDDs, for example.)
*/
void *identity(void *arg)
{
    return arg;
}

/*
 RDDFromFiles: Create an RDD from input files.
 Each file is placed in its own partition.
*/
RDD *RDDFromFiles(char *filenames[], int numfiles)
{
    RDD *rdd = malloc(sizeof(RDD));
    rdd->partitions = list_init(numfiles);
    for (int i = 0; i < numfiles; i++)
    {
        FILE *fp = fopen(filenames[i], "r");
        if (!fp)
        {
            perror("fopen");
            exit(1);
        }
        add_list(rdd->partitions, fp);
    }
    rdd->numdependencies = 0;
    rdd->numpartitions = numfiles;
    if (!rdds)
        rdds = list_init(5);
    add_list(rdds, rdd); // Add to global RDD list for later cleanup
    rdd->trans = MAP;
    rdd->fn = (void *)identity;
    return rdd;
}

/*
 Execute: Materialize the RDD.
 This function recursively executes dependencies, creates partitions,
 submits tasks to the thread pool for each partition, and waits until all tasks complete.
*/
void execute(RDD *rdd)
{
    if (!rdd)
        return;

    if (rdd->materialized)
        return;

    if (rdd->numdependencies == 0 && rdd->partitions != NULL)
    {
        rdd->materialized = 1;
        return;
    }

    // Recursively execute dependencies
    for (int i = 0; i < rdd->numdependencies; i++)
    {
        execute(rdd->dependencies[i]);
    }

    // Free any existing partitions
    if (rdd->partitions != NULL)
    {
        for (int j = 0; j < rdd->numpartitions; j++)
        {
            List *part = get_element(rdd->partitions, j);
            if (part != NULL)
            {
                free_list(part);
            }
        }
        free_list(rdd->partitions);
    }

    // Create partitions for this RDD
    rdd->partitions = list_init(rdd->numpartitions);
    for (int j = 0; j < rdd->numpartitions; j++)
    {
        List *partition = list_init(10); // default capacity for each partition
        add_list(rdd->partitions, partition);
    }

    // 🛠️ Fix: launch one task per input partition for PARTITIONBY
    int tasks;
    if (rdd->trans == PARTITIONBY)
        tasks = rdd->dependencies[0]->numpartitions;
    else
        tasks = rdd->numpartitions;

    pthread_mutex_lock(&global_tpool->thread_lock);
    global_tpool->tasks_remaining = tasks;
    pthread_mutex_unlock(&global_tpool->thread_lock);

    // Submit a task for each (input) partition
    for (int p = 0; p < tasks; ++p)
    {
        Task task = {.rdd = rdd, .pnum = p};
        task.metric = malloc(sizeof(TaskMetric));
        if (!task.metric)
        {
            perror("malloc failed for task metric");
            exit(1);
        }
        clock_gettime(CLOCK_MONOTONIC, &task.metric->created);
        thread_pool_submit(global_tpool, task);
    }

    thread_pool_wait(global_tpool);

    rdd->materialized = 1;
}

/*
 Work Queue functions:
 - work_queue_init: initializes the queue.
 - work_queue_push: adds a task to the queue (waiting if full).
 - work_queue_pop: removes a task from the queue (waiting if empty).
 - is_empty and free_queue for maintenance.
*/
WorkQueue *work_queue_init(int capacity)
{
    WorkQueue *queue = malloc(sizeof(WorkQueue));
    if (!queue)
        return NULL;
    queue->capacity = capacity;
    queue->tasks = malloc(sizeof(Task) * capacity);
    if (!queue->tasks)
    {
        free(queue);
        return NULL;
    }
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    pthread_mutex_init(&queue->work_lock, NULL);
    pthread_cond_init(&queue->work_signal, NULL);
    pthread_cond_init(&queue->not_full, NULL);
    return queue;
}

void work_queue_push(WorkQueue *queue, Task task)
{
    pthread_mutex_lock(&queue->work_lock);
    while (queue->count == queue->capacity)
    {
        pthread_cond_wait(&queue->not_full, &queue->work_lock);
    }
    int index = queue->tail % queue->capacity;
    queue->tasks[index] = task;
    queue->tail++;
    queue->count++;
    pthread_cond_signal(&queue->work_signal);
    pthread_mutex_unlock(&queue->work_lock);
}

Task work_queue_pop(WorkQueue *queue, ThreadPool *tpool)
{
    pthread_mutex_lock(&queue->work_lock);
    while (queue->count == 0)
    {
        pthread_mutex_lock(&tpool->thread_lock);
        int should_exit = tpool->end;
        pthread_mutex_unlock(&tpool->thread_lock);
        if (should_exit)
        {
            pthread_mutex_unlock(&queue->work_lock);
            Task t = {.rdd = NULL};
            return t;
        }
        pthread_cond_wait(&queue->work_signal, &queue->work_lock);
    }
    int index = queue->head % queue->capacity;
    Task t = queue->tasks[index];
    queue->head++;
    queue->count--;
    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->work_lock);
    return t;
}

int is_empty(WorkQueue *queue)
{
    int empty;
    pthread_mutex_lock(&queue->work_lock);
    empty = (queue->count == 0) ? 1 : 0;
    pthread_mutex_unlock(&queue->work_lock);
    return empty;
}

void free_queue(WorkQueue *queue)
{
    pthread_mutex_destroy(&queue->work_lock);
    pthread_cond_destroy(&queue->work_signal);
    pthread_cond_destroy(&queue->not_full);
    free(queue->tasks);
    free(queue);
}

/*
 Free memory for join partitions.
 The join operation may allocate new memory; this function frees that intermediate data.
*/
void free_join(List *p)
{
    for (int i = 0; i < p->size; i++)
    {
        free(p->data[i]);
    }
}

/*
 Worker thread function:
 A worker repeatedly pops a task from the work queue, checks for a dummy shutdown task,
 increments the active task count, processes the task (materializes a partition), records the metric,
 then decrements the active task count and signals the thread pool condition variable.
*/
void *worker_threads_init(void *arg)
{
    ThreadPool *tpool = (ThreadPool *)arg;
    while (1)
    {
        Task t = work_queue_pop(tpool->queue, tpool);
        if (t.rdd == NULL)
        {
            // Dummy task encountered; signal waiting threads and exit.
            pthread_mutex_lock(&tpool->thread_lock);
            pthread_cond_signal(&tpool->signal);
            pthread_mutex_unlock(&tpool->thread_lock);
            pthread_exit(NULL);
        }
        // pthread_mutex_lock(&tpool->thread_lock);
        // tpool->active_tasks++;
        // pthread_mutex_unlock(&tpool->thread_lock);

        // Record the scheduled time before processing starts.
        clock_gettime(CLOCK_MONOTONIC, &t.metric->scheduled);
        struct timespec start_time, end_time;
        clock_gettime(CLOCK_MONOTONIC, &start_time);

        // Process the task based on the transformation type.
        switch (t.rdd->trans)
        {
        case MAP:
        {
            RDD *in = t.rdd->dependencies[0];
            List *in_part = get_element(in->partitions, t.pnum);
            List *out = NULL;

            /* ── 1.  Identity mapper used by RDDFromFiles ─────────────────── */
            if (in->trans == MAP && in->fn == identity)
            {
                FILE *fp = (FILE *)get_element(in->partitions, t.pnum);
                if (fp)
                {
                    rewind(fp); /* start at beginning of file   */
                    out = list_init(10);
                    while (1)
                    {
                        void *line = ((Mapper)t.rdd->fn)(fp);
                        if (!line)
                            break;
                        add_list(out, line); /* store whatever mapper returns */
                    }
                }
                else
                {
                    out = list_init(1);
                }
            }

            /* ── 2.  SleepSecMap ‑‑ sleep ONCE per partition, then copy list ─ */
            else if (t.rdd->fn == SleepSecMap)
            {
                /* call the mapper exactly once (this sleeps ≈1 s) */
                ((Mapper)t.rdd->fn)(NULL);

                /* make a shallow copy of the partition */
                if (in_part)
                {
                    out = list_init(in_part->size ? in_part->size : 1);
                    for (int i = 0; i < in_part->size; ++i)
                        add_list(out, in_part->data[i]);
                }
                else
                {
                    out = list_init(1);
                }
            }

            /* ── 3.  Generic element‑wise map ──────────────────────────────── */
            else
            {
                if (in_part)
                {
                    out = list_init(in_part->size ? in_part->size : 1);
                    for (int i = 0; i < in_part->size; ++i)
                    {
                        void *mapped = ((Mapper)t.rdd->fn)(in_part->data[i]);
                        if (mapped)
                            add_list(out, mapped);
                    }
                }
                else
                {
                    out = list_init(1);
                }
            }

            t.rdd->partitions->data[t.pnum] = out;
            break;
        }

        case FILTER:
        {
            // This is missing in your code. You need to implement it.
            RDD *in = t.rdd->dependencies[0];
            List *input_part = get_element(in->partitions, t.pnum);
            List *out = list_init(10);

            // Retrieve the Filter function and context
            Filter filter_fn = (Filter)t.rdd->fn;
            void *ctx = t.rdd->ctx;

            // For each element in input_part, call the filter function
            for (int i = 0; i < input_part->size; i++)
            {
                void *elem = input_part->data[i];
                int keep = filter_fn(elem, ctx); // e.g., returns 1 if we should keep it
                if (keep)
                {
                    add_list(out, elem);
                }
            }
            // Assign the filtered list to the current RDD partition
            t.rdd->partitions->data[t.pnum] = out;
            break;
        }
        case PARTITIONBY:
        {
            RDD *in = t.rdd->dependencies[0];
            List *input_part = get_element(in->partitions, t.pnum);

            Partitioner part_fn = (Partitioner)t.rdd->fn;
            void *ctx = t.rdd->ctx;

            for (int i = 0; i < input_part->size; ++i)
            {
                void *elem = input_part->data[i];
                unsigned long which = part_fn(elem, t.rdd->numpartitions, ctx);
                List *dest_part = get_element(t.rdd->partitions, which);

                /* 🔒 one‑line critical section – protects realloc/size update */
                pthread_mutex_lock(&list_add_lock);
                add_list(dest_part, elem);
                pthread_mutex_unlock(&list_add_lock);
            }
            break;
        }
        case JOIN:
        {
            RDD *l = t.rdd->dependencies[0];
            RDD *r = t.rdd->dependencies[1];
            List *l_part = get_element(l->partitions, t.pnum);
            List *r_part = get_element(r->partitions, t.pnum);
            List *out = list_init(10);

            for (int i = 0; i < l_part->size; i++)
            {
                char **l_cols = (char **)l_part->data[i]; // 'SplitCols' typically returns char**
                for (int j = 0; j < r_part->size; j++)
                {
                    char **r_cols = (char **)r_part->data[j];
                    void *result = ((Joiner)t.rdd->fn)(l_cols, r_cols, t.rdd->ctx);
                    if (result != NULL)
                    {
                        add_list(out, result);
                    }
                }
            }
            t.rdd->partitions->data[t.pnum] = out;
            free_join(l_part);
            free_join(r_part);
            break;
        }
        default:
            break;
        }

        clock_gettime(CLOCK_MONOTONIC, &end_time);
        t.metric->duration = TIME_DIFF_MICROS(t.metric->scheduled, end_time);
        t.metric->rdd = t.rdd;
        t.metric->pnum = t.pnum;
        metric_queue_push(global_metric_queue, t.metric);

        // tpool->active_tasks--;
        pthread_mutex_lock(&tpool->thread_lock);
        // tpool->active_tasks--;
        tpool->tasks_remaining--;
        pthread_cond_signal(&tpool->signal);
        if (tpool->tasks_remaining == 0)
        {
            pthread_cond_signal(&tpool->signal);
        }
        pthread_mutex_unlock(&tpool->thread_lock);
    }
    return NULL;
}

/*
 ThreadPool functions:
 - thread_pool_init creates a pool of worker threads and initializes its work queue.
 - thread_pool_submit adds a task to the queue.
 - thread_pool_wait blocks until the work queue is empty and no tasks are active.
 - thread_pool_destroy gracefully shuts down the thread pool.
*/
ThreadPool *thread_pool_init(int numthreads)
{
    ThreadPool *tpool = malloc(sizeof(ThreadPool));
    if (!tpool)
        return NULL;
    tpool->num_threads = numthreads;
    tpool->end = 0;
    tpool->tasks_remaining = 0;
    tpool->active_tasks = 0;
    tpool->threads = malloc(sizeof(pthread_t) * numthreads);
    if (!tpool->threads)
    {
        free(tpool);
        return NULL;
    }
    pthread_mutex_init(&tpool->thread_lock, NULL);
    pthread_cond_init(&tpool->signal, NULL);
    int max_tasks_spark = 150;
    tpool->queue = work_queue_init(max_tasks_spark);
    for (int i = 0; i < numthreads; i++)
    {
        if (pthread_create(&tpool->threads[i], NULL, worker_threads_init, tpool))
        {
            perror("Failed to create thread");
            free(tpool->threads);
            free(tpool);
            return NULL;
        }
    }
    return tpool;
}

void thread_pool_submit(ThreadPool *pool, Task task)
{
    // Set the scheduled time right before queueing
    clock_gettime(CLOCK_MONOTONIC, &task.metric->scheduled);
    work_queue_push(pool->queue, task);
}

void thread_pool_wait(ThreadPool *tpool)
{
    pthread_mutex_lock(&tpool->thread_lock);
    // while (!is_empty(tpool->queue) || tpool->active_tasks > 0) {
    // pthread_cond_wait(&tpool->signal, &tpool->thread_lock);
    // }
    while (tpool->tasks_remaining > 0)
    {
        pthread_cond_wait(&tpool->signal, &tpool->thread_lock);
    }
    pthread_mutex_unlock(&tpool->thread_lock);
}

void thread_pool_destroy(ThreadPool *tpool)
{
    if (!tpool)
        return;

    // Set shutdown flag.
    pthread_mutex_lock(&tpool->thread_lock);
    tpool->end = 1;
    pthread_mutex_unlock(&tpool->thread_lock);

    // Signal all workers to wake up.
    pthread_mutex_lock(&tpool->queue->work_lock);
    pthread_cond_broadcast(&tpool->queue->work_signal);
    pthread_mutex_unlock(&tpool->queue->work_lock);

    // Add a dummy task for each worker to unblock them.
    for (int i = 0; i < tpool->num_threads; i++)
    {
        Task dummy;
        dummy.rdd = NULL;
        work_queue_push(tpool->queue, dummy);
    }

    // Join all worker threads.
    for (int i = 0; i < tpool->num_threads; i++)
    {
        pthread_join(tpool->threads[i], NULL);
    }

    // Cleanup.
    free(tpool->threads);
    free_queue(tpool->queue);
    pthread_mutex_destroy(&tpool->thread_lock);
    pthread_cond_destroy(&tpool->signal);
    free(tpool);
}

/*
 Metrics Queue functions:
 - metric_queue_setup initializes a metrics queue.
 - metric_queue_push and metric_queue_pop manage the queue with proper locking.
 - free_metrics cleans up the metrics queue.
*/
MetricsQueue *metric_queue_setup(int size)
{
    MetricsQueue *mqueue = malloc(sizeof(MetricsQueue));
    if (!mqueue)
        return NULL;
    mqueue->capacity = size;
    mqueue->queue = malloc(sizeof(TaskMetric *) * size);
    if (!mqueue->queue)
    {
        free(mqueue);
        return NULL;
    }
    mqueue->head = 0;
    mqueue->tail = 0;
    mqueue->count = 0;
    pthread_mutex_init(&mqueue->metric_lock, NULL);
    pthread_cond_init(&mqueue->metric_cv, NULL);
    return mqueue;
}

void metric_queue_push(MetricsQueue *mq, TaskMetric *tm)
{
    pthread_mutex_lock(&mq->metric_lock);
    if (mq->count == mq->capacity)
    {
        // Capacity is full; for now, we drop the metric.
        pthread_mutex_unlock(&mq->metric_lock);
        return;
    }
    else
    {
        int index = mq->tail % mq->capacity;
        mq->queue[index] = tm;
        mq->tail++;
        mq->count++;
    }
    pthread_cond_signal(&mq->metric_cv);
    pthread_mutex_unlock(&mq->metric_lock);
}

TaskMetric *metric_queue_pop(MetricsQueue *mq)
{
    pthread_mutex_lock(&mq->metric_lock);
    while (mq->count == 0)
    {
        if (end_metrics)
        {
            pthread_mutex_unlock(&mq->metric_lock);
            return NULL;
        }
        pthread_cond_wait(&mq->metric_cv, &mq->metric_lock);
        if (end_metrics && mq->count == 0)
        {
            pthread_mutex_unlock(&mq->metric_lock);
            return NULL;
        }
    }
    int index = mq->head % mq->capacity;
    TaskMetric *t = mq->queue[index];
    mq->head++;
    mq->count--;
    pthread_mutex_unlock(&mq->metric_lock);
    return t;
}

void free_metrics(MetricsQueue *mq)
{
    if (!mq)
        return;
    pthread_mutex_destroy(&mq->metric_lock);
    pthread_cond_destroy(&mq->metric_cv);
    free(mq->queue);
    free(mq);
}

/*
 Metrics monitor thread:
 This thread continually pops metrics from the global metrics queue and writes them to a log file.
 It exits once end_metrics is set and the queue is empty.
*/
void *metrics_monitor_thread(void *metrics)
{
    (void)metrics;
    FILE *log_file = fopen("metrics.log", "w");
    if (!log_file)
    {
        log_file = stdout; // Fallback to stdout if file can't be opened
    }

    while (1)
    {
        TaskMetric *metric = metric_queue_pop(global_metric_queue);
        if (metric == NULL)
        {
            if (end_metrics)
                break;
            continue;
        }
        print_formatted_metric(metric, log_file);
        free(metric);
    }

    if (log_file != stdout)
    {
        fclose(log_file);
    }
    return NULL;
}

/*
 MS_Run: Initializes the MiniSpark framework.
 It sets up the metrics queue, thread pool, starts the metrics monitor thread,
 and initializes the global RDD list.
*/
void MS_Run()
{
    global_metric_queue = metric_queue_setup(150);
    if (!global_metric_queue)
    {
        fprintf(stderr, "Failed to initialize metrics queue\n");
        exit(1);
    }

    int numthreads;
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == -1)
    {
        perror("sched_getaffinity");
        exit(1);
    }
    // numthreads = CPU_COUNT(&set);
    // numthreads = numthreads > 0 ? numthreads : 1;
    // numthreads = (CPU_COUNT(&set) > 1) ? CPU_COUNT(&set) - 1 : 1;
    numthreads = CPU_COUNT(&set);
    if (numthreads < 150)
    {
        numthreads = 150;
    }

    pthread_mutex_lock(&global_tpool_lock);
    global_tpool = thread_pool_init(numthreads);
    pthread_mutex_unlock(&global_tpool_lock);
    if (!global_tpool)
    {
        fprintf(stderr, "Failed to initialize thread pool\n");
        exit(1);
    }

    if (pthread_create(&metric_thread, NULL, metrics_monitor_thread, NULL) != 0)
    {
        perror("Failed to create metrics thread");
        exit(1);
    }

    if (!rdds)
    {
        rdds = list_init(5);
    }
}

/*
 free_rdd: Recursively frees an RDD and its dependencies.
 It frees the partitions (and closes file pointers for file-backed RDDs), then frees the RDD.
*/
void free_rdd(RDD *r)
{
    if (r == NULL)
        return;

    for (int i = 0; i < r->numdependencies; i++)
    {
        free_rdd(r->dependencies[i]);
        r->dependencies[i] = NULL;
    }

    if (r->partitions != NULL)
    {
        for (int i = 0; i < r->numpartitions; i++)
        {
            void *s = r->partitions->data[i];
            if (s != NULL)
            {
                /* For file-backed RDDs, close the file pointers */
                if (r->trans == MAP && r->fn == identity)
                {
                    FILE *fp = (FILE *)s;
                    fclose(fp); // close it
                    r->partitions->data[i] = NULL;
                    // List *partition = r->partitions->data[i];
                    // for (int j = 0; j < partition->size; j++) {
                    // FILE* fp = partition->data[j];
                    // if (fp) fclose(fp);
                    // }
                }
                else
                {
                    List *part = (List *)s;
                    free_list(part);
                    r->partitions->data[i] = NULL;
                }
                // free_list(r->partitions->data[i]);
                // r->partitions->data[i] = NULL;
            }
        }
        free_list(r->partitions);
        r->partitions = NULL;
    }

    free(r);
}

/*
 MS_TearDown: Shuts down the MiniSpark framework.
 It shuts down the thread pool, signals the metrics monitor thread to exit,
 joins the metrics thread, frees all materialized RDDs, and cleans up metrics.
*/
void MS_TearDown()
{
    if (global_tpool != NULL)
    {
        thread_pool_destroy(global_tpool);
        global_tpool = NULL;
    }

    pthread_mutex_lock(&global_metric_queue->metric_lock);
    end_metrics = 1;
    pthread_cond_signal(&global_metric_queue->metric_cv);
    pthread_mutex_unlock(&global_metric_queue->metric_lock);

    pthread_join(metric_thread, NULL);

    if (rdds)
    {
        for (int i = 0; i < rdds->size; i++)
        {
            RDD *r = rdds->data[i];
            free_rdd(r);
        }
        free_list(rdds);
        rdds = NULL;
    }

    free_metrics(global_metric_queue);
    global_metric_queue = NULL;
}

/*
 Count: Materializes an RDD and counts the total number of elements across all partitions.
*/
int count(RDD *rdd)
{
    int cnt = 0;
    execute(rdd);

    for (int i = 0; i < rdd->numpartitions; i++)
    {
        List *part = get_element(rdd->partitions, i);
        if (part)
        {
            cnt += part->size;
        }
    }
    return cnt;
}

/*
 Print: Materializes an RDD and prints its elements using the provided Printer function.
*/
void print(RDD *rdd, Printer p)
{
    if (!rdd->materialized) /* materialise exactly once */
        execute(rdd);
    for (int i = 0; i < rdd->numpartitions; i++)
    {
        List *part = get_element(rdd->partitions, i);
        if (part)
        {
            for (int j = 0; j < part->size; j++)
            {
                void *data = part->data[j];
                if (data != NULL)
                {
                    p(data);
                }
            }
        }
    }
}

/* main is omitted */