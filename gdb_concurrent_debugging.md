# Debugging Concurrent Applications in GDB

This guide will walk through some key commands for multithreaded debugging in gdb. There is an attatched example which might be useful!

## Key GDB Commands for Multithreading

### Listing Threads

To view all threads running in your program:

```sh
(gdb) info threads
```

Each thread will be assigned a thread number, which you can use to switch between them.

### Switching Between Threads

To switch focus to a specific thread:

```sh
(gdb) thread <thread_number>
(gdb) t <thread_number>
```

This allows you to inspect the execution state and variables of a particular thread.

### Locking the Scheduler

To ensure that only the current thread executes when stepping through code, use:

```sh
(gdb) set scheduler-locking on
```

This prevents other threads from running. If this is not enabled, even if the other threads may hit a specific breakpoint, once you continue or step through your current threads code, the other threads will also start executing.

Note: You can only use this once your application is running. If you face `Target 'exec' cannot support this command.` you must first break main, run your app and then call this command.

### Setting Breakpoints

You can set breakpoints as usual, and they will be hit when any thread reaches them:

```sh
(gdb) break <where>
(gdb) b <where>
```

To set a breakpoint for a specific thread:

```sh
(gdb) break <where> thread <thread_number>
```

`<where>` in this context refers to either the line number / function name you want to set a breakpoint at.

### Conditional Breakpoints

Sometimes we would like gdb to stop a thread at a specific breakpoint if certain conditions are true. We can set conditoinal breakpoints using the following command

```sh
(gdb) break <where> if <condition>
```

## Example: Debugging a Multithreaded Program

Consider the following simple multithreaded program:

```c
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define NUM_THREADS 3
#define N 10

int counter = 0;

void *increment_counter(void *arg) {
  for (int i = 0; i < N; i++) {
    counter++;
  }
  return NULL;
}

int main() {
  pthread_t threads[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_create(&threads[i], NULL, increment_counter, NULL);
  }

  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }
  printf("%d\n", counter);
  return 0;
}
```

We in this example will try to emulate the concurency bug where two threads try to add the same global variable without locks.

### Debugging Guide

As always, we need to compile and call gdb on our program :)

```sh
$ gcc -g -pthread -o threads_debug threads_debug.c
$ gdb ./threads_debug
```

Now once we are in gdb, we can set a breakpoint at the main and run our application. This is usually a standard practice when we don't know where our bug lies, and we just want to get a feel for where the bug could be. (Note that I am using the short forms for the `break` and `run` commands)

```sh
(gdb) b main
(gdb) r
```

At this point we can see what threads are available using the `info threads` command. The resulting output should be similar to this

```sh
(gdb) info threads
  Id   Target Id                                         Frame
* 1    Thread 0x7ffff7d72740 (LWP 58802) "threads_debug" main () at threads_debug.c:18
```

This is our initial main thread which is running.

Note: The default gdb UI showcasing one line of execution at a time is usually very tedious to look at. Another useful command which allows you to see the source code is `lay src` (or its variant `tui enable`).

With this setup we can begin debugging our application. We should first setup a breakpoint at the function we are interested in which in this case is increment_counter.

```sh
(gdb) b increment_counter
```

Now at this point it is very important to call the `set scheduler-locking on` command. Otherwise the threads will resume execution after they hit a breakpoint (try what happens when you execute the subsequent commands without locking on as an exercise). This command will efectively stop each thread at the increment_counter function and not let them proceed untill we call continue on them.

```sh
(gdb) set scheduler-locking on
```

We can now keep calling `next` (or the shorter alias `n`) on our main function until it is done creating all the threads (till line 25) (Incase you get switched to some other thread, just run `t 1` to go back to the main threads execution). Now at this point if you call `info threads` you should see the following result:

```sh
(gdb) info threads
  Id   Target Id                                           Frame
* 1    Thread 0x7ffff7d72740 (LWP 4026407) "threads_debug" main () at threads_debug.c:25
  2    Thread 0x7ffff7d71640 (LWP 4026707) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12
  3    Thread 0x7ffff7570640 (LWP 4026786) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12
  4    Thread 0x7ffff6d6f640 (LWP 4027531) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12

```

Notice how all the 3 threads are stuck at line 12 which is the start of increment_counter function. This wouldn't have been the case if scheduler-locking was off :)

Now, let's attempt to reproduce the concurrency bug. Our goal is to execute the threads in a way that both read the shared value into their registers, increment it independently, and then write back the same incremented value—overwriting each other's updates.

To achieve this, we switch to thread 2 and use the `next` command until we reach the increment instruction at line 13. At this point, we can observe the assembly instruction corresponding to `counter++`. The following sequence of commands will guide you through the process:

```sh
(gdb) t 2
(gdb) n
(gdb) lay asm
```

`lay asm` is another variant of the `lay` command which allows you to see the assembly instructions. With this you should be able to see the 3 assembly instructions our `counter++` command is compiled to:

```sh
mov    0x2e70(%rip),%eax
add    $0x1,%eax
mov    %eax,0x2e67(%rip)
```

We will call `ni` which is an alias for `nexti` which will execute one assembly instruction. We will move the shared value in our register, switch over to thread 3 and again call `ni` to move the old shared value to our other threads register. This will reproduce the concurency error.

Note: Another helpful command is `p $eax` if you would like to see the contents of the `%eax` register.

```sh
(gdb) ni
(gdb) t 3
(gdb) n
(gdb) ni
(gdb) p counter
$1 = 0
```

We print `counter` and observe that it is still 0, as neither thread has incremented and stored the value yet. Now, we will complete one iteration of the loop for both threads 2 and 3. We do this by repeatedly calling `ni` until the instruction `mov %eax, 0x2e67(%rip)` executes. Once that happens, we switch to thread 2, follow the same steps, and then print the value of `counter`.

```sh
(gdb) ni
(gdb) ni
(gdb) t 2
(gdb) ni
(gdb) ni
(gdb) lay src
(gdb) t 3
```

At this point, you should see that thread 2 has returned to line 12, ready to execute the next iteration of the loop. The same applies to thread 3.

```sh
(gdb) info thread
  Id   Target Id                                           Frame
  1    Thread 0x7ffff7d72740 (LWP 4026407) "threads_debug" main () at threads_debug.c:25
  2    Thread 0x7ffff7d71640 (LWP 4026707) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12
* 3    Thread 0x7ffff7570640 (LWP 4026786) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12
  4    Thread 0x7ffff6d6f640 (LWP 4027531) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12
(gdb) p counter
$2 = 1
```

Notice how the counter has incremented only once, when both the threads have executed one iteration of the for loop.

We can now let each thread just continue running. This will simulate a schedule where each thread is allowed to run untill completion. You can run a thread to completion using `continue` or just `c`.

```sh
(gdb) c
Continuing.
[Thread 0x7ffff7570640 (LWP 4026786) exited]
No unwaited-for children left.
(gdb) info threads
  Id   Target Id                                           Frame
  1    Thread 0x7ffff7d72740 (LWP 4026407) "threads_debug" main () at threads_debug.c:25
  2    Thread 0x7ffff7d71640 (LWP 4026707) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12
  4    Thread 0x7ffff6d6f640 (LWP 4027531) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12

The current thread <Thread ID 3> has terminated.  See 'help thread'.
(gdb) t 2
(gdb) info threads
(gdb) c
Continuing.
[Thread 0x7ffff7d71640 (LWP 4026707) exited]
No unwaited-for children left.
(gdb) info threads
  Id   Target Id                                           Frame
  1    Thread 0x7ffff7d72740 (LWP 4026407) "threads_debug" main () at threads_debug.c:25
  4    Thread 0x7ffff6d6f640 (LWP 4027531) "threads_debug" increment_counter (arg=0x0) at threads_debug.c:12

The current thread <Thread ID 2> has terminated.  See 'help thread'.
(gdb) t 4
(gdb) c
Continuing.
[Thread 0x7ffff6d6f640 (LWP 4027531) exited]
No unwaited-for children left.
(gdb) info threads
  Id   Target Id                                           Frame
  1    Thread 0x7ffff7d72740 (LWP 4026407) "threads_debug" main () at threads_debug.c:25

The current thread <Thread ID 4> has terminated.  See 'help thread'.
```

At this point, if you try printing the value of our counter, you will see the following result.

```sh
(gdb) p counter
$3 = 29
```

With this we have successfully emulated the concurency bug :)

As an exercise, try scheduling these three threads in a way that results in the counter holding the value `2` after execution. In general, for at least two threads, the counter will satisfy `2 <= counter <= NUM_THREADS * N`.

Hint:

```sh
(gdb) ???
.....
(gdb) b 13 thread 2 if i == 9
....
(gdb) t 2
(gdb) ni
.....
(gdb) t 2
(gdb) c
(gdb) p counter
$4 = 2
```
