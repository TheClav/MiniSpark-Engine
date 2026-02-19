CC = gcc
CFLAGS = -Wall -Wextra -Og -g -pthread -I$(SOL_DIR) -I$(LIB_DIR)
TSAN_CFLAGS = -fsanitize=thread

APP_DIR = .
LIB_DIR = ../../lib
SOL_DIR = ../../src
BIN_DIR = .

PROGRAMS = 1.tmp 2.tmp 3.tmp 5.tmp 11.tmp 12.tmp 13.tmp 14.tmp 15.tmp 18.tmp 19.tmp 20.tmp 7.tmp 8.tmp 9.tmp 10.tmp 16.tmp 4.tmp 6.tmp
PROGRAMS_TSAN = 17.tmp 
CHECKERS = 19checker.tmp 

# List all solution sources and create object lists (normal versus TSAN)
SOLUTION_SRCS := $(wildcard $(SOL_DIR)/*.c)
SOLUTION_OBJS := $(SOLUTION_SRCS:%.c=%.o)
TSAN_SOL_OBJS := $(SOLUTION_SRCS:$(SOL_DIR)/%.c=$(SOL_DIR)/tsan_%.o)

OBJS = $(LIB_DIR)/lib.o
TSAN_LIB_OBJS = $(LIB_DIR)/tsan_lib.o

all: $(PROGRAMS) $(PROGRAMS_TSAN) $(CHECKERS)

# --- Standard Build Rules ---
$(PROGRAMS): %.tmp : $(APP_DIR)/%.o $(SOLUTION_OBJS) $(LIB_DIR)/lib.o
	$(CC) $(CFLAGS) -o $@ $^

$(CHECKERS): %.tmp : $(APP_DIR)/%.o $(LIB_DIR)/lib.o
	$(CC) $(CFLAGS) -o $@ $^

# Standard object compilation rules
$(APP_DIR)/%.o: $(APP_DIR)/%.c
	$(CC) $(CFLAGS) -c -o $@ $<

$(SOL_DIR)/%.o: $(SOL_DIR)/%.c
	$(CC) $(CFLAGS) -c -o $@ $<

$(LIB_DIR)/%.o: $(LIB_DIR)/%.c
	$(CC) $(CFLAGS) -c -o $@ $<

# --- TSAN Build Rules ---
# Link TSAN programs from separate TSAN object files
$(PROGRAMS_TSAN): %.tmp : $(APP_DIR)/tsan_%.o $(TSAN_SOL_OBJS) $(TSAN_LIB_OBJS)
	$(CC) $(CFLAGS) $(TSAN_CFLAGS) -o $@ $^

# TSAN-specific object compilation rules (note the added TSAN_CFLAGS)
$(APP_DIR)/tsan_%.o: $(APP_DIR)/%.c
	$(CC) $(CFLAGS) $(TSAN_CFLAGS) -c -o $@ $<

$(SOL_DIR)/tsan_%.o: $(SOL_DIR)/%.c
	$(CC) $(CFLAGS) $(TSAN_CFLAGS) -c -o $@ $<

$(LIB_DIR)/tsan_lib.o: $(LIB_DIR)/lib.c
	$(CC) $(CFLAGS) $(TSAN_CFLAGS) -c -o $@ $<



clean:
	rm -f *.tmp $(APP_DIR)/*.o $(SOL_DIR)/*.o $(LIB_DIR)/*.o
