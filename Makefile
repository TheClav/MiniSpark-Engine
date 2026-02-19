CC = gcc
CFLAGS = -Wall -Wextra -O0 -g -pthread -I$(SOL_DIR) -I$(LIB_DIR)

APP_DIR = applications
LIB_DIR = lib
SOL_DIR = src
BIN_DIR = bin

PROGRAMS = linecount cat grep grepcount sumjoin concurrency

MS_OBJS = $(SOL_DIR)/minispark.o  #Put .o files 

OBJS = $(MS_OBJS) $(LIB_DIR)/lib.o
BINS = $(PROGRAMS:%=$(BIN_DIR)/%)

all: $(BIN_DIR) $(BINS)

$(BIN_DIR):
	mkdir -p $@

# compile all the bins
$(BIN_DIR)/%: $(APP_DIR)/%.o $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# compile all the objects
$(APP_DIR)/%.o: $(APP_DIR)/%.c
	$(CC) $(CFLAGS) -c -o $@ $^

$(SOL_DIR)/libminispark.a : $(MS_OBJS)
	ar rcs $@ $^

$(SOL_DIR)/%.o: $(SOL_DIR)/%.c
	$(CC) $(CFLAGS) -c -o $@ $^

$(LIB_DIR)/%.o: $(LIB_DIR)/%.c
	$(CC) $(CFLAGS) -c -o $@ $^

clean:
	rm -f $(BINARIES) $(APP_DIR)/*.o $(SOL_DIR)/*.o $(LIB_DIR)/*.o $(SOL_DIR)/*.a
	rm -rf $(BIN_DIR)
