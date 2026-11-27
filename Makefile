CC = gcc
CFLAGS = -Wall -Wextra -O2 -pthread
LDFLAGS = -pthread -lrt

all: start proc

start: start.c common.c common.h
	$(CC) $(CFLAGS) start.c common.c -o start $(LDFLAGS)

proc: proc.c common.c common.h
	$(CC) $(CFLAGS) proc.c common.c -o proc $(LDFLAGS)

clean:
	rm -f start proc 