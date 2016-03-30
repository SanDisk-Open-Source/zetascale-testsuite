# $Id: Makefile 9983 2012-11-08 17:07:42 fgao $

FDF_SDK_DIR = ../zs_sdk

PROG = ../bin/zs_test_engine

SRCS = \
	zs_test_engine.c \
	thread.c
   

LIBS = $(FDF_SDK_DIR)/lib/libzsdll.a \
	-lpthread -lrt -lnsl  -lutil -ldl -lm -laio -levent -llz4
CFLAGS = -g -O3 -std=gnu99 -fgnu89-inline -fstack-protector-all \
    	-D_GNU_SOURCE -DUSE_ZS_API -DUSE_BTREE -DUSE_SNAPSHOT -DUSE_3_1 

all:
	$(CC) $(CFLAGS) $(SRCS) $(LIBS) -I$(FDF_SDK_DIR)/include -o $(PROG)

.PHONY: clean

clean:
	$(RM) *.o $(PROG)
