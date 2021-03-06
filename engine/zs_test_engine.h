//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 *  zs_test_engine.h
 *
 *  Copyright 2012 Sandisk, Inc.  All rights reserved.
 *
 *  Authors:
 *      yiwen sun <yiwensun@hengtiansoft.com>
 *
 */
#ifndef ZS_TEST_ENGINE_H
#define ZS_TEST_ENGINE_H

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <netdb.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#ifdef USE_ZS_API
#include "zs.h"
#elif defined USE_SDF_API
#include "api/sdf.h"
#endif

#define IS_UDP(x) (x == udp_transport)

#define DATA_BUFFER_SIZE 2048

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 10

#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8

#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
#define MCD_MAX_NUM_CNTRS 68000

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

typedef struct conn conn;

/**
* Stats stored per-thread.
*/
struct thread_stats {
    pthread_mutex_t   mutex;
    uint64_t          get_cmds;
    uint64_t          get_misses;
    uint64_t          touch_cmds;
    uint64_t          touch_misses;
    uint64_t          delete_misses;
    uint64_t          incr_misses;
    uint64_t          decr_misses;
    uint64_t          cas_misses;
    uint64_t          bytes_read;
    uint64_t          bytes_written;
    uint64_t          flush_cmds;
    uint64_t          conn_yields; /* # of yields for connections (-R option)*/
    uint64_t          auth_cmds;
    uint64_t          auth_errors;
};


/** 
* Global stats.
*/ 
struct stats {
    pthread_mutex_t mutex; 
    unsigned int  curr_items;
    unsigned int  total_items;
    uint64_t      curr_bytes;
    unsigned int  curr_conns;
    unsigned int  total_conns;
    unsigned int  conn_structs;
    uint64_t      get_cmds; 
    uint64_t      set_cmds;
    uint64_t      get_hits;
    uint64_t      get_misses;
    uint64_t      evictions;
    uint64_t      reclaimed;   
    time_t        started;          /* when the process was started */
    bool          accepting_conns;  /* whether we are currently accepting */
    uint64_t      listen_disabled_num;
};

/*
* NOTE: If you modify this table you _MUST_ update the function state_text
*/
/**
* Possible states of a connection.
*/
enum conn_states {
    conn_listening,  /**< the socket which listens for connections */
    conn_new_cmd,    /**< Prepare connection for next command */
    conn_waiting,    /**< waiting for a readable socket */
    conn_read,       /**< reading in a command line */
    conn_parse_cmd,  /**< try to parse a command from the input buffer */
    conn_write,      /**< writing out a simple response */
    conn_nread,      /**< reading in a fixed number of bytes */
    conn_swallow,    /**< swallowing unnecessary bytes w/o storing */
    conn_closing,    /**< closing this connection */
    conn_mwrite,     /**< writing out many items sequentially */
    conn_max_state   /**< Max state value (used for assertion) */
};

enum protocol {
    ascii_prot = 3, /* arbitrary value. */
    binary_prot,
    negotiating_prot /* Discovering the protocol */
};

enum network_transport {
    local_transport, /* Unix sockets*/
    tcp_transport,
    udp_transport
};

/* When adding a setting, be sure to update process_stat_settings */
/**
* Globally accessible settings as derived from the commandline.
*/
struct settings {
    size_t maxbytes;
    int maxconns;
    int port;
    int udpport;
    char *inter;
    int verbose;
    int evict_to_free;
    char *socketpath;   /* path to unix socket if using local socket */
    int access;  /* access mask (a la chmod) for unix domain socket */
    double factor;          /* chunk size growth factor */
    int chunk_size;
    int num_threads;        /* number of worker (without dispatcher) libevent threads to run */
    char prefix_delimiter;  /* character that marks a key prefix (for stats) */
    int detail_enabled;     /* nonzero if we're collecting detailed stats */
    int reqs_per_event;     /* Maximum number of io to process on each
                               io-event. */
    bool use_cas;
    enum protocol binding_protocol;
    int backlog;
    int item_size_max;        /* Maximum item size, and upper end for slabs */
    bool sasl;              /* SASL on/off */

    int zs_reformat;
    int zs_admin_port;
    char *zs_stats_file;
    char *zs_log_file;
    char *zs_log_flush_dir;
    char *zs_crash_dir;
    char *shmem_basedir;
    char *zs_flash_filename;
    char *zs_flog_mode;
    char *zs_flog_nvram_file;
    int zs_flog_nvram_file_offset;
};

extern struct stats stats;
extern time_t process_started;
extern struct settings settings;

typedef struct {
    pthread_t thread_id;        /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    int notify_receive_fd;      /* receiving end of notify pipe */
    int notify_send_fd;         /* sending end of notify pipe */
    struct thread_stats stats;  /* Stats generated by this thread */
    struct conn_queue *new_conn_queue; /* queue of new connections to handle */
    uint8_t item_lock_type;     /* use fine-grained or global item lock */
#ifdef USE_ZS_API
    struct ZS_thread_state *zs_thrd_state;
#elif defined USE_SDF_API
    struct SDF_thread_state *sdf_thrd_state;
#endif
} LIBEVENT_THREAD;

typedef struct {
    pthread_t thread_id;        /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
} LIBEVENT_DISPATCHER_THREAD; 

/**
* The structure representing a connection into memcached.
*/
struct conn {
    int    sfd;
    enum conn_states  state;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */

    char   *rbuf;   /** buffer to read commands into */
    char   *rcurr;  /** but if we parsed some already, this is where we stopped */
    int    rsize;   /** total allocated size of rbuf */
    int    rbytes;  /** how much data, starting from rcur, do we have unparsed */

    char   *wbuf;
    char   *wcurr;
    int    wsize;
    int    wbytes;
    /** which state to go into after finishing current write */
    enum conn_states  write_and_go;
    void   *write_and_free; /** free this memory after finishing writing */

    char   *ritem;  /** when we read in an item's value, it goes here */
    int    rlbytes;

    /* data for the nread state */

    /** 
    * item is used to hold an item structure created after reading the command
    * line of set/add/replace commands, but before we finished reading the actual
    * data. The data is read into ITEM_data(item) to avoid extra copying.
    */

    void   *item;     /* for commands set/add/replace  */

    /* data for the swallow state */
    int    sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */

    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

//    item   **ilist;   /* list of items to write out */
    int    isize;
//    item   **icurr;
    int    ileft;

    char   **suffixlist;
    int    suffixsize;
    char   **suffixcurr;
    int    suffixleft;

    enum protocol protocol;   /* which protocol this connection speaks */
    enum network_transport transport; /* what transport is used by this connection */

    /* data for UDP clients */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char *hdrbuf; /* udp packet headers */
    int    hdrsize;   /* number of headers' worth of space is allocated */

    bool   noreply;   /* True if the reply should not be sent. */
    /* current stats command */
    struct {
        char *buffer;
        size_t size;
        size_t offset;
    } stats;

    /* Binary protocol stuff */
    /* This is where the binary header goes */
//    protocol_binary_request_header binary_header;
    uint64_t cas; /* the cas to return */
    short cmd; /* current command being processed */
    int opaque;
    int keylen;
    conn   *next;     /* Used for generating a list of conn structures */
    LIBEVENT_THREAD *thread; /* Pointer to the thread object serving this connection */
 
#ifdef USE_ZS_API
    struct ZS_thread_state *zs_thd_state;
    struct ZS_iterator *iterator;
#elif defined USE_SDF_API
    struct SDF_thread_state *sdf_thd_state;
    struct SDF_iterator *iterator;
#endif

#ifdef USE_BTREE
    struct ZS_cursor *cursor;
    ZS_range_meta_t  rmeta;
#endif
};


void STATS_LOCK(void);
void STATS_UNLOCK(void);

/*
 * Functions
  */
void do_accept_new_conns(const bool do_accept);
conn *conn_new(const int sfd, const enum conn_states init_state, const int event_flags, const int read_buffer_size, enum network_transport transport, struct event_base *base);


/*
* Functions such as the libevent-related calls that need to do cross-thread
* communication in multithreaded mode (rather than actually doing the work
* in the current thread) are called via "dispatch_" frontends, which are
* also #define-d to directly call the underlying code in singlethreaded mode.
*/ 

void thread_init(int nthreads, struct event_base *main_base);
void dispatch_conn_new(int sfd, enum conn_states init_state, int event_flags, int read_buffer_size, enum network_transport transport);

void accept_new_conns(const bool do_accept);


#endif
