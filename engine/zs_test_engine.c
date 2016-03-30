/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 *  zs_test_engine
 *
 *  Copyright 2012 Sandisk, Inc.  All rights reserved.
 *
 *  Authors:
 *      yiwen sun <yiwensun@hengtiansoft.com>
 *      
 */

#include "zs_test_engine.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <ctype.h>
#include <stdarg.h>
#include <pwd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <sysexits.h>
#include <stddef.h>
#include <getopt.h>
#include <math.h>

// #define DEBUG 0

#define COMMAND_TOKEN 0
#define MAX_TOKENS 50
#define CHAR_BLK_SIZE 200*1024*1024     // 200 M
#define MAX_KEY_LEN 2000
#define MAX_DATA_LEN 30*1024*1024        // 8 M

/* Use this for string generation */
#define CHAR_COUNT 64       /* number of characters used to generate character table */
const char ALPHANUMBERICS[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-";

char range_key_start[MAX_KEY_LEN + 1] = {0};
char range_key_end[MAX_KEY_LEN + 1] = {0};

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occured (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

typedef struct token_s {
    char *value;
    size_t length;
} token_t;

/** exported globals **/
struct stats stats;
struct settings settings;
time_t process_started;     /* when the process was started */

#ifdef USE_ZS_API
struct ZS_state *zs_state;
#elif defined USE_SDF_API
struct SDF_state *sdf_state;
#endif
#ifdef USE_ZS_API
__thread struct ZS_thread_state *zs_thd_state;
#elif defined USE_SDF_API
__thread struct SDF_thread_state *sdf_thd_state;
#endif

/** file scope variables **/
static conn *listen_conn = NULL;
static struct event_base *main_base;

char *char_block = NULL;
int dynamic_durability_test = 1;
#define MAX_CTNRS_FOR_DURABILITY_TEST 70000
int durability_records_array[MAX_CTNRS_FOR_DURABILITY_TEST];

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

/*
 * forward declarations
  */
static void drive_machine(conn *c);
static int new_socket(struct addrinfo *ai);

static void stats_init(void);
static void conn_init(void);

static void conn_free(conn *c);
static void conn_close(conn *c);

static void conn_shrink(conn *c);

/* event handling, network IO */
static void event_handler(const int fd, const short which, void *arg);

static const char *state_text(enum conn_states state);
static void conn_set_state(conn *c, enum conn_states state);
static void out_string(conn *c, const char *str);
static const char *prot_text(enum protocol prot);
static void process_command(conn *c, char *command);
static int add_msghdr(conn *c);
static int add_iov(conn *c, const void *buf, int len);

static void durability_sig_handler(int signum) {
   
    FILE *fd = fopen("./dynamic_durability.log", "wr+");

    for (int i = 0; i < MAX_CTNRS_FOR_DURABILITY_TEST; i++) {
        if (durability_records_array[i] != 0) {
            fprintf(fd, "%d = %d\n", i, durability_records_array[i]);
        }    
    }

    fflush(fd);
    fclose(fd);

    exit(1);
}

/**
 * used to initilize the global character block. The character
 * block is used to generate the suffix of the key and value. we
 * only store a pointer in the character block for each key
 * suffix or value string. It can save much memory to store key
 * or value string.
 */
static void init_random_block()
{
    char *ptr = NULL;

    char_block = (char *)malloc(CHAR_BLK_SIZE + MAX_DATA_LEN + 1);

    if (char_block == NULL) {
        fprintf(stderr, "Can't allocate global char block.");
        exit(1);
    }

    ptr = char_block;

    for (int i = 0; i < CHAR_BLK_SIZE + MAX_DATA_LEN + 1; i ++) {
        *(ptr++) = ALPHANUMBERICS[random() % CHAR_COUNT];
    }
}

static void stats_init(void) { 
    return;    
}

static void settings_init(void) {
    settings.use_cas = true;
    settings.access = 0700; 
    settings.port = 24422; 
    settings.udpport = 24422;
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
    settings.maxconns = 1024;         /* to limit connections-related memory to about 5MB */
    settings.verbose = 0;
//    settings.oldest_live = 0;
    settings.evict_to_free = 1;       /* push old items out of cache when memory runs out */
    settings.socketpath = NULL;       /* by default, not using a unix socket */
    settings.factor = 1.25;
    settings.chunk_size = 48;         /* space for a modest key and value */
    settings.num_threads = 4;         /* N workers */
    settings.prefix_delimiter = ':';       
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024; /* The famous 1MB upper limit. */
    settings.zs_reformat = -1;
    settings.zs_admin_port = 51350;
    settings.zs_stats_file = "/tmp/zsstats.log";
    settings.zs_log_file = "/tmp/zs.log";
    settings.zs_log_flush_dir = "/tmp";
    settings.zs_crash_dir = "/tmp";
    settings.shmem_basedir = "/tmp";
    settings.zs_flash_filename = "/schooner/data/schooner0";
    settings.zs_flog_mode = "ZS_FLOG_FILE_MODE";
    settings.zs_flog_nvram_file = "/tmp/nvram_file";
    settings.zs_flog_nvram_file_offset = 0;


    if (1 == dynamic_durability_test) {
        memset(durability_records_array, 0, sizeof(durability_records_array));
        signal(SIGTERM, durability_sig_handler); 
    }
}

void read_options(int argc, char* argv[])
{
    static struct option opts[] = {
        {"port",               required_argument, 0, 'p'},
        {"zs_reformat",        required_argument, 0, 'r'},
        {"socketpath",         required_argument, 0, 's'},
        {"threads",            required_argument, 0, 't'},
        {"zs_admin_port",      required_argument, 0, 'n'},
        {"zs_stats_file",      required_argument, 0, 'l'},
        {"zs_log_file",        required_argument, 0, 'z'},
        {"zs_log_flush_dir",   required_argument, 0, 'f'},
        {"zs_crash_dir",       required_argument, 0, 'c'},
        {"shmem_basedir",      required_argument, 0, 'b'},
        {"zs_flash_filename",  required_argument, 0, 'm'},
        {"zs_flog_mode",       required_argument, 0, 'd'},
        {"zs_flog_nvram_file", required_argument, 0, 'i'},
        {"zs_flog_nvram_file_offset", required_argument, 0, 'o'},
        {0, 0, 0, 0}
    };

    int optind;
    int c;

    while((c = getopt_long (argc, argv, "p:r:s:t:n:l:z:f:c:b:m:", opts, &optind)) != -1)
    {
        if (c == -1)
            break;

        switch(!c ? optind : c)
        {
            case 'p':
            case 0:
                settings.port = atoi(optarg);
                break;
            case 'r':
            case 1:
                settings.zs_reformat = atoi(optarg);
                break;
            case 's':
            case 2:
                settings.socketpath = optarg;
                break;
            case 't':
            case 3:
                settings.num_threads = atoi(optarg);
                break;
            case 'n':
            case 4:
                settings.zs_admin_port = atoi(optarg);
                break;
            case 'l':
            case 5:
                settings.zs_stats_file = optarg;
                break;
            case 'z':
            case 6:
                settings.zs_log_file = optarg;
                break;
            case 'f':
            case 7:
                settings.zs_log_flush_dir = optarg;
                break;
            case 'c':
            case 8:
                settings.zs_crash_dir = optarg;
                break;
            case 'b':
            case 9:
                settings.shmem_basedir = optarg;
                break;
            case 'm':
            case 10:
                settings.zs_flash_filename = optarg;
                break;
            case 'd':
            case 11:
                settings.zs_flog_mode = optarg;
                break;
            case 'i':
            case 12:
                settings.zs_flog_nvram_file = optarg;
                break;
            case 'o':
            case 13:
                settings.zs_flog_nvram_file_offset = atoi(optarg);
                break;
        }
    }
}


/*
* Ensures that there is room for another struct iovec in a connection's
* iov list.
*
* Returns 0 on success, -1 on out-of-memory.
*/
static int ensure_iov_space(conn *c) {
    assert(c != NULL);

    if (c->iovused >= c->iovsize) {
        int i, iovnum;
        struct iovec *new_iov = (struct iovec *)realloc(c->iov,
                (c->iovsize * 2) * sizeof(struct iovec));
        if (! new_iov)
            return -1;
        c->iov = new_iov;
        c->iovsize *= 2;

        /* Point all the msghdr structures at the new list. */
        for (i = 0, iovnum = 0; i < c->msgused; i++) {
            c->msglist[i].msg_iov = &c->iov[iovnum];
            iovnum += c->msglist[i].msg_iovlen;
        }
    }

    return 0;
}

/*
* Adds data to the list of pending data that will be written out to a
* connection.
*
* Returns 0 on success, -1 on out-of-memory.
*/
static int add_iov(conn *c, const void *buf, int len) {
    struct msghdr *m;
    int leftover;
    bool limit_to_mtu;

    assert(c != NULL);

    do {
        m = &c->msglist[c->msgused - 1];

        /*
        * Limit UDP packets, and the first payloads of TCP replies, to
        * UDP_MAX_PAYLOAD_SIZE bytes.
        */
        limit_to_mtu = IS_UDP(c->transport) || (1 == c->msgused);

        /* We may need to start a new msghdr if this one is full. */
        if (m->msg_iovlen == IOV_MAX ||
                (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
            add_msghdr(c);
            m = &c->msglist[c->msgused - 1];
        }

        if (ensure_iov_space(c) != 0)
            return -1;

        /* If the fragment is too big to fit in the datagram, split it up */
        if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE) {
            leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
            len -= leftover;
        } else {
            leftover = 0;
        }

        m = &c->msglist[c->msgused - 1];
        m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;

        c->msgbytes += len;
        c->iovused++;
        m->msg_iovlen++;
        
        buf = ((char *)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return 0;
}


/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int add_msghdr(conn *c)
{
    struct msghdr *msg;

    assert(c != NULL);

    if (c->msgsize == c->msgused) {
        msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
        if (! msg)
            return -1;
        c->msglist = msg;
        c->msgsize *= 2;
    }

    msg = c->msglist + c->msgused;

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(msg, 0, sizeof(struct msghdr));

    msg->msg_iov = &c->iov[c->iovused];

    if (c->request_addr_size > 0) {
        msg->msg_name = &c->request_addr;
        msg->msg_namelen = c->request_addr_size;
    }

    c->msgbytes = 0;
    c->msgused++;

    if (IS_UDP(c->transport)) {
        /* Leave room for the UDP header, which we'll fill in later. */
        return add_iov(c, NULL, UDP_HEADER_SIZE);
    }

    return 0;
}

/*
 * Free list management for connections.
  */
static conn **freeconns;
static int freetotal;
static int freecurr;
/* Lock for connection freelist */
static pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;


static void conn_init(void) {
    freetotal = 200; 
    freecurr = 0; 
    if ((freeconns = calloc(freetotal, sizeof(conn *))) == NULL) {
        fprintf(stderr, "Failed to allocate connection structures\n");
    }    
    return;
}

/*
* Returns a connection from the freelist, if any.
*/
conn *conn_from_freelist() {
    conn *c;

    pthread_mutex_lock(&conn_lock);
    if (freecurr > 0) {
        c = freeconns[--freecurr];
    } else {
        c = NULL;
    }
    pthread_mutex_unlock(&conn_lock);

    return c;
}

/*
* Adds a connection to the freelist. 0 = success.
*/
bool conn_add_to_freelist(conn *c) {
    bool ret = true;
    pthread_mutex_lock(&conn_lock);
    if (freecurr < freetotal) {
        freeconns[freecurr++] = c;
        ret = false;
    } else {
        /* try to enlarge free connections array */
        size_t newsize = freetotal * 2;
        conn **new_freeconns = realloc(freeconns, sizeof(conn *) * newsize);
        if (new_freeconns) {
            freetotal = newsize;
            freeconns = new_freeconns;
            freeconns[freecurr++] = c;
            ret = false;
        }
    }
    pthread_mutex_unlock(&conn_lock);
    return ret;
}

static bool update_event(conn *c, const int new_flags) {
    assert(c != NULL);

    struct event_base *base = c->event.ev_base;
    if (c->ev_flags == new_flags)
        return true;
    if (event_del(&c->event) == -1) return false;
    event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = new_flags;
    if (event_add(&c->event, 0) == -1) return false;
    return true;
}

/*
* Sets whether we are listening for new connections or not.
*/
void do_accept_new_conns(const bool do_accept) {
    conn *next;

    for (next = listen_conn; next; next = next->next) {
        if (do_accept) {
            update_event(next, EV_READ | EV_PERSIST);
            if (listen(next->sfd, settings.backlog) != 0) {
                perror("listen");
            }
        }
        else {
            update_event(next, 0);
            if (listen(next->sfd, 0) != 0) {
                perror("listen");
            }
        }
    }

    if (do_accept) {
        STATS_LOCK();
        stats.accepting_conns = true;
        STATS_UNLOCK();
    } else {
        STATS_LOCK();
        stats.accepting_conns = false;
        stats.listen_disabled_num++;
        STATS_UNLOCK();
    }
}

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_command(char *command, token_t *tokens, const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;

    assert(command != NULL && tokens != NULL && max_tokens > 1);

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
            }
            s = e + 1;
        }
        else if (*e == '\0') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }

            break; /* string end */
        }
    }

    /*
    * If we scanned the whole string, the terminal value pointer is null,
    * otherwise it is the first unprocessed character.
    */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

#ifdef USE_BTREE
static uint32_t get_rangequery_flag(char *flag_str) {
    
    uint32_t flag = 0;
    
    int str_len = strlen(flag_str);
    int i = 0;
    int j = i;
    int flag_len = 0;
    
    for (i = 0; i < str_len; i++) {
        for (j = i; flag_str[j] != '|' && j < str_len; j++);
        
        flag_len = j - i;

        if (0 == strncmp(flag_str+i, "ZS_RANGE_BUFFER_PROVIDED", flag_len)) {
            flag |= ZS_RANGE_BUFFER_PROVIDED;
            
            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_BUFFER_PROVIDED\n");
            #endif

            i = j;
        } 
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_ALLOC_IF_TOO_SMALL", flag_len)) { 
            flag |= ZS_RANGE_ALLOC_IF_TOO_SMALL;
 
            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_ALLOC_IF_TOO_SMALL\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_SEQNO_LE", flag_len)) { 
            flag |= ZS_RANGE_SEQNO_LE;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_SEQNO_LE\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_SEQNO_GT_LE", flag_len)) { 
            flag |= ZS_RANGE_SEQNO_GT_LE;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_SEQNO_GT_LE\n");
            #endif

            i = j;          
        }         
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_START_GT", flag_len)) { 
            flag |= ZS_RANGE_START_GT;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_START_GT\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_START_GE", flag_len)) { 
            flag |= ZS_RANGE_START_GE;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_START_GE\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_START_LT", flag_len)) { 
            flag |= ZS_RANGE_START_LT;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_START_LT\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_START_LE", flag_len)) { 
            flag |= ZS_RANGE_START_LE;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_START_LE\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_END_GT", flag_len)) { 
            flag |= ZS_RANGE_END_GT;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_END_GT\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_END_GE", flag_len)) { 
            flag |= ZS_RANGE_END_GE;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_END_GE\n");
            #endif

            i = j;          
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_END_LT", flag_len)) { 
            flag |= ZS_RANGE_END_LT;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_END_LT\n");
            #endif
 
            i = j; 
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_END_LE", flag_len)) { 
            flag |= ZS_RANGE_END_LE;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_END_LE\n");
            #endif
 
            i = j; 
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_KEYS_ONLY", flag_len)) { 
            flag |= ZS_RANGE_KEYS_ONLY;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_KEYS_ONLY\n");
            #endif
 
            i = j; 
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_PRIMARY_KEY", flag_len)) { 
            flag |= ZS_RANGE_PRIMARY_KEY;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_PRIMARY_KEY\n");
            #endif
 
            i = j; 
        }
        else if (0 == strncmp(flag_str+i, "ZS_RANGE_INDEX_USES_DATA", flag_len)) { 
            flag |= ZS_RANGE_INDEX_USES_DATA;

            #ifdef DEBUG
            fprintf(stderr, "get_rangequery_flag: ZS_RANGE_INDEX_USES_DATA\n");
            #endif
 
            i = j; 
        }
    }

#ifdef DEBUG
    fprintf(stderr, "get_rangequery_flag return flags = 0x%x\n", flag);
#endif

    return flag;

}

static inline void process_ZSGetRange_command(conn *c, token_t *tokens, size_t ntokens) {  
   
    ZS_cguid_t  cguid = 0;

    uint32_t keybuf_size  = 0;
    uint64_t databuf_size = 0;
    uint32_t keylen_start = 0;
    uint32_t keylen_end   = 0;
    uint64_t start_seq    = 0;
    uint64_t end_seq      = 0;
    int      start_key    = 0;
    int      end_key      = 0;
    uint32_t flags        = 0;
    char    *flag_str     = NULL;

    ZS_status_t ret;

    int start_key_is_set = 0;
    int end_key_is_set = 0;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "keybuf_size=", strlen("keybuf_size="))) {
            keybuf_size = atoi(tokens[i].value + strlen("keybuf_size="));
        } else if (0 == strncmp(tokens[i].value, "databuf_size=", strlen("databuf_size="))) {
            databuf_size = atoi(tokens[i].value + strlen("databuf_size="));
        } else if (0 == strncmp(tokens[i].value, "keylen_start=", strlen("keylen_start=")) ) {
            keylen_start = atoi(tokens[i].value + strlen("keylen_start="));
        } else if (0 == strncmp(tokens[i].value, "keylen_end=", strlen("keylen_end=")) ) {
            keylen_end = atoi(tokens[i].value + strlen("keylen_end="));
        } else if (0 == strncmp(tokens[i].value, "start_seq=", strlen("start_seq=")) ) {
            start_seq = atoi(tokens[i].value + strlen("start_seq="));
        } else if (0 == strncmp(tokens[i].value, "end_seq=", strlen("end_seq=")) ) {
            end_seq = atoi(tokens[i].value + strlen("end_seq="));
        } else if (0 == strncmp(tokens[i].value, "start_key=", strlen("start_key=")) ) {
            start_key = atoi(tokens[i].value + strlen("start_key="));
            start_key_is_set = 1;
        } else if (0 == strncmp(tokens[i].value, "end_key=", strlen("end_key="))) {
            end_key = atoi(tokens[i].value + strlen("end_key="));
            end_key_is_set = 1;
        } else if (0 == strncmp(tokens[i].value, "flags=", strlen("flags="))) {
            flag_str = tokens[i].value + strlen("flags=");
            flags = get_rangequery_flag(flag_str); 
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }
    
    /* Initialize rmeta */
    memset(&c->rmeta, 0, sizeof(c->rmeta));
    c->rmeta.key_start = NULL;
    c->rmeta.key_end   = NULL;
    c->rmeta.class_cmp_fn = NULL;
    c->rmeta.allowed_fn = NULL;
    c->rmeta.cb_data = NULL;

    if (keylen_start && start_key_is_set) {
        c->rmeta.key_start = range_key_start;
        c->rmeta.keylen_start = keylen_start;
        sprintf(c->rmeta.key_start, "%0*d", c->rmeta.keylen_start, start_key);
    }
    if (keylen_end && end_key_is_set) {
        c->rmeta.key_end = range_key_end;
        c->rmeta.keylen_end = keylen_end;
        sprintf(c->rmeta.key_end, "%0*d", c->rmeta.keylen_end, end_key);
    }

    if ( c->rmeta.key_start == NULL && c->rmeta.key_end == NULL)
        c->rmeta.flags = 0;

    if (flags & ZS_RANGE_BUFFER_PROVIDED) {
        if (keybuf_size == 0 || databuf_size == 0) {
            out_string(c, "CLIENT_ERROR: keybuf_size/databuf_size is not specified");
            return;
        }else {
            c->rmeta.keybuf_size = keybuf_size;
            c->rmeta.databuf_size = databuf_size;
        }
    }

    if (flags & ZS_RANGE_SEQNO_LE) {
        if (end_seq == 0) {
            out_string(c, "CLIENT_ERROR: end_seq is not specified");
            return;
        } else {
            c->rmeta.end_seq = end_seq;
        }
    }

    if (flags & ZS_RANGE_SEQNO_GT_LE) {
        if (start_seq == 0 || end_seq == 0) {
            out_string(c, "CLIENT_ERROR start_seq/end_seq is not specified");
            return;
        }else {
            c->rmeta.start_seq = start_seq;
            c->rmeta.end_seq = end_seq;
        }
    }

    c->rmeta.flags |= flags;


    ret = ZSGetRange (
              c->zs_thd_state,
              cguid,
              ZS_RANGE_PRIMARY_INDEX,
              &c->cursor,        
              &c->rmeta       
          );

#ifdef DEBUG
    fprintf(stderr, "ZSGetRange rmeta.keybuf_size = %d\n", c->rmeta.keybuf_size);
    fprintf(stderr, "ZSGetRange rmeta.databuf_size = %d\n", c->rmeta.databuf_size);
    fprintf(stderr, "ZSGetRange rmeta.keylen_start = %d\n", c->rmeta.keylen_start);
    fprintf(stderr, "ZSGetRange rmeta.keylen_end = %d\n", c->rmeta.keylen_end);
    fprintf(stderr, "ZSGetRange rmeta.key_start = %s\n", c->rmeta.key_start);
    fprintf(stderr, "ZSGetRange rmeta.key_end   = %s\n", c->rmeta.key_end);
    fprintf(stderr, "ZSGetRange rmeta.flags = 0x%x\n", c->rmeta.flags);
#endif
   
    char output_str[100];
    if (ret != ZS_SUCCESS)
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
    else
        sprintf(output_str, "OK");

    out_string(c, output_str);       

    return;   
}

static inline void process_ZSGetNextRange_command(conn *c, token_t *tokens, size_t ntokens) {  
   
    int n_in;
    int n_out = 0;
    int check = 0;
    int key;
    int check_fail = 0;
    int check_warning = 0;
    int check_warning_fail = 0;

    ZS_range_data_t *values = NULL;
    
    ZS_status_t ret;

    int n_in_unit = 100;
    int n_in_quotient = 0;
    int n_in_remainder = 0;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {

        if (0 == strncmp(tokens[i].value, "n_in=", strlen("n_in="))) {
            n_in = atoi(tokens[i].value + strlen("n_in="));
        } else if (0 == strncmp(tokens[i].value, "check=", strlen("check="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("check="), "yes" ) ) {
                check = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("check="), "no" ) ) {
                check = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for check" );
                return;
            }
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }
        
    values = (ZS_range_data_t *)malloc(sizeof(ZS_range_data_t) * n_in_unit); 
    assert(values);
    memset(values, 0, sizeof(ZS_range_data_t) * n_in_unit);        
	if(c->rmeta.flags & ZS_RANGE_BUFFER_PROVIDED){
		for(int i=0; i< n_in_unit; i++){
			values[i].key = (char *)malloc(c->rmeta.keybuf_size);
            assert(values[i].key);
			values[i].data = (char *)malloc(c->rmeta.databuf_size);
            assert(values[i].data);
		}
	}

    n_in_quotient = n_in / n_in_unit;
    n_in_remainder = n_in % n_in_unit;
   
    char output_str[100]; 

    int m;
    int n_in_tmp = 0;

    for (m=0; m<=n_in_quotient; m++) {

        n_in_tmp = m < n_in_quotient ? n_in_unit : n_in_remainder;

        /* There are some cases that assign
         * n_in == n_in_quotient == n_in_remainder == 0 ,
         * meanwhile m == n_in_tmp == 0 ,
         * therefore if n_in_tmp == 0, it must be m == 0 that go on looping
        */
        if (n_in_tmp != 0 || m == 0) {


            ret = ZSGetNextRange (
                      c->zs_thd_state,
                      c->cursor,
                      n_in_tmp,
                      &n_out,        
                      values
                  );

            //assert(n_in_tmp >= n_out);

        #ifdef DEBUG
            fprintf(stderr, "n_in_tmp = %d, n_out = %d, n_out_sum = %d, ret = %s\n",
                n_in_tmp, n_out, m*n_in_unit+n_out, ZSStrError(ret));
        #endif

            if (ret == ZS_SUCCESS || ret == ZS_QUERY_DONE) {
                if (n_out == 0) {
                    //ret == ZS_QUERY_DONE
                    
                    /* There are some cases that assign
                     * n_in == n_in_quotient == n_in_remainder == 0 ,
                     * meanwhile m == n_in_tmp == 0 ,
                     * therefore if m*n_in_unit+n_out == 0 , it is first time loop and gets ZS_QUERY_DONE status.
                    */
                    if (m*n_in_unit+n_out == 0)
                        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));

                    break;
                }

                if (check) {
                    for (int i = 0; i < n_out; i++) {
        #ifdef DEBUG
                        fprintf(stderr, "ZSGetNextRange key:%.8s, keylen:%d, datalen:%d, values:%s\n", 
                            values[i].key, values[i].keylen, values[i].datalen, values[i].data);
        #endif          
                        
                        char fmt[100];
                        sprintf(fmt, "%%0%dd", values[i].keylen);
                        sscanf(values[i].key, fmt, &key);

                        if (0 != strncmp(values[i].data, char_block+key, values[i].datalen)) {
                            check_fail++;
                        }
                            
                    }
                }

                if (check_fail) {
                    sprintf(output_str, "SERVER_ERROR %d items check failed", check_fail); 
                } else {
                    sprintf(output_str, "OK n_out=%d", m*n_in_unit + n_out);
                }
            }else if (ret == ZS_WARNING) {
                sprintf(output_str, "SERVER_ERROR: %s -> n_out=%d", ZSStrError(ret), m*n_in_unit + n_out); 
                for (int i = 0; i < n_out; i++) {
                    char *tmp;
                    tmp = (char *) malloc(values[i].keylen+1);
                    snprintf(tmp,values[i].keylen+1,"%s",values[i].key);
        #ifdef DEBUG
                    fprintf(stderr, "ZSGetNextRange key:%.8s, keylen:%d, datalen:%d, values:%s\n",
                         values[i].key, values[i].keylen, values[i].datalen, values[i].data);
        #endif
                    if (c->rmeta.flags & ZS_RANGE_ALLOC_IF_TOO_SMALL)  {
                        check_warning = 1;
                    } else {
                        if ((values[i].status & ZS_KEY_BUFFER_TOO_SMALL) && (values[i].status & ZS_KEY_BUFFER_TOO_SMALL)) {
                            sprintf(output_str, "SERVER_ERROR: %s -> ZS_KEY_BUFFER_TOO_SMALL & ZS_DATA_BUFFER_TOO_SMALL",  ZSStrError(ret));
                        } else if (values[i].status & ZS_DATA_BUFFER_TOO_SMALL) {
                            sprintf(output_str, "SERVER_ERROR: %s -> ZS_DATA_BUFFER_TOO_SMALL",  ZSStrError(ret));
                        } else if (values[i].status & ZS_KEY_BUFFER_TOO_SMALL) {
                            sprintf(output_str, "SERVER_ERROR: %s -> ZS_KEY_BUFFER_TOO_SMALL",  ZSStrError(ret));
                        }
                    }
                    //fprintf(stderr, "ZSGetNextRange warning: %d\n",values[i].status);
                }

                if (check_warning) {
                     for (int i = 0; i < n_out; i++) {
        #ifdef DEBUG
                        fprintf(stderr, "ZSGetNextRange key:%.8s, keylen:%d, datalen:%d, values:%s\n", 
                            values[i].key, values[i].keylen, values[i].datalen, values[i].data);
        #endif          
                        
                        char fmt[100];
                        sprintf(fmt, "%%0%dd", values[i].keylen);
                        sscanf(values[i].key, fmt, &key);

                        if (0 != strncmp(values[i].data, char_block+key, values[i].datalen)) {
                            check_warning_fail++;
                        }
                    }
                }

                if (check_warning_fail) {
                    sprintf(output_str, "SERVER_ERROR: %s -> %d items check failed \
                        when ZS_RANGE_ALLOC_IF_TOO_SMALL flag is specified", ZSStrError(ret), check_warning_fail); 
                } else if (check_warning) {
                    sprintf(output_str, "OK n_out=%d", m*n_in_unit + n_out);
                }

            }else if (ret != ZS_SUCCESS) { 
                sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
                out_string(c, output_str);       
                for (int i = 0; i < n_out; i++) {
        #ifdef DEBUG
                    fprintf(stderr, "ZSGetNextRange key:%.8s, keylen:%d, datalen:%d, values:%s\n", 
                         values[i].key, values[i].keylen, values[i].datalen, values[i].data);
        #endif  
                    /*
                    if(values[i].status & ZS_DATA_BUFFER_TOO_SMALL){
                        ZSFreeBuffer(values[i].data);
                        values[i].data=NULL;
                    }
                    if(values[i].status & ZS_KEY_BUFFER_TOO_SMALL){
                        ZSFreeBuffer(values[i].key);
                        values[i].key=NULL;
                    }
                    */
                    fprintf(stderr, "ZSGetNextRange warning: %d",values[i].status);
                }        

                //return;
            }

            if((c->rmeta.flags & ZS_RANGE_BUFFER_PROVIDED) == 0){
                for (int i = 0; i < n_out; i++) {
                    if (values[i].key != NULL) ZSFreeBuffer(values[i].key);
                    if (values[i].data != NULL) ZSFreeBuffer(values[i].data);
                }
            }

        }//endof if (n_in_tmp != 0)
    }//endof for (m=0; m<=n_in_quotient; m++)

    out_string(c, output_str);  

    if(c->rmeta.flags & ZS_RANGE_BUFFER_PROVIDED){
        for (int i = 0; i < n_in_unit; i++) {
            if (values[i].key != NULL) free(values[i].key);
            if (values[i].data != NULL) free(values[i].data);
        }
    }

    free(values);
  
    return;   
}

static inline void process_ZSGetRangeFinish_command(conn *c, token_t *tokens, size_t ntokens) {  
   
    ZS_status_t ret;
        
    ret = ZSGetRangeFinish (
              c->zs_thd_state,
              c->cursor
          );
    
    char output_str[100];
    if (ret != ZS_SUCCESS) { 
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);       
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);           
    }
  
    return;   
}

#endif

#ifdef USE_ZS_API
static inline void process_ZSOpenContainer_command(conn *c, token_t *tokens, size_t ntokens) {  

    ZS_container_props_t props;
    char *cname = NULL;

    int size = 0;
    int fifo_mode = 0;
    int persistent = 0;
    int evicting = 0;
    int writethru = 0;
    //int async_writes = 0;
    ZS_durability_level_t durability_level = 0;
    ZS_cguid_t  cguid;
    int num_shards = 1;
    int flags = 0;
    uint64_t container_type = 0;

    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cname=", strlen("cname="))) {
             cname = tokens[i].value + strlen("cname=");
        } else if (0 == strncmp(tokens[i].value, "fifo_mode=", strlen("fifo_mode="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "yes" ) ) {
                fifo_mode = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "no" ) ) {
                fifo_mode = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "fifo_mode" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "persistent=", strlen("persistent=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "yes" ) ) {
                persistent = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "no" ) ) {
                persistent = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "persistent" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "evicting=", strlen("evicting=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("evicting="), "yes" ) ) {
                evicting = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("evicting="), "no" ) ) {
                evicting = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "evicting" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "async_writes=", strlen("async_writes=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("async_writes="), "yes" ) ) {
                // async_writes = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("async_writes="), "no" ) ) {
                // async_writes = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "async_writes" );
                return;
            } 
        } else if (0 == strncmp(tokens[i].value, "writethru=", strlen("writethru=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "yes" ) ) {
                writethru = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "no" ) ) {
                writethru = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "writethru" );
                return;
            } 

        } else if (0 == strncmp(tokens[i].value, "size=", strlen("size="))) {
            size = atoi( tokens[i].value + strlen("size=") );
        } else if (0 == strncmp(tokens[i].value, "durability_level=", strlen("durability_level="))) {
            
            if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "ZS_DURABILITY_PERIODIC" ) ) {
                durability_level = ZS_DURABILITY_PERIODIC;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "ZS_DURABILITY_SW_CRASH_SAFE" ) ) {
                durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "ZS_DURABILITY_HW_CRASH_SAFE" ) ) {
                durability_level = ZS_DURABILITY_HW_CRASH_SAFE;
            } 
            else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for "
                        "durability_level" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "num_shards=", strlen("num_shards="))) {
            num_shards = atoi( tokens[i].value + strlen("num_shards=") );
        } else if (0 == strncmp(tokens[i].value, "flags=", strlen("flags="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_CTNR_RW_MODE" ) ) {
                flags |= ZS_CTNR_RW_MODE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_CTNR_CREATE" ) ) {
                flags |= ZS_CTNR_CREATE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_CTNR_RO_MODE" ) ) {
                flags |= ZS_CTNR_RO_MODE;
            } else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for flags" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "type=", strlen("type="))) {
            if ( 0 == strcmp( tokens[i].value + strlen("type="), "BTREE" ) ) {
                container_type = 0;
            } else if ( 0 == strcmp( tokens[i].value + strlen("type="), "HASH" ) ) {
                container_type = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("type="), "LOGGING" ) ) {
                container_type = 2;
            } else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for type" );
                return;
            }
        } 
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if ( NULL == cname ) {
        out_string( c, "CLIENT_ERROR please specify a container name" );
        return;
    }

    props.size_kb = size;
    props.fifo_mode = fifo_mode ? ZS_TRUE : ZS_FALSE; 
    props.persistent = persistent ? ZS_TRUE : ZS_FALSE;
    props.evicting = evicting ? ZS_TRUE : ZS_FALSE;
    props.writethru = writethru ? ZS_TRUE : ZS_FALSE;

    // FIXME, disable async write in ZS1.2 QA
    props.async_writes = ZS_FALSE; // async_writes ? ZS_TRUE : ZS_FALSE;
    props.durability_level = durability_level ? durability_level : ZS_DURABILITY_PERIODIC;     
    props.num_shards = num_shards ? num_shards : 1;     
    props.flags         = container_type;
    if (props.flags == 0)     { props.flash_only = 1;}

    ret = ZSOpenContainer (
            c->zs_thd_state,
            cname,
            &props,
            flags,
            &cguid
          );
    
     char output_str[100];
     if (ret != ZS_SUCCESS) { 
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);       
     } else {
        sprintf(output_str, "OK cguid=%lu", cguid);
        out_string(c, output_str);           
     }
  
     return;   
}

static inline void process_ZSCloseContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = ZSCloseContainer(
            c->zs_thd_state,
            cguid
          );
 
    char output_str[100];
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSDeleteContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = ZSDeleteContainer(
            c->zs_thd_state,
            cguid
          );
 
    char output_str[100];
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSGetContainers_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguids[MCD_MAX_NUM_CNTRS];
    uint32_t n_cguids = 0;
    ZS_status_t ret;

    ret = ZSGetContainers(
            c->zs_thd_state,
            cguids,
            &n_cguids
          );
 
    char output_str[2048];
    int offset = 0;
    int output_number = 0;
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK n_cguids=%u", n_cguids);
	/*
        offset += sprintf(output_str, "OK n_cguids=%u\n", n_cguids);
        output_number = n_cguids > MCD_MAX_NUM_CNTRS ? MCD_MAX_NUM_CNTRS : n_cguids;

        for (int i = 0; i < output_number; i++) {
            offset += sprintf(output_str+offset, "%lu ", cguids[i]);
        }*/
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSGetContainerProps_command(conn *c, token_t *tokens, size_t ntokens) {
    
    ZS_container_props_t props;
    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    ret = ZSGetContainerProps(
            c->zs_thd_state,
            cguid,
            &props
          );
 
    char output_str[200];
    int offset = 0;
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK\n");
       
        offset += sprintf(output_str+offset, "cguid=%ld\n", props.cguid);
        offset += sprintf(output_str+offset, "fifo_mode=%u\n", props.fifo_mode);
        offset += sprintf(output_str+offset, "persistent=%d\n", props.persistent);
        offset += sprintf(output_str+offset, "evicting=%d\n", props.evicting);
        offset += sprintf(output_str+offset, "writethru=%d\n", props.writethru);
        offset += sprintf(output_str+offset, "async_writes=%d\n", props.async_writes);
        offset += sprintf(output_str+offset, "size=%lu kb\n", props.size_kb);
        offset += sprintf(output_str+offset, "durability_level=%d\n", props.durability_level);
        offset += sprintf(output_str+offset, "num_shards=%d", props.num_shards);

        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSSetContainerProps_command(conn *c, token_t *tokens, size_t ntokens) {   
    ZS_container_props_t props;

    int size = 0;
    int fifo_mode = 0;
    int persistent = 0;
    int evicting = 0;
    int writethru = 0;
    //int async_writes = 0;
    ZS_durability_level_t durability_level = 0;
    ZS_cguid_t cguid = 0;
    int num_shards = 1;
    int flags = 0;

    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "fifo_mode=", strlen("fifo_mode="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "yes" ) ) {
                fifo_mode = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "no" ) ) {
                fifo_mode = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "fifo_mode" );
                return;
            } 
        } else if (0 == strncmp(tokens[i].value, "persistent=", strlen("persistent=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "yes" ) ) {
                persistent = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "no" ) ) {
                persistent = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "persistent" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "evicting=", strlen("evicting=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("evicting="), "yes" ) ) {
                evicting = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("evicting="), "no" ) ) {
                evicting = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "evicting" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "async_writes=", strlen("async_writes=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("async_writes="), "yes" ) ) {
                // async_writes = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("async_writes="), "no" ) ) {
                // async_writes = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "async_writes" );
                return;
            } 
        } else if (0 == strncmp(tokens[i].value, "writethru=", strlen("writethru=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "yes" ) ) {
                writethru = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "no" ) ) {
                writethru = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "writethru" );
                return;
            } 

        } else if (0 == strncmp(tokens[i].value, "size=", strlen("size="))) {
            size = atoi( tokens[i].value + strlen("size=") );
        } else if (0 == strncmp(tokens[i].value, "durability_level=", strlen("durability_level="))) {
            
            if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "ZS_DURABILITY_PERIODIC" ) ) {
                durability_level = ZS_DURABILITY_PERIODIC;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "ZS_DURABILITY_SW_CRASH_SAFE" ) ) {
                durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "ZS_DURABILITY_HW_CRASH_SAFE" ) ) {
                durability_level = ZS_DURABILITY_HW_CRASH_SAFE; 
            }
            else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for "
                        "durability_level" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "num_shards=", strlen("num_shards="))) {
            num_shards = atoi( tokens[i].value + strlen("num_shards=") );
        } else if (0 == strncmp(tokens[i].value, "flags=", strlen("flags="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_CTNR_RW_MODE" ) ) {
                flags |= ZS_CTNR_RW_MODE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_CTNR_CREATE" ) ) {
                flags |= ZS_CTNR_CREATE;
            } else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for flags" );
                return;
            }
        } 
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
 
    props.size_kb = size;
    props.fifo_mode = fifo_mode ? ZS_TRUE : ZS_FALSE; 
    props.persistent = persistent ? ZS_TRUE : ZS_FALSE;
    props.evicting = evicting ? ZS_TRUE : ZS_FALSE;
    props.writethru = writethru ? ZS_TRUE : ZS_FALSE;

    // FIXME, disable async write in ZS1.2 QA
    props.async_writes = ZS_FALSE; // async_writes ? ZS_TRUE : ZS_FALSE;
    props.durability_level = durability_level ? durability_level : ZS_DURABILITY_PERIODIC;     
    props.num_shards = num_shards ? num_shards : 1;     
    
    ret = ZSSetContainerProps (
            c->zs_thd_state,
            cguid,
            &props
          );
   
    char output_str[100];
    if (ret != ZS_SUCCESS) { 
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);       
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);           
    }
  
    return;   
}

static inline void process_ZSReadObject_command(conn *c, token_t *tokens, size_t ntokens) {
    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int data_offset = 0;
    int data_len = 0;

    int logging = 0;      // we add logging flag to support logging container.
    uint64_t counter = 0;
    char *data = NULL;    // we add logging flag to support logging container.
    char *pg = NULL;      // we add logging flag to support logging container.
    char *osd = NULL;     // we add logging flag to support logging container.
    uint64_t get_data_len = 0;
 
    int nops = 1;
    int check = 0;
    int keep_read = 0;

    char output_str[100];

    int key = -1;        // we add integer key to support range query.
    char key_str[MAX_KEY_LEN];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "data_offset=", strlen("data_offset="))) {  
            data_offset = atoi( tokens[i].value + strlen("data_offset=") );
        } else if (0 == strncmp(tokens[i].value, "data_len=", strlen("data_len="))) {   
            data_len = atoi( tokens[i].value + strlen("data_len=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        } else if (0 == strncmp(tokens[i].value, "check=", strlen("check="))) { 
            if ( 0 == strcmp( tokens[i].value + strlen("check="), "yes" ) ) {
                check = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("check="), "no" ) ) {
                check = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for check" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "keep_read=", strlen("keep_read="))) { 
            if ( 0 == strcmp( tokens[i].value + strlen("keep_read="), "yes" ) ) {
                keep_read = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("keep_read="), "no" ) ) {
                keep_read = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for keep_read" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "key=", strlen("key="))) {
            key = atoi( tokens[i].value + strlen("key=") );
        } else if (0 == strncmp(tokens[i].value, "logging=", strlen("logging="))) {
            if ( 0 == strcmp( tokens[i].value + strlen("logging="), "yes" ) ) {
                logging = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("logging="), "no" ) ) {
                logging = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for logging" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "counter=", strlen("counter="))) {
            counter = strtoul( tokens[i].value + strlen("counter="), NULL, 10 );
        } else if (0 == strncmp(tokens[i].value, "pg=", strlen("pg="))) {
	    //pg = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(pg, tokens[i].length - strlen("pg="),tokens[i].value + strlen("pg="));
            pg = tokens[i].value + strlen("pg=");
        } else if (0 == strncmp(tokens[i].value, "osd=", strlen("osd="))) {
	    //osd = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(osd, tokens[i].length - strlen("osd="), tokens[i].value + strlen("osd="));
            osd = tokens[i].value + strlen("osd=");
        }
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
 
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    int offset = 0;
    int read_fail = 0;
    int check_fail = 0;
    uint64_t tmp_counter = 0;
    for (int i = 0; i < abs(nops); i++) {
	if (logging == 0) {
	    if (key < 0) {
                ret = ZSReadObject(
                      c->zs_thd_state,
                      cguid,
                      char_block + key_offset + i,
                      key_len,
                      &data,
                      &get_data_len
                 );
            } else {
                if (nops < 0) {
                    sprintf(key_str, "%0*d", key_len, key-i);
                } else {
                    sprintf(key_str, "%0*d", key_len, key+i);
                }
                ret = ZSReadObject(
                      c->zs_thd_state,
                      cguid,
                      key_str,
                      key_len,
                      &data,
                      &get_data_len
                  );
            }
	} else {
	    if (nops < 0) { 
		tmp_counter = counter - i;
	    }else {
		tmp_counter = counter + i;
	    }

            memset(key_str, 0, sizeof(key_str));
	    if (osd == NULL){
	        sprintf(key_str, "%lu_%s", tmp_counter, pg);	
            }else {
		sprintf(key_str, "%lu_%s_%s", tmp_counter, pg, osd);	
	    }
                ret = ZSReadObject(
                      c->zs_thd_state,
                      cguid,
                      key_str,
                      strlen(key_str),
                      &data,
                      &get_data_len
                  );
	}

        if (ret != ZS_SUCCESS) {
            if (1 == keep_read) {
                read_fail++;
            } else {
                offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
                offset += sprintf(output_str+offset, "%d th ZSReadObject failed, key_offset=%d", i, key_offset+i);
                out_string(c, output_str);
                return; 
            }
        } else {

            if (check) {
                if ((get_data_len != data_len) || (0 != strncmp(data, char_block+data_offset+i, get_data_len)))  {
                    check_fail++;    
                }
            }

#ifdef DEBUG
            fprintf(stderr, "ZSReadObject key:%s ==> val:%.10s\n", key_str, char_block + data_offset + i);
#endif

            ZSFreeBuffer(data);
            data = NULL; 
        }
    }

    if ((0 == check_fail) && (0 == read_fail)) { 
        sprintf(output_str, "OK"); 
    } else {
        sprintf(output_str, "SERVER_ERROR %d items read failed %d items check failed", read_fail, check_fail);
    }

    out_string(c, output_str);

    return;
}

static inline void process_ZSWriteObject_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int data_offset = 0;
    int data_len = 0;
    int nops = 1;
    uint32_t flags = 0;
    char output_str[100];

    int logging = 0;      // we add logging flag to support logging container.
    char *pg = NULL;      // we add logging flag to support logging container.
    char *osd = NULL;     // we add logging flag to support logging container.
    uint64_t counter = 0;
    int key = -1;        // we add integer key to support range query
    char key_str[MAX_KEY_LEN];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "data_offset=", strlen("data_offset="))) {  
            data_offset = atoi( tokens[i].value + strlen("data_offset=") );
        } else if (0 == strncmp(tokens[i].value, "data_len=", strlen("data_len="))) {   
            data_len = atoi( tokens[i].value + strlen("data_len=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        } else if (0 == strncmp(tokens[i].value, "flags=", strlen("flags="))) {
            if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_WRITE_MUST_NOT_EXIST" ) ) {
                flags |= ZS_WRITE_MUST_NOT_EXIST;
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_WRITE_MUST_EXIST" ) ) {
                flags |= ZS_WRITE_MUST_EXIST;
#ifdef USE_3_1
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_WRITE_TRIM" ) ) {
                flags |= ZS_WRITE_TRIM;
#endif
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "0" ) ) {
                flags = 0; 
            } else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for flags" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "key=", strlen("key="))) { 
            key = atoi( tokens[i].value + strlen("key=") );
        } else if (0 == strncmp(tokens[i].value, "logging=", strlen("logging="))) {
            if ( 0 == strcmp( tokens[i].value + strlen("logging="), "yes" ) ) {
                logging = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("logging="), "no" ) ) {
                logging = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for logging" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "counter=", strlen("counter="))) {
            counter = strtoul( tokens[i].value + strlen("counter="), NULL, 10 );
        } else if (0 == strncmp(tokens[i].value, "pg=", strlen("pg="))) {
	    //pg = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(pg, tokens[i].length - strlen("pg="),tokens[i].value + strlen("pg="));
            pg = tokens[i].value + strlen("pg=");
        } else if (0 == strncmp(tokens[i].value, "osd=", strlen("osd="))) {
	    //osd = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(osd, tokens[i].length - strlen("osd="), tokens[i].value + strlen("osd="));
            osd = tokens[i].value + strlen("osd=");
        } 
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
#ifdef USE_3_1
    if ( (logging == 0) && (flags == ZS_WRITE_TRIM)) {
        out_string( c, "CLIENT_ERROR ZS_WRITE_TRIM only support LOGGING Container!");
        return;
    }
#endif
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    if (data_offset < 0 || data_offset > CHAR_BLK_SIZE || data_len < 0 || data_len > MAX_DATA_LEN) {
        out_string( c, "CLIENT_ERROR please do not let data cross the char block table");
        return;
    }

    int offset = 0;
    uint64_t tmp_counter = 0;
    for (int i = 0; i < abs(nops); i++) {
        if ( (key < 0) && (logging == 0)) {
            ret = ZSWriteObject(
                    c->zs_thd_state,
                    cguid,
                    char_block + key_offset + i,
                    key_len,
                    char_block + data_offset + i,
                    data_len,
                    flags
                  );
        } else {
	    if (logging == 0) {
            	if (nops < 0) { 
                	sprintf(key_str, "%0*d", key_len, key-i); 
	            } else {
        	        sprintf(key_str, "%0*d", key_len, key+i);
	            }
	    }else {
		if (nops < 0) { 
			tmp_counter = counter - i; 
		}
		else {
			tmp_counter =  counter + i; 
		}
	            memset(key_str, 0, sizeof(key_str));
		    if (osd == NULL){
	        	sprintf(key_str, "%lu_%s", tmp_counter, pg);	
	            }else {
			sprintf(key_str, "%lu_%s_%s", tmp_counter, pg, osd);	
		    }
	    }

            //fprintf(stderr, "ZSWriteObject key:%s ======= \n", key_str);

            ret = ZSWriteObject(
                    c->zs_thd_state,
                    cguid,
                    key_str,
                    strlen(key_str),
                    char_block + data_offset + i,
                    data_len,
                    flags
                  );
        }

        if (ret != ZS_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th ZSWriteObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 

        if (1 == dynamic_durability_test && ret == ZS_SUCCESS) {
            (void)__sync_fetch_and_add (&durability_records_array[cguid], 1);
        }

#ifdef DEBUG
        fprintf(stderr, "ZSWriteObject key:%s ==> val:%.10s\n", key_str, char_block + data_offset + i);
#endif

    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;
}

static inline void process_ZSDeleteObject_command(conn *c, token_t *tokens, size_t ntokens) {
    
    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int nops = 1;

    char output_str[100];

    int logging = 0;      // we add logging flag to support logging container.
    char *pg = NULL;      // we add logging flag to support logging container.
    char *osd = NULL;     // we add logging flag to support logging container.
    uint64_t counter = 0;
    int key = -1;        // we add integer key to support range query
    char key_str[MAX_KEY_LEN];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        } else if (0 == strncmp(tokens[i].value, "key=", strlen("key="))) {
            key = atoi( tokens[i].value + strlen("key=") );
        } else if (0 == strncmp(tokens[i].value, "logging=", strlen("logging="))) {
            if ( 0 == strcmp( tokens[i].value + strlen("logging="), "yes" ) ) {
                logging = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("logging="), "no" ) ) {
                logging = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for logging" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "counter=", strlen("counter="))) {
            counter = strtoul( tokens[i].value + strlen("counter="), NULL, 10 );
        } else if (0 == strncmp(tokens[i].value, "pg=", strlen("pg="))) {
	    //pg = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(pg, tokens[i].length - strlen("pg="),tokens[i].value + strlen("pg="));
            pg = tokens[i].value + strlen("pg=");
        } else if (0 == strncmp(tokens[i].value, "osd=", strlen("osd="))) {
	    //osd = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(osd, tokens[i].length - strlen("osd="), tokens[i].value + strlen("osd="));
            osd = tokens[i].value + strlen("osd=");
        } 
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    int offset = 0;
    uint64_t tmp_counter = 0;
    for (int i = 0; i < abs(nops); i++) {
        if ((key < 0) && (logging == 0)){
            ret = ZSDeleteObject(
                    c->zs_thd_state,
                    cguid,
                    char_block + key_offset + i,
                    key_len
                  );
        } else { 
	    if (logging == 0) {
            	if (nops < 0) { 
                	sprintf(key_str, "%0*d", key_len, key-i); 
	            } else {
        	        sprintf(key_str, "%0*d", key_len, key+i);
	            }
	    }else {
		if (nops < 0) { 
			tmp_counter = counter - i;
		}else {
			tmp_counter = counter + i;
		}

	            memset(key_str, 0, sizeof(key_str));
		    if (osd == NULL){
	        	sprintf(key_str, "%lu_%s", tmp_counter, pg);	
	            }else {
			sprintf(key_str, "%lu_%s_%s", tmp_counter, pg, osd);	
		    }
	    }
		fprintf(stderr, "ZSDeleteObject: key %s =====\n", key_str);
            
            ret = ZSDeleteObject(
                    c->zs_thd_state,
                    cguid,
                    key_str,
                    strlen(key_str)
                  );
        }            

        if (ret != ZS_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th ZSDeleteObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 
    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;
}

#ifdef USE_3_1
static inline void process_ZSEnumeratePGObjects_command(conn *c, token_t *tokens, size_t ntokens) {
    ZS_cguid_t cguid = 0;
    ZS_status_t ret;
    char *data = NULL;    // we add logging flag to support logging container.
    char *pg = NULL;      // we add logging flag to support logging container.
    char *osd = NULL;     // we add logging flag to support logging container.
    uint64_t counter = 0;     // we add logging flag to support logging container.
    char key_str[MAX_KEY_LEN];

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "counter=", strlen("counter="))) {
            counter = strtoul( tokens[i].value + strlen("counter="), NULL, 10 );
        } else if (0 == strncmp(tokens[i].value, "pg=", strlen("pg="))) {
	    //pg = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(pg, tokens[i].length - strlen("pg="),tokens[i].value + strlen("pg="));
            pg = tokens[i].value + strlen("pg=");
        } else if (0 == strncmp(tokens[i].value, "osd=", strlen("osd="))) {
	    //osd = (char *) malloc (sizeof(char) * tokens[i].length);
            //snprintf(osd, tokens[i].length - strlen("osd="), tokens[i].value + strlen("osd="));
            osd = tokens[i].value + strlen("osd=");
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    memset(key_str, 0, sizeof(key_str));
    if (osd == NULL){
       	sprintf(key_str, "%lu_%s", counter, pg);	
    }else {
	sprintf(key_str, "%lu_%s_%s", counter, pg, osd);	
    }

    ret = ZSEnumeratePGObjects(
              c->zs_thd_state,
              cguid,
              &c->iterator,
	      key_str,
	      strlen(key_str)	      
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}
#endif

static inline void process_ZSEnumerateContainerObjects_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    ret = ZSEnumerateContainerObjects(
              c->zs_thd_state,
              cguid,
              &c->iterator
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSNextEnumeratedObject_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_status_t ret;

    char output_str[100];

    char *get_key = NULL;
    uint32_t get_key_len = 0;
    char *get_data = NULL; 
    uint64_t get_data_len = 0;
    int success_count = 0;
    
    ret = ZSNextEnumeratedObject(
              c->zs_thd_state,
              c->iterator,
              &get_key,
              &get_key_len,
              &get_data,
              &get_data_len    
          );
    
    ZSFreeBuffer(get_key);
    ZSFreeBuffer(get_data);

    get_key = NULL;
    get_data = NULL;

    while (ret == ZS_SUCCESS) {

        success_count++;
        ret = ZSNextEnumeratedObject(
              c->zs_thd_state,
              c->iterator,
              &get_key,
              &get_key_len,
              &get_data,
              &get_data_len    
          ); 
    
        ZSFreeBuffer(get_key);
        ZSFreeBuffer(get_data);

        get_key = NULL;
        get_data = NULL;
    }
 
    if (0 == success_count) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK enumerate %d objects", success_count);
        out_string(c, output_str);
    }
   
}

static inline void process_ZSFinishEnumeration_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_status_t ret;

    char output_str[100];

    ret = ZSFinishEnumeration(
              c->zs_thd_state,
              c->iterator
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    c->iterator = NULL;

    return;
}

static inline void process_ZSFlushObject_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int nops = 1;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    int offset = 0;
    for (int i = 0; i < nops; i++) {

        ret = ZSFlushObject(
                c->zs_thd_state,
                cguid,
                char_block + key_offset + i,
                key_len
              );

        if (ret != ZS_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th ZSFlushObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 
    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;

}

static inline void process_ZSFlushContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = ZSFlushContainer(
            c->zs_thd_state,
            cguid
          );
 
    char output_str[100];
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSFlushCache_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_status_t ret;

    char output_str[100];

    ret = ZSFlushCache(
              c->zs_thd_state
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;

}

static inline void process_ZSGetStats_command(conn *c, token_t *tokens, size_t ntokens) {
    
    out_string(c, "SERVER_ERROR ZSGetStats is not implemented yet");

    return;
}

static inline void process_ZSGetContainerStats_command(conn *c, token_t *tokens, size_t ntokens) {
 
    out_string(c, "SERVER_ERROR ZSGetContainerStats is not implemented yet");

    return;
}

static inline void process_ZSTransactionStart_command(conn *c, token_t *tokens, size_t ntokens) {
 
    ZS_status_t ret;

    char output_str[100];

    ret = ZSTransactionStart(
              c->zs_thd_state
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSTransactionCommit_command(conn *c, token_t *tokens, size_t ntokens) {
 
    ZS_status_t ret;

    char output_str[100];

    ret = ZSTransactionCommit(
              c->zs_thd_state
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}


static inline void process_ZSTransactionRollback_command(conn *c, token_t *tokens, size_t ntokens) {
 
    ZS_status_t ret;

    char output_str[100];

    ret = ZSTransactionRollback(
              c->zs_thd_state
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSTransactionGetMode_command(conn *c, token_t *tokens, size_t ntokens) {
 
    ZS_status_t ret;
    int mode;

    char output_str[100];

    ret = ZSTransactionGetMode(
              c->zs_thd_state,
	      &mode
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK mode=%d", mode);
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSTransactionSetMode_command(conn *c, token_t *tokens, size_t ntokens) {
 
    ZS_status_t ret;
    int mode;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "mode=", strlen("mode="))) {
            mode = atoi( tokens[i].value + strlen("mode=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    ret = ZSTransactionSetMode(
              c->zs_thd_state,
	      mode
          );

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void grace_shutdown(){
	ZS_status_t ret;

	ret = ZSShutdown(zs_state);

	if (ret != ZS_SUCCESS){
		fprintf(stderr, "==Failed to shutdown ZS gracefully : %s ==\n",ZSStrError(ret));
	}else {
		fprintf(stderr, "==Shutdown ZS sucessfully!==\n");
	}
	return;
}
#ifdef USE_BTREE
static inline void process_ZSMPut_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;
    
    int num_objs = 0;
    uint32_t flags = 0;
    uint32_t objs_written;

    int key_offset = 0;
    int key_len = 0;
    int data_offset = 0;
    int data_len = 0;
    int logging = 0;      // we add logging flag to support logging container.
    char *pg = NULL;      // we add logging flag to support logging container.
    char *osd = NULL;     // we add logging flag to support logging container.
    uint64_t counter = 0;

    ZS_obj_t *objs;

    int mput_unit = 100;
    int mput_quotient = 0;
    int mput_remainder = 0;
    uint32_t objs_written_sum = 0;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "num_objs=", strlen("num_objs="))) {
            //if(atoi(tokens[i].value + strlen("num_objs=")) < 0) {
            //    out_string( c, "CLIENT_ERROR num_objs must not less than zero" );
            //    return;
            //} else {
                num_objs = atoi(tokens[i].value + strlen("num_objs="));
            //}
        } else if (0 == strncmp(tokens[i].value, "flags=", strlen("flags="))) {
            if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_WRITE_MUST_NOT_EXIST" ) ) {
                flags |= ZS_WRITE_MUST_NOT_EXIST;
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "ZS_WRITE_MUST_EXIST" ) ) {
                flags |= ZS_WRITE_MUST_EXIST;
            } else if ( 0 == strcmp( tokens[i].value + strlen("flags="), "0" ) ) {
                flags = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for flags" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "logging=", strlen("logging="))) {
            if ( 0 == strcmp( tokens[i].value + strlen("logging="), "yes" ) ) {
                logging = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("logging="), "no" ) ) {
                logging = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for logging" );
                return;
            }
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) {
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "data_offset=", strlen("data_offset="))) {
            data_offset = atoi( tokens[i].value + strlen("data_offset=") );
        } else if (0 == strncmp(tokens[i].value, "data_len=", strlen("data_len="))) {
            data_len = atoi( tokens[i].value + strlen("data_len=") );
        } else if (0 == strncmp(tokens[i].value, "counter=", strlen("counter="))) {
            counter = strtoul( tokens[i].value + strlen("counter="), NULL, 10 );
        } else if (0 == strncmp(tokens[i].value, "pg=", strlen("pg="))) {
            pg = tokens[i].value + strlen("pg=");
        } else if (0 == strncmp(tokens[i].value, "osd=", strlen("osd="))) {
            osd = tokens[i].value + strlen("osd=");
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    if (data_offset < 0 || data_offset > CHAR_BLK_SIZE || data_len < 0 || data_len > MAX_DATA_LEN) {
        out_string( c, "CLIENT_ERROR please do not let data cross the char block table");
        return;
    }

    objs = (ZS_obj_t *)malloc(sizeof(ZS_obj_t) * mput_unit);
    assert(objs);
    for (int i = 0; i<mput_unit; i++) {
      objs[i].key = (char *)malloc(key_len + 1);
      assert(objs[i].key);
    }
    
    mput_quotient = abs(num_objs) / mput_unit;
    mput_remainder = abs(num_objs) % mput_unit;

    char output_str[100];

    int m, n;
    int mput_tmp = 0;
    uint64_t tmp_counter = 0;

    for(m=0; m<=mput_quotient; m++) {

        mput_tmp = m < mput_quotient ? mput_unit: mput_remainder;  

        if (mput_tmp != 0) {

            for (n=0; n<mput_tmp; n++) {
                // FIXME, flags should always be '0' in zs code !?
                //fprintf(stdout, "keylen:%d ======\n", key_len);
                if (logging == 0) {
	                sprintf(objs[n].key, "%0*d", key_len, key_offset + m*mput_unit + n);
                	objs[n].key_len = key_len;
		}else {
			if ( num_objs < 0 ) {
				tmp_counter = counter - m*mput_unit - n;
			}else {
				tmp_counter = counter + m*mput_unit + n;
			}
                    if (osd == NULL){
                        sprintf(objs[n].key, "%lu_%s", tmp_counter, pg);
                    }else {
                        sprintf(objs[n].key, "%lu_%s_%s", tmp_counter, pg, osd);
                    }			
		    objs[n].key_len = strlen(objs[n].key);
		}
                objs[n].flags = 0; //flags;
                objs[n].data_len = data_len;
                objs[n].data = char_block + data_offset + m*mput_unit + n;
            }

            ret = ZSMPut(
                      c->zs_thd_state,
                      cguid,
                      mput_tmp,
                      objs,
                      flags,
                      &objs_written
                  );

        #ifdef DEBUG
            fprintf(stderr, "mput_tmp = %d, objs_written = %d, objs_written_sum = %d, ret = %s\n",
                mput_tmp, objs_written, m*mput_unit+objs_written, ZSStrError(ret));
        #endif

            if (ret != ZS_SUCCESS) {
                sprintf(output_str, "SERVER_ERROR %s: MPut objs %d ~ %d failed", ZSStrError(ret), m*mput_unit, (m+1)*mput_unit);
                out_string(c, output_str);

                for (int i = 0; i < mput_unit; i++) {
                    //fprintf(stdout, "MPUT KEY: %s \n", objs[i].key);
                    free(objs[i].key);
                }

                free(objs);

                return;

            } else {
                objs_written_sum += objs_written;
            }
        }//endof if(mput_tmp != 0)
    }//endof for(m=0; m<=mput_quotient; m++)
       
	for(int i = 0; i < mput_unit; i++){
		//fprintf(stdout, "MPUT KEY: %s \n", objs[i].key);
		free(objs[i].key);
	}
    free(objs);

    assert(ret == ZS_SUCCESS); 
    assert(objs_written_sum == abs(num_objs)); 
    sprintf(output_str, "OK objs_written=%d", objs_written_sum);
    out_string(c, output_str);

    return;
}
#endif
#ifdef USE_SNAPSHOT
static inline void process_ZSCreateContainerSnapshot_command(conn *c, token_t *tokens, size_t ntokens){

    ZS_cguid_t cguid = 0;
    ZS_status_t ret;
    uint64_t snap_seq=0;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    ret = ZSCreateContainerSnapshot(
            c->zs_thd_state,
            cguid,
            &snap_seq
          );

    char output_str[100];
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK snap_seq=%lu", snap_seq);
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSDeleteContainerSnapshot_command(conn *c, token_t *tokens, size_t ntokens){
    ZS_cguid_t cguid = 0;
    ZS_status_t ret;
    uint64_t snap_seq;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "snap_seq=", strlen("snap_seq="))) {
            snap_seq = strtoul( tokens[i].value + strlen("snap_seq="),0,10 );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    ret = ZSDeleteContainerSnapshot(
            c->zs_thd_state,
            cguid,
            snap_seq
          );

    char output_str[100];
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_ZSGetContainerSnapshots_command(conn *c, token_t *tokens, size_t ntokens){
    ZS_cguid_t cguid = 0;
    ZS_status_t ret;
    uint32_t n_snapshots;
    ZS_container_snapshots_t *snap_seqs;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    ret = ZSGetContainerSnapshots(
            c->zs_thd_state,
            cguid,
            &n_snapshots,
            &snap_seqs
          );

    char output_str[100];
    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK n_snapshots=%d", n_snapshots);
        out_string(c, output_str);
    }
    return;
}


static uint32_t crc_table[256];
static void init_crc_table(void)
{
    uint32_t c;
    uint32_t i, j;

    for (i = 0; i < 256; i++) {
        c = (uint32_t)i;
        for (j = 0; j < 8; j++) {
            if (c & 1)
                c =  0xedb88320L^ (c >> 1);
            else
                c = c >> 1;
        }
        crc_table[i] = c;
    }
}

/*计算buffer的crc校验码*/
static uint32_t crcCheckSum(const char *buffer, int size)
{
    uint32_t crc = 0xffffffff;
    uint32_t i;
    for (i = 0; i < size; i++) {
        crc = crc_table[(crc ^ *buffer++) & 0xff] ^ (crc >> 8);
    }
    return crc ;
}

static inline void process_ZSDataChecksum_command(conn *c, token_t * tokens, size_t ntokens){
    ZS_cguid_t cguid	= 0;
    uint64_t snap_seq	= -1;
    ZS_status_t ret;
    uint64_t checksum	= 0;
	uint32_t start_seq	= -1;
	uint32_t end_seq	= -1;

    ZS_range_meta_t *rmeta;
    ZS_range_data_t *rvalues;
    int n_out;
    int errors = 0;
    int i, k, x, y;
    int overall_cnt = 0;
    int act_key;
    int chunk_size = 20;
    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "snap_seq=", strlen("snap_seq="))) {
            snap_seq = atoi( tokens[i].value + strlen("snap_seq=") );
		} else if (0 == strncmp(tokens[i].value, "start_seq=", strlen("start_seq="))){
			start_seq = atoi( tokens[i].value + strlen("start_seq="));
		} else if (0 == strncmp(tokens[i].value, "end_seq=", strlen("end_seq="))){
			end_seq = atoi( tokens[i].value + strlen("end_seq="));
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    init_crc_table();
    /* Initialize rmeta */
    c->rmeta.key_start = NULL;
    c->rmeta.key_end   = NULL;
    c->rmeta.class_cmp_fn = NULL;
    c->rmeta.allowed_fn = NULL;
    c->rmeta.cb_data = NULL;

    if(snap_seq != -1){
        c->rmeta.flags = ZS_RANGE_SEQNO_LE;
        c->rmeta.end_seq = snap_seq;
	} else if ( start_seq != -1 && end_seq != -1) {
		c->rmeta.flags	= ZS_RANGE_SEQNO_GT_LE;
		c->rmeta.start_seq	= start_seq;
		c->rmeta.end_seq	= end_seq;	
    }else{
        c->rmeta.flags = 0;
    }

    ret = ZSGetRange(c->zs_thd_state,
                      cguid,
                      ZS_RANGE_PRIMARY_INDEX,
                      &c->cursor,
                      &c->rmeta);

    if (ret != ZS_SUCCESS) {
        fprintf(stdout, "ZSStartRangeQuery failed with status=%d\n", ret);
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
        return;
    }

    do {
        rvalues = (ZS_range_data_t *)
                   malloc(sizeof(ZS_range_data_t) * chunk_size);
        assert(rvalues);

        ret = ZSGetNextRange(c->zs_thd_state,
                              c->cursor,
                              chunk_size,
                              &n_out,
                              rvalues);

        if ((n_out != chunk_size) && (ret != ZS_QUERY_DONE)) {
            fprintf(stdout, "Error: Snapshot read chunk size "
                     "expected %d, actual %d\n", chunk_size, n_out);
            errors++;
        } else if ((ret != ZS_SUCCESS) && (ret != ZS_QUERY_DONE)) {
            fprintf(stdout, "Error: Snapshot read returned %d\n", ret);
            errors++;
            goto freeup;
        }
        for (i = 0; i < n_out; i++) {
			char *keyvalue;
			uint32_t length = rvalues[i].keylen + rvalues[i].datalen + 1;
			keyvalue	= (char *)malloc(length);
			assert(keyvalue);
			memset(keyvalue, 0, sizeof(keyvalue));
			snprintf(keyvalue, rvalues[i].keylen + 1, "%s", rvalues[i].key);
			//strncpy(keyvalue, rvalues[i].key , rvalues[i].keylen);
			strncat(keyvalue, rvalues[i].data, rvalues[i].datalen);	
            checksum	+=  crcCheckSum(keyvalue, length);
            overall_cnt++;
			free(keyvalue);
            ZSFreeBuffer(rvalues[i].key);
            ZSFreeBuffer(rvalues[i].data);
        }
freeup:
        free(rvalues);
    } while (ret != ZS_QUERY_DONE);

    ret = ZSGetRangeFinish(c->zs_thd_state, c->cursor);
    if (ret != ZS_SUCCESS) {
        fprintf(stdout, "ERROR: ZSGetRangeFinish failed ret=%d\n", ret);
    }

    if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK checksum=%ld, count=%d", checksum, overall_cnt);
        out_string(c, output_str);
    }
    return;

}
#endif

#ifdef USE_3_1
static inline void process_ZSRenameContainer_command(conn *c, token_t * tokens, size_t ntokens){

    char *name = NULL;
    ZS_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "name=", strlen("name="))) {
            name = tokens[i].value + strlen("name=");
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if ( NULL == name ) {
        out_string( c, "CLIENT_ERROR please specify a container name" );
        return;
    }

    ret = ZSRenameContainer (
            c->zs_thd_state,
            cguid,
            name
          );

     char output_str[100];
     if (ret != ZS_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
     } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
     }

     return;
}
#endif


#elif defined USE_SDF_API
/* ntokens is overwritten here... shrug.. */
static inline void process_SDFCreateContainer_command(conn *c, token_t *tokens, size_t ntokens) {  

    SDF_container_props_t props;
    char *cname = NULL;
    SDF_cguid_t  cguid;
    int fifo_mode = 0;
    int persistent = 0;
    int writethru = 0;
    int size = 0;
    SDF_durability_level_t durability_level = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cname=", strlen("cname="))) {
             cname = tokens[i].value + strlen("cname=");
        } else if (0 == strncmp(tokens[i].value, "fifo_mode=", strlen("fifo_mode="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "yes" ) ) {
                fifo_mode = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "no" ) ) {
                fifo_mode = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "fifo_mode" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "persistent=", strlen("persistent=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "yes" ) ) {
                persistent = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "no" ) ) {
                persistent = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "persistent" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "writethru=", strlen("writethru=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "yes" ) ) {
                writethru = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "no" ) ) {
                writethru = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "writethru" );
                return;
            } 

        } else if (0 == strncmp(tokens[i].value, "size=", strlen("size="))) {
            size = atoi( tokens[i].value + strlen("size=") );
        } else if (0 == strncmp(tokens[i].value, "durability_level=", strlen("durability_level="))) {
            
            if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "SDF_FULL_DURABILITY" ) ) {
                durability_level = SDF_FULL_DURABILITY;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "SDF_RELAXED_DURABILITY" ) ) {
                durability_level = SDF_RELAXED_DURABILITY;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "SDF_NO_DURABILITY" ) ) {
                durability_level = SDF_NO_DURABILITY;
            } else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for "
                        "durability_level" );
                return;
            }
        }
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if ( NULL == cname ) {
        out_string( c, "CLIENT_ERROR please specify a container name" );
        return;
    }

    props.fifo_mode = fifo_mode ? SDF_TRUE : SDF_FALSE; // xxxzzz
    props.container_type.persistence = persistent ? SDF_TRUE : SDF_FALSE;
    props.cache.writethru = writethru ? SDF_TRUE : SDF_FALSE;
    props.container_id.size = size ? size : 1024*1024 ; // default is 1G, unit is kb 
    props.durability_level = durability_level? durability_level : SDF_FULL_DURABILITY;     

    /* set container default property first */
    props.container_type.type = SDF_OBJECT_CONTAINER;
    props.container_type.caching_container = SDF_TRUE;
    props.container_type.async_writes = SDF_FALSE;

    // props.container_id.num_objs = 1000000; // is this enforced? xxxzzz

    props.shard.num_shards = 1;

    ret = SDFCreateContainer (
            c->sdf_thd_state,
            cname,
            &props,
            &cguid
          );
    
     char output_str[100];
     if (ret != SDF_SUCCESS) { 
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);       
     } else {
        sprintf(output_str, "OK cguid=%llu", cguid);
        out_string(c, output_str);           
     }
  
     return;   
}

static inline void process_SDFOpenContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    SDF_container_mode_t mode = SDF_READ_WRITE_MODE;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "mode=", strlen("mode="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("mode="), "SDF_READ_MODE" ) ) {
                mode = SDF_READ_MODE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("mode="), "SDF_WRITE_MODE" ) ) {
                mode = SDF_WRITE_MODE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("mode="), "SDF_APPEND_MODE" ) ) {
                mode = SDF_APPEND_MODE;
            } else if ( 0 == strcmp( tokens[i].value + strlen("mode="), "SDF_READ_WRITE_MODE" ) ) {
                mode = SDF_READ_WRITE_MODE;
            } else {
                out_string( c, "CLIENT_ERROR please specify correct mode for for "
                        "SDF_container_mode_t" );
                return;
            }

        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = SDFOpenContainer(
            c->sdf_thd_state,
            cguid,
            mode
          );
 
    char output_str[100];
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFCloseContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = SDFCloseContainer(
            c->sdf_thd_state,
            cguid
          );
 
    char output_str[100];
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFDeleteContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = SDFDeleteContainer(
            c->sdf_thd_state,
            cguid
          );
 
    char output_str[100];
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFStartContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = SDFStartContainer(
            c->sdf_thd_state,
            cguid
          );
 
    char output_str[100];
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFStopContainer_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = SDFStopContainer(
            c->sdf_thd_state,
            cguid
          );
 
    char output_str[100];
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFGetContainerProps_command(conn *c, token_t *tokens, size_t ntokens) {
    
    SDF_container_props_t props;
    SDF_cguid_t cguid = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    ret = SDFGetContainerProps(
            c->sdf_thd_state,
            cguid,
            &props
          );
 
    char output_str[100];
    int offset = 0;
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK\n");
        offset += sprintf(output_str+offset, "cguid=%lld\n", props.cguid);
        offset += sprintf(output_str+offset, "fifo_mode=%u\n", props.fifo_mode);
        offset += sprintf(output_str+offset, "persistence=%d\n", props.container_type.persistence);
        offset += sprintf(output_str+offset, "writethru=%d\n", props.cache.writethru);
        offset += sprintf(output_str+offset, "size=%llu\n", props.container_id.size);
        offset += sprintf(output_str+offset, "durability_level=%d", props.durability_level);

        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFSetContainerProps_command(conn *c, token_t *tokens, size_t ntokens) {  

    SDF_container_props_t props;
    SDF_cguid_t  cguid;
    int fifo_mode = 0;
    int persistent = 0;
    int writethru = 0;
    int size = 0;
    SDF_durability_level_t durability_level = 0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "fifo_mode=", strlen("fifo_mode="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "yes" ) ) {
                fifo_mode = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("fifo_mode="), "no" ) ) {
                fifo_mode = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "fifo_mode" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "persistent=", strlen("persistent=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "yes" ) ) {
                persistent = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("persistent="), "no" ) ) {
                persistent = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "persistent" );
                return;
            }

        } else if (0 == strncmp(tokens[i].value, "writethru=", strlen("writethru=")) ) {

            if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "yes" ) ) {
                writethru = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("writethru="), "no" ) ) {
                writethru = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for "
                        "writethru" );
                return;
            } 
        } else if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "size=", strlen("size="))) {
            size = atoi( tokens[i].value + strlen("size=") );
        } else if (0 == strncmp(tokens[i].value, "durability_level=", strlen("durability_level="))) {

            if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "SDF_FULL_DURABILITY" ) ) {
                durability_level = SDF_FULL_DURABILITY;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "SDF_RELAXED_DURABILITY" ) ) {
                durability_level = SDF_RELAXED_DURABILITY;
            } else if ( 0 == strcmp( tokens[i].value + strlen("durability_level="), "SDF_NO_DURABILITY" ) ) {
                durability_level = SDF_NO_DURABILITY;
            } 
        }
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    props.fifo_mode = fifo_mode ? SDF_TRUE : SDF_FALSE; // xxxzzz
    props.container_type.persistence = persistent ? SDF_TRUE : SDF_FALSE;
    props.cache.writethru = writethru ? SDF_TRUE : SDF_FALSE;
    props.container_id.size = size ? size : 1024*1024 ; // default is 1G, unit is kb
    props.durability_level = durability_level? durability_level : SDF_FULL_DURABILITY;

    /* set container default property first */

    props.container_type.type = SDF_OBJECT_CONTAINER;
    props.container_type.caching_container = SDF_TRUE;
    props.container_type.async_writes = SDF_FALSE;

    props.container_id.num_objs = 1000000; // is this enforced? xxxzzz

    props.shard.num_shards = 1;
    
    ret = SDFSetContainerProps (
            c->sdf_thd_state,
            cguid,
            &props
          );
   
    char output_str[100];
    if (ret != SDF_SUCCESS) { 
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);       
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);           
    }
  
    return;   
}

static inline void process_SDFGetContainers_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguids[MCD_MAX_NUM_CNTRS];
    uint32_t n_cguids = 0;
    ZS_status_t ret;

    ret = SDFGetContainers(
            c->sdf_thd_state,
            cguids,
            &n_cguids
          );
 
    char output_str[2048];
    int offset = 0;
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        offset += sprintf(output_str, "OK n_cguids=%u\n", n_cguids);
        for (int i = 0; i < n_cguids; i++) {
            offset += sprintf(output_str+offset, "%llu ", cguids[i]);             
        }
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFFlushContainers_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    SDF_time_t current_time=0;
    ZS_status_t ret;

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    ret = SDFFlushContainer(
            c->sdf_thd_state,
            cguid,
            current_time
          );
 
    char output_str[100];
    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFCreateBufferedObject_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    SDF_time_t current_time = 0;
    SDF_time_t expiry_time = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int data_offset = 0;
    int data_len = 0;
    int nops = 1;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "data_offset=", strlen("data_offset="))) {  
            data_offset = atoi( tokens[i].value + strlen("data_offset=") );
        } else if (0 == strncmp(tokens[i].value, "data_len=", strlen("data_len="))) {   
            data_len = atoi( tokens[i].value + strlen("data_len=") );
        } else if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        } else if (0 == strncmp(tokens[i].value, "expiry_time=", strlen("expiry_time="))) { 
            expiry_time = atoi( tokens[i].value + strlen("expiry_time=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    if (data_offset < 0 || data_offset > CHAR_BLK_SIZE || data_len < 0 || data_len > MAX_DATA_LEN) {
        out_string( c, "CLIENT_ERROR please do not let data cross the char block table");
        return;
    }

    int offset = 0;
    for (int i = 0; i < nops; i++) {

        ret = SDFCreateBufferedObject(
                c->sdf_thd_state,
                cguid,
                char_block + key_offset + i,
                key_len,
                char_block + data_offset + i,
                data_len,
                current_time,
                expiry_time
              );

        if (ret != SDF_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th SDFCreateBufferedObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 
    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;
}

static inline void process_SDFGetForReadBufferedObject_command(conn *c, token_t *tokens, size_t ntokens) {
    SDF_cguid_t cguid = 0;
    SDF_time_t current_time = 0;
    SDF_time_t expiry_time = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int data_offset = 0;
    int data_len = 0;

    char *data = NULL; 
    uint64_t get_data_len = 0;
 
    int nops = 1;
    int check = 0;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "data_offset=", strlen("data_offset="))) {  
            data_offset = atoi( tokens[i].value + strlen("data_offset=") );
        } else if (0 == strncmp(tokens[i].value, "data_len=", strlen("data_len="))) {   
            data_len = atoi( tokens[i].value + strlen("data_len=") );
        } else if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        } else if (0 == strncmp(tokens[i].value, "check=", strlen("check="))) { 
 
            if ( 0 == strcmp( tokens[i].value + strlen("check="), "yes" ) ) {
                check = 1;
            } else if ( 0 == strcmp( tokens[i].value + strlen("check="), "no" ) ) {
                check = 0;
            } else {
                out_string( c, "CLIENT_ERROR please specify yes|no for check" );
                return;
            }
        } 
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
 
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    int offset = 0;
    int check_fail = 0;
    for (int i = 0; i < nops; i++) {
        
        ret = SDFGetForReadBufferedObject(
                c->sdf_thd_state,
                cguid,
                char_block + key_offset + i,
                key_len,
                &data,
                &get_data_len,
                current_time,
                &expiry_time
              );

        if (ret != SDF_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th SDFGetForReadBufferedObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 

        if (check) {
            if ((get_data_len != data_len) || (0 != strncmp(data, char_block+data_offset+i, get_data_len)))  {
                check_fail++;    
            }
        }

        SDFFreeBuffer(c->sdf_thd_state, data);
        
        data = NULL;
    }

    if (0 == check_fail) { 
        sprintf(output_str, "OK"); 
    } else {
        sprintf(output_str, "SERVER_ERROR %d items check failed", check_fail);
    }

    out_string(c, output_str);

    return;
}

static inline void process_SDFSetBufferedObject_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    SDF_time_t current_time = 0;
    SDF_time_t expiry_time = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int data_offset = 0;
    int data_len = 0;
    int nops = 1;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "data_offset=", strlen("data_offset="))) {  
            data_offset = atoi( tokens[i].value + strlen("data_offset=") );
        } else if (0 == strncmp(tokens[i].value, "data_len=", strlen("data_len="))) {   
            data_len = atoi( tokens[i].value + strlen("data_len=") );
        } else if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        } else if (0 == strncmp(tokens[i].value, "expiry_time=", strlen("expiry_time="))) { 
            expiry_time = atoi( tokens[i].value + strlen("expiry_time=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    if (data_offset < 0 || data_offset > CHAR_BLK_SIZE || data_len < 0 || data_len > MAX_DATA_LEN) {
        out_string( c, "CLIENT_ERROR please do not let data cross the char block table");
        return;
    }

    int offset = 0;
    for (int i = 0; i < nops; i++) {

        ret = SDFSetBufferedObject(
                c->sdf_thd_state,
                cguid,
                char_block + key_offset + i,
                key_len,
                char_block + data_offset + i,
                data_len,
                current_time,
                expiry_time
              );

        if (ret != SDF_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th SDFSetBufferedObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 
    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;
}

static inline void process_SDFPutBufferedObject_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    SDF_time_t current_time = 0;
    SDF_time_t expiry_time = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int data_offset = 0;
    int data_len = 0;
    int nops = 1;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "data_offset=", strlen("data_offset="))) {  
            data_offset = atoi( tokens[i].value + strlen("data_offset=") );
        } else if (0 == strncmp(tokens[i].value, "data_len=", strlen("data_len="))) {   
            data_len = atoi( tokens[i].value + strlen("data_len=") );
        } else if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        } else if (0 == strncmp(tokens[i].value, "expiry_time=", strlen("expiry_time="))) { 
            expiry_time = atoi( tokens[i].value + strlen("expiry_time=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    if (data_offset < 0 || data_offset > CHAR_BLK_SIZE || data_len < 0 || data_len > MAX_DATA_LEN) {
        out_string( c, "CLIENT_ERROR please do not let data cross the char block table");
        return;
    }

    int offset = 0;
    for (int i = 0; i < nops; i++) {

        ret = SDFPutBufferedObject(
                c->sdf_thd_state,
                cguid,
                char_block + key_offset + i,
                key_len,
                char_block + data_offset + i,
                data_len,
                current_time,
                expiry_time
              );

        if (ret != SDF_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th SDFPutBufferedObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 
    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;
}

static inline void process_SDFRemoveObjectWithExpiry_command(conn *c, token_t *tokens, size_t ntokens) {
    
    SDF_cguid_t cguid = 0;
    SDF_time_t current_time = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int nops = 1;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    int offset = 0;
    for (int i = 0; i < nops; i++) {

        ret = SDFRemoveObjectWithExpiry(
                c->sdf_thd_state,
                cguid,
                char_block + key_offset + i,
                key_len,
                current_time
              );

        if (ret != SDF_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th SDFRemoveObjectWithExpiry failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 
    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;

}

static inline void process_SDFFlushObject_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    SDF_time_t current_time = 0;
    ZS_status_t ret;

    int key_offset = 0;
    int key_len = 0;
    int nops = 1;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else if (0 == strncmp(tokens[i].value, "key_offset=", strlen("key_offset="))) { 
            key_offset = atoi( tokens[i].value + strlen("key_offset=") );
        } else if (0 == strncmp(tokens[i].value, "key_len=", strlen("key_len="))) {  
            key_len = atoi( tokens[i].value + strlen("key_len=") );
        } else if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        } else if (0 == strncmp(tokens[i].value, "nops=", strlen("nops="))) { 
            nops = atoi( tokens[i].value + strlen("nops=") );
        }  
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }
    
    if (key_offset < 0 || key_offset > CHAR_BLK_SIZE || key_len < 0 || key_len > MAX_KEY_LEN) {
        out_string( c, "CLIENT_ERROR please do not let key cross the char block table");
        return;
    }

    int offset = 0;
    for (int i = 0; i < nops; i++) {

        ret = SDFFlushObject(
                c->sdf_thd_state,
                cguid,
                char_block + key_offset + i,
                key_len,
                current_time
              );

        if (ret != SDF_SUCCESS) {
            offset += sprintf(output_str, "SERVER_ERROR %s\n",  ZSStrError(ret));
            offset += sprintf(output_str+offset, "%d th SDFFlushObject failed, key_offset=%d", i, key_offset+i);
            out_string(c, output_str);
            return;
        } 
    }

    sprintf(output_str, "OK");
    out_string(c, output_str);

    return;

}

static inline void process_SDFEnumerateContainerObjects_command(conn *c, token_t *tokens, size_t ntokens) {

    SDF_cguid_t cguid = 0;
    ZS_status_t ret;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cguid=", strlen("cguid="))) {
            cguid = atoi( tokens[i].value + strlen("cguid=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    if (0 == cguid) {
        out_string( c, "CLIENT_ERROR please specify a cguid" );
        return;
    }

    ret = SDFEnumerateContainerObjects(
              c->sdf_thd_state,
              cguid,
              &c->iterator
          );

    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;
}

static inline void process_SDFNextEnumeratedObject_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_status_t ret;

    char output_str[100];

    char *get_key = NULL;
    uint32_t get_key_len = 0;
    char *get_data = NULL; 
    uint64_t get_data_len = 0;
    int success_count = 0;
    
    ret = SDFNextEnumeratedObject(
              c->sdf_thd_state,
              c->iterator,
              &get_key,
              &get_key_len,
              &get_data,
              &get_data_len    
          );
    
    SDFFreeBuffer(c->sdf_thd_state, get_key);
    SDFFreeBuffer(c->sdf_thd_state, get_data);

    while (ret == SDF_SUCCESS) {

        success_count++;
        ret = SDFNextEnumeratedObject(
              c->sdf_thd_state,
              c->iterator,
              &get_key,
              &get_key_len,
              &get_data,
              &get_data_len    
          ); 
    
        SDFFreeBuffer(c->sdf_thd_state, get_key);
        SDFFreeBuffer(c->sdf_thd_state, get_data);
    }
 
    if (0 == success_count) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK enumerate %d objects", success_count);
        out_string(c, output_str);
    }
   
}

static inline void process_SDFFinishEnumeration_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_status_t ret;

    char output_str[100];

    ret = SDFFinishEnumeration(
              c->sdf_thd_state,
              c->iterator
          );

    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    c->iterator = NULL;

    return;
}

static inline void process_SDFGetStats_command(conn *c, token_t *tokens, size_t ntokens) {
    
    out_string(c, "SERVER_ERROR SDFGetStats is not implemented yet");

    return;
}

static inline void process_SDFGetContainerStats_command(conn *c, token_t *tokens, size_t ntokens) {
 
    out_string(c, "SERVER_ERROR SDFGetContainerStats is not implemented yet");

    return;
}

static inline void process_SDFGenerateCguid_command(conn *c, token_t *tokens, size_t ntokens) {

    int64_t cntr_id64 = 0;
    SDF_cguid_t cguid = 0;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "cntr_id64=", strlen("cntr_id64="))) {
            cntr_id64 = atoi( tokens[i].value + strlen("cntr_id64=") );
        } else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    cguid = SDFGenerateCguid(
              c->sdf_thd_state,
              cntr_id64
          );

    sprintf(output_str, "OK cguid=%llu", cguid);
    out_string(c, output_str);           
 
    return;
}

static inline void process_SDFFlushCache_command(conn *c, token_t *tokens, size_t ntokens) {

    ZS_status_t ret;
    SDF_time_t current_time = 0;

    char output_str[100];

    for (int i = COMMAND_TOKEN+1; i < ntokens-1; i++) {
        if (0 == strncmp(tokens[i].value, "current_time=", strlen("current_time="))) {
            current_time = atoi( tokens[i].value + strlen("current_time=") );
        }
        else {
            out_string( c, "CLIENT_ERROR unknown option" );
            return;
        }
    }

    ret = SDFFlushCache(
              c->sdf_thd_state,
              current_time
          );

    if (ret != SDF_SUCCESS) {
        sprintf(output_str, "SERVER_ERROR %s",  ZSStrError(ret));
        out_string(c, output_str);
    } else {
        sprintf(output_str, "OK");
        out_string(c, output_str);
    }

    return;

}
#endif

static void process_command(conn *c, char *command) {

    token_t tokens[MAX_TOKENS];
    size_t ntokens;
//    int comm;

    assert(c != NULL);

    if (settings.verbose > 1) 
        fprintf(stderr, "<%d %s\n", c->sfd, command);

    /*   
    * for commands set/add/replace, we build an item and read the data
    * directly into it, then continue in nread_complete().
    */

    c->msgcurr = 0; 
    c->msgused = 0; 
    c->iovused = 0; 
    if (add_msghdr(c) != 0) { 
        out_string(c, "SERVER_ERROR out of memory preparing response");
        return;
    }    

    if (command == NULL || 0 == strcmp(command , "")) {
	out_string(c, "CLIENT_ERROR: empty string.");
	return;
    }
    ntokens = tokenize_command(command, tokens, MAX_TOKENS);

#ifdef USE_ZS_API    
    if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSOpenContainer")) {
        process_ZSOpenContainer_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSCloseContainer")) {
        process_ZSCloseContainer_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSDeleteContainer")) {
        process_ZSDeleteContainer_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetContainers")) {
        process_ZSGetContainers_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetContainerProps")) {
        process_ZSGetContainerProps_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSSetContainerProps")) {
        process_ZSSetContainerProps_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSReadObject")) {
        process_ZSReadObject_command(c, tokens, ntokens);      
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSWriteObject")) {
        process_ZSWriteObject_command(c, tokens, ntokens); 
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSDeleteObject")) {
        process_ZSDeleteObject_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSEnumerateContainerObjects")) {
        process_ZSEnumerateContainerObjects_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSNextEnumeratedObject")) {
        process_ZSNextEnumeratedObject_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSFinishEnumeration")) {
        process_ZSFinishEnumeration_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSFlushObject")) {
        process_ZSFlushObject_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSFlushContainer")) {
        process_ZSFlushContainer_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSFlushCache")) {
        process_ZSFlushCache_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetStats")) {
        process_ZSGetStats_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetContainerStats")) {
        process_ZSGetContainerStats_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSTransactionStart")) {
        process_ZSTransactionStart_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSTransactionCommit")) {
        process_ZSTransactionCommit_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSTransactionRollback")) {
        process_ZSTransactionRollback_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSTransactionSetMode")) {
        process_ZSTransactionSetMode_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSTransactionGetMode")) {
        process_ZSTransactionGetMode_command(c, tokens, ntokens);
    } 
#ifdef USE_BTREE
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetRange")) {
        process_ZSGetRange_command(c, tokens, ntokens);
    } 
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetNextRange")) {
        process_ZSGetNextRange_command(c, tokens, ntokens);
    }  
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetRangeFinish")) {
        process_ZSGetRangeFinish_command(c, tokens, ntokens);
    }   
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSMPut")) {
        process_ZSMPut_command(c, tokens, ntokens);
    }      
#ifdef USE_SNAPSHOT
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSCreateContainerSnapshot")) {
        process_ZSCreateContainerSnapshot_command(c, tokens, ntokens);
    }
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSDeleteContainerSnapshot")) {
        process_ZSDeleteContainerSnapshot_command(c, tokens, ntokens);
    }
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSGetContainerSnapshots")) {
        process_ZSGetContainerSnapshots_command(c, tokens, ntokens);
    }
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSDataChecksum")) {
        process_ZSDataChecksum_command(c, tokens, ntokens);
    }
#endif
#endif
#ifdef USE_3_1
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSRenameContainer")) {
        process_ZSRenameContainer_command(c, tokens, ntokens);
    }
    else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "ZSEnumeratePGObjects")) {
        process_ZSEnumeratePGObjects_command(c, tokens, ntokens);
    }
#endif
    else if (ntokens == 2 && (0 == strcmp(tokens[COMMAND_TOKEN].value, "Hello"))) {  
        out_string(c, "Hello, beauty");
    } else if (ntokens == 2 && (0 == strcmp(tokens[COMMAND_TOKEN].value, "quit"))) {
        conn_set_state(c, conn_closing);
    } else if (ntokens == 2 && (0 == strcmp(tokens[COMMAND_TOKEN].value, "exit"))) {
		grace_shutdown();
        exit(0);
    }   
    else {
        out_string(c, "ERROR");
    }

#elif defined USE_SDF_API
    if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFCreateContainer")) {
        process_SDFCreateContainer_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFOpenContainer")) {
        process_SDFOpenContainer_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFCloseContainer")) { 
        process_SDFCloseContainer_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFDeleteContainer")) { 
        process_SDFDeleteContainer_command(c, tokens, ntokens);       
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFStartContainer")) { 
        process_SDFStartContainer_command(c, tokens, ntokens);              
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFStopContainer")) { 
        process_SDFStopContainer_command(c, tokens, ntokens);                         
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFGetContainerProps")) {
        process_SDFGetContainerProps_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFSetContainerProps")) { 
        process_SDFSetContainerProps_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFGetContainers")) { 
        process_SDFGetContainers_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFFlushContainer")) { 
        process_SDFFlushContainers_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFCreateBufferedObject")) { 
        process_SDFCreateBufferedObject_command(c, tokens, ntokens);       
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFGetForReadBufferedObject")) {  
        process_SDFGetForReadBufferedObject_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFSetBufferedObject")) {  
        process_SDFSetBufferedObject_command(c, tokens, ntokens);
    }  else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFPutBufferedObject")) {  
        process_SDFPutBufferedObject_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFRemoveObjectWithExpiry")) {  
        process_SDFRemoveObjectWithExpiry_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFFlushObject")) {  
        process_SDFFlushObject_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFEnumerateContainerObjects")) {  
        process_SDFEnumerateContainerObjects_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFNextEnumeratedObject")) {  
        process_SDFNextEnumeratedObject_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFFinishEnumeration")) {  
        process_SDFFinishEnumeration_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFGetStats")) {  
        process_SDFGetStats_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFGetContainerStats")) {  
        process_SDFGetContainerStats_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFGenerateCguid")) {  
        process_SDFGenerateCguid_command(c, tokens, ntokens);
    } else if (0 == strcmp(tokens[COMMAND_TOKEN].value, "SDFFlushCache")) {  
        process_SDFFlushCache_command(c, tokens, ntokens);
    } else if (ntokens == 2 && (0 == strcmp(tokens[COMMAND_TOKEN].value, "Hello"))) {  
        out_string(c, "Hello, beauty");
    } else if (ntokens == 2 && (0 == strcmp(tokens[COMMAND_TOKEN].value, "quit"))) {
        conn_set_state(c, conn_closing);
    } else if (ntokens == 2 && (0 == strcmp(tokens[COMMAND_TOKEN].value, "exit"))) {
        exit(0);
    }   
    else {
        out_string(c, "ERROR");
    }
#endif

    return;    
}

static void reset_cmd_handler(conn *c) {
    c->cmd = -1;
//    c->substate = bin_no_state;
//    if(c->item != NULL) {
//        item_remove(c->item);
//        c->item = NULL;
//    }
    conn_shrink(c);
    if (c->rbytes > 0) {
        conn_set_state(c, conn_parse_cmd);
    } else {
        conn_set_state(c, conn_waiting);
    }
}

static void out_string(conn *c, const char *str) {
    size_t len;

    assert(c != NULL);

    if (c->noreply) {
        if (settings.verbose > 1)
            fprintf(stderr, ">%d NOREPLY %s\n", c->sfd, str);
        c->noreply = false;
        conn_set_state(c, conn_new_cmd);
        return;
    }

    if (settings.verbose > 1)
        fprintf(stderr, ">%d %s\n", c->sfd, str);

    len = strlen(str);
    if ((len + 2) > c->wsize) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }

    memcpy(c->wbuf, str, len);
    memcpy(c->wbuf + len, "\r\n", 2);
    c->wbytes = len + 2;
    c->wcurr = c->wbuf;

    conn_set_state(c, conn_write);
    c->write_and_go = conn_new_cmd;
    return;
}


/**
* Convert a state name to a human readable form.
*/
static const char *state_text(enum conn_states state) {
    const char* const statenames[] = { "conn_listening",
        "conn_new_cmd",
        "conn_waiting",
        "conn_read",
        "conn_parse_cmd",
        "conn_write",
        "conn_nread",
        "conn_swallow",
        "conn_closing",
        "conn_mwrite" };
    return statenames[state];
}

/*
* Sets a connection's current state in the state machine. Any special
* processing that needs to happen on certain state transitions can
* happen here.
*/
static void conn_set_state(conn *c, enum conn_states state) {
    assert(c != NULL);
    assert(state >= conn_listening && state < conn_max_state);

    if (state != c->state) {
        if (settings.verbose > 2) {
            fprintf(stderr, "%d: going from %s to %s\n",
                    c->sfd, state_text(c->state),
                    state_text(state));
        }

        c->state = state;

        if (state == conn_write || state == conn_mwrite) {
//            MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);
        }
    }
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(conn *c) {
    assert(c != NULL);
    assert(c->rcurr <= (c->rbuf + c->rsize));
    assert(c->rbytes > 0);

//    if (c->protocol == negotiating_prot || c->transport == udp_transport)  {
//        if ((unsigned char)c->rbuf[0] == (unsigned char)PROTOCOL_BINARY_REQ) {
//            c->protocol = binary_prot;
//        } else {
            c->protocol = ascii_prot;
//        }    

        if (settings.verbose > 1) { 
            fprintf(stderr, "%d: Client using the %s protocol\n", c->sfd,
                    prot_text(c->protocol));
        }    
//    }    

    char *el, *cont;

    if (c->rbytes == 0)
        return 0;

    el = memchr(c->rcurr, '\n', c->rbytes);
    if (!el) {
        if (c->rbytes > 1024) {
            /*
            * We didn't have a '\n' in the first k. This _has_ to be a
            * large multiget, if not we should just nuke the connection.
            */
            char *ptr = c->rcurr;
            while (*ptr == ' ') { /* ignore leading whitespaces */
                ++ptr;
            }

            if (ptr - c->rcurr > 100 ||
                    (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5))) {

                conn_set_state(c, conn_closing);
                return 1;
            }
        }

        return 0;
    }
    cont = el + 1;
    if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
        el--;
    }
    *el = '\0';

    assert(cont <= (c->rcurr + c->rbytes));

    process_command(c, c->rcurr);

    c->rbytes -= (cont - c->rcurr);
    c->rcurr = cont;

    assert(c->rcurr <= (c->rbuf + c->rsize));

    return 1;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 *
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start looking
 * at the data I've got after a number of reallocs...
 *
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c) {
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res; 
    int num_allocs = 0; 
    assert(c != NULL);

    if (c->rcurr != c->rbuf) {
        if (c->rbytes != 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes);
        c->rcurr = c->rbuf;
    }    

    while (1) {
        if (c->rbytes >= c->rsize) {
            if (num_allocs == 4) { 
                return gotdata;
            }    
            ++num_allocs;
            char *new_rbuf = realloc(c->rbuf, c->rsize * 2);
            if (!new_rbuf) {
                if (settings.verbose > 0) 
                    fprintf(stderr, "Couldn't realloc input buffer\n");
                c->rbytes = 0; /* ignore what we read */
                out_string(c, "SERVER_ERROR out of memory reading request");
                c->write_and_go = conn_closing;
                return READ_MEMORY_ERROR;
            }    
            c->rcurr = c->rbuf = new_rbuf;
            c->rsize *= 2;
        }    

        int avail = c->rsize - c->rbytes;
        res = read(c->sfd, c->rbuf + c->rbytes, avail);
        if (res > 0) { 
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.bytes_read += res;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            gotdata = READ_DATA_RECEIVED;
            c->rbytes += res;
            if (res == avail) {
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            return READ_ERROR;
        }
        if (res == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            return READ_ERROR;
        }
    }
    return gotdata;
}

/*
* Transmit the next chunk of data from our list of msgbuf structures.
*
* Returns:
*   TRANSMIT_COMPLETE   All done writing.
*   TRANSMIT_INCOMPLETE More data remaining to write.
*   TRANSMIT_SOFT_ERROR Can't write any more right now.
*   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
*/
static enum transmit_result transmit(conn *c) {
    assert(c != NULL);

    if (c->msgcurr < c->msgused &&
            c->msglist[c->msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        c->msgcurr++;
    }
    if (c->msgcurr < c->msgused) {
        ssize_t res;
        struct msghdr *m = &c->msglist[c->msgcurr];

        res = sendmsg(c->sfd, m, 0);
        if (res > 0) {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.bytes_written += res;
            pthread_mutex_unlock(&c->thread->stats.mutex);

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= m->msg_iov->iov_len) {
                res -= m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0) {
                m->msg_iov->iov_base = (caddr_t)m->msg_iov->iov_base + res;
                m->msg_iov->iov_len -= res;
            }
            return TRANSMIT_INCOMPLETE;
        }
        if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0)
                    fprintf(stderr, "Couldn't update event\n");
                conn_set_state(c, conn_closing);
                return TRANSMIT_HARD_ERROR;
            }
            return TRANSMIT_SOFT_ERROR;
        }
        /* if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
           we have a real error, on which we close the connection */
        if (settings.verbose > 0)
            perror("Failed to write, and not due to blocking");
        if (IS_UDP(c->transport))
            conn_set_state(c, conn_read);
        else
            conn_set_state(c, conn_closing);
        return TRANSMIT_HARD_ERROR;
    } else {
        return TRANSMIT_COMPLETE;
    }
}

static void drive_machine(conn *c) {

    bool stop = false;
    int sfd, flags = 1;
    socklen_t addrlen;
    struct sockaddr_storage addr;
    int nreqs = settings.reqs_per_event;
    int res;

    assert(c != NULL);

    while (!stop) {

        switch(c->state) {
            case conn_listening:
                addrlen = sizeof(addr);
                if ((sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen)) == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        /* these are transient, so don't log anything */
                        stop = true;
                    } else if (errno == EMFILE) {
                        if (settings.verbose > 0)
                            fprintf(stderr, "Too many open connections\n");
                        accept_new_conns(false);
                        stop = true;
                    } else {
                        perror("accept()");
                        stop = true;
                    }
                    break;
                }
                if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
                        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
                    perror("setting O_NONBLOCK");
                    close(sfd);
                    break;
                }

                dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST,
                        DATA_BUFFER_SIZE, tcp_transport);
                stop = true;
                break;

            case conn_waiting:
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }

                conn_set_state(c, conn_read);
                stop = true;
                break;

            case conn_read:
            //    res = IS_UDP(c->transport) ? try_read_udp(c) : try_read_network(c);
                res = try_read_network(c);

                switch (res) {
                    case READ_NO_DATA_RECEIVED:
                        conn_set_state(c, conn_waiting);
                        break;
                    case READ_DATA_RECEIVED:
                        conn_set_state(c, conn_parse_cmd);
                        break;
                    case READ_ERROR:
                        conn_set_state(c, conn_closing);
                        break;
                    case READ_MEMORY_ERROR: /* Failed to allocate more memory */
                        /* State already set by try_read_network */
                        break;
                }
                break;

            case conn_parse_cmd :
                if (try_read_command(c) == 0) {
                    /* wee need more data! */
                    conn_set_state(c, conn_waiting);
                }

                break;

            case conn_new_cmd:
                /* Only process nreqs at a time to avoid starving other
                   connections */

                --nreqs;
                if (nreqs >= 0) {
                    reset_cmd_handler(c);
                } else {
                    pthread_mutex_lock(&c->thread->stats.mutex);
                    c->thread->stats.conn_yields++;
                    pthread_mutex_unlock(&c->thread->stats.mutex);
                    if (c->rbytes > 0) {
                        /* We have already read in data into the input buffer,
                           so libevent will most likely not signal read events
                           on the socket (unless more data is available. As a
                           hack we should just put in a request to write data,
                           because that should be possible ;-)
                           */
                        if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                            if (settings.verbose > 0)
                                fprintf(stderr, "Couldn't update event\n");
                            conn_set_state(c, conn_closing);
                        }
                    }
                    stop = true;
                }
                break;
            case conn_swallow:
                /* we are reading sbytes and throwing them away */
                if (c->sbytes == 0) {
                    conn_set_state(c, conn_new_cmd);
                    break;
                }

                /* first check if we have leftovers in the conn_read buffer */
                if (c->rbytes > 0) {
                    int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
                    c->sbytes -= tocopy;
                    c->rcurr += tocopy;
                    c->rbytes -= tocopy;
                    break;
                }

                /*  now try reading from the socket */
                res = read(c->sfd, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize);
                if (res > 0) {
                    pthread_mutex_lock(&c->thread->stats.mutex);
                    c->thread->stats.bytes_read += res;
                    pthread_mutex_unlock(&c->thread->stats.mutex);
                    c->sbytes -= res;
                    break;
                }
                if (res == 0) { /* end of stream */
                    conn_set_state(c, conn_closing);
                    break;
                }
                if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    if (!update_event(c, EV_READ | EV_PERSIST)) {
                        if (settings.verbose > 0)
                            fprintf(stderr, "Couldn't update event\n");
                        conn_set_state(c, conn_closing);
                        break;
                    }
                    stop = true;
                    break;
                }
                /* otherwise we have a real error, on which we close the connection */
                if (settings.verbose > 0)
                    fprintf(stderr, "Failed to read, and not due to blocking\n");
 
                conn_set_state(c, conn_closing);
                break;

            case conn_write:
                /*
                * We want to write out a simple response. If we haven't already,
                * assemble it into a msgbuf list (this will be a single-entry
                * list for TCP or a two-entry list for UDP).
                */
                if (c->iovused == 0 || (IS_UDP(c->transport) && c->iovused == 1)) {
                    if (add_iov(c, c->wcurr, c->wbytes) != 0) {
                        if (settings.verbose > 0)
                            fprintf(stderr, "Couldn't build response\n");
                        conn_set_state(c, conn_closing);
                        break;
                    }
                }

                /* fall through... */
            case conn_mwrite:
                /*
                if (IS_UDP(c->transport) && c->msgcurr == 0 && build_udp_headers(c) != 0) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Failed to build UDP headers\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                */
                switch (transmit(c)) {
                    case TRANSMIT_COMPLETE:
                        
                        if (c->state == conn_mwrite) {
                            /*
                            while (c->ileft > 0) {
                                item *it = *(c->icurr);
                                assert((it->it_flags & ITEM_SLABBED) == 0);
                                item_remove(it);
                                c->icurr++;
                                c->ileft--;
                            }
                            while (c->suffixleft > 0) {
                                char *suffix = *(c->suffixcurr);
                                cache_free(c->thread->suffix_cache, suffix);
                                c->suffixcurr++;
                                c->suffixleft--;
                            }
                            */
                            /* XXX:  I don't know why this wasn't the general case */
                            /*
                            if(c->protocol == binary_prot) {
                                conn_set_state(c, c->write_and_go);
                            } else {
                                conn_set_state(c, conn_new_cmd);
                            }
                            */
                        } else if (c->state == conn_write) {
                            if (c->write_and_free) {
                                free(c->write_and_free);
                                c->write_and_free = 0;
                            }
                            conn_set_state(c, c->write_and_go);
                        } else {
                            if (settings.verbose > 0)
                                fprintf(stderr, "Unexpected state %d\n", c->state);
                            conn_set_state(c, conn_closing);
                        }
                        break;

                    case TRANSMIT_INCOMPLETE:
                    case TRANSMIT_HARD_ERROR:
                        break;                   /* Continue in state machine. */

                    case TRANSMIT_SOFT_ERROR:
                        stop = true;
                        break;
                }
                break;

            case conn_closing:
            //    if (IS_UDP(c->transport))
            //        conn_cleanup(c);
            //    else
                    conn_close(c);
                stop = true;
                break;
            case conn_max_state:
                assert(false);
                break;

            default:
                break;
        }
    }

    return;
}

void event_handler(const int fd, const short which, void *arg) {
    conn *c;

    c = (conn *)arg;
    assert(c != NULL);

    c->which = which;

#ifdef USE_ZS_API
    c->zs_thd_state = zs_thd_state;
#elif defined USE_SDF_API
    c->sdf_thd_state = sdf_thd_state;
#endif

    /* sanity */
    if (fd != c->sfd) {
        if (settings.verbose > 0) 
            fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
        conn_close(c);
        return;
    }    

    drive_machine(c);

    /* wait for next event */
    return;
}

static int new_socket(struct addrinfo *ai) {
    int sfd;
    int flags;

    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
            fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
    return sfd;
}

static const char *prot_text(enum protocol prot) {
    char *rv = "unknown";
    switch(prot) {
        case ascii_prot:
            rv = "ascii";
            break;
        case binary_prot:
            rv = "binary";
            break;
        case negotiating_prot:
            rv = "auto-negotiate";
            break;
    }    
    return rv;
}

conn *conn_new(const int sfd, enum conn_states init_state,
        const int event_flags,
        const int read_buffer_size, enum network_transport transport,
        struct event_base *base) {
    conn *c = conn_from_freelist();

    if (NULL == c) { 
        if (!(c = (conn *)calloc(1, sizeof(conn)))) {
            fprintf(stderr, "calloc()\n");
            return NULL;
        }    

        c->rbuf = c->wbuf = 0; 
        c->iov = 0; 
        c->msglist = 0; 

        c->rsize = read_buffer_size;
        c->wsize = DATA_BUFFER_SIZE;
        c->iovsize = IOV_LIST_INITIAL;
        c->msgsize = MSG_LIST_INITIAL;

        c->rbuf = (char *)malloc((size_t)c->rsize);
        c->wbuf = (char *)malloc((size_t)c->wsize);
        c->iov = (struct iovec *)malloc(sizeof(struct iovec) * c->iovsize);
        c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);
        

        c->iterator = NULL;
/*
        if (c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0 || 
                c->msglist == 0 || c->suffixlist == 0) { 
            conn_free(c);
            fprintf(stderr, "malloc()\n");
            return NULL;
        }    
*/
        if (c->rbuf == 0 || c->wbuf == 0 || c->iov == 0 || 
            c->msglist == 0 ) { 
            conn_free(c);
            fprintf(stderr, "malloc()\n");
            return NULL;
        }    

        STATS_LOCK();
        stats.conn_structs++;
        STATS_UNLOCK();
    }    

    c->transport = transport;
    c->protocol = settings.binding_protocol;

    /* unix socket mode doesn't need this, so zeroed out.  but why
    * is this done for every command?  presumably for UDP
    * mode.  */
    if (!settings.socketpath) {
        c->request_addr_size = sizeof(c->request_addr);
    } else {
        c->request_addr_size = 0;
    }

    if (settings.verbose > 1) {
        if (init_state == conn_listening) {
            fprintf(stderr, "<%d server listening (%s)\n", sfd,
                    prot_text(c->protocol));
        } else if (IS_UDP(transport)) {
            fprintf(stderr, "<%d server listening (udp)\n", sfd);
        } else if (c->protocol == negotiating_prot) {
            fprintf(stderr, "<%d new auto-negotiating client connection\n",
                    sfd);
        } else if (c->protocol == ascii_prot) {
            fprintf(stderr, "<%d new ascii client connection.\n", sfd);
        } else if (c->protocol == binary_prot) {
            fprintf(stderr, "<%d new binary client connection.\n", sfd);
        } else {
            fprintf(stderr, "<%d new unknown (%d) client connection\n",
                    sfd, c->protocol);
            assert(0);
        }
    }

    c->sfd = sfd;
    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
//    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;

    c->noreply = false;

    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (event_add(&c->event, 0) == -1) {
        if (conn_add_to_freelist(c)) {
            conn_free(c);
        }
        perror("event_add");
        return NULL;
    }

    STATS_LOCK();
    stats.curr_conns++;
    stats.total_conns++;
    STATS_UNLOCK();

    return c;
}

static void conn_cleanup(conn *c) {
    assert(c != NULL);

    if (c->item) {
//        item_remove(c->item);
        c->item = 0; 
    }    

/*
    if (c->ileft != 0) { 
        for (; c->ileft > 0; c->ileft--,c->icurr++) {
            item_remove(*(c->icurr));
        }    
    }    
*/    
/*
    if (c->suffixleft != 0) { 
        for (; c->suffixleft > 0; c->suffixleft--, c->suffixcurr++) {
            cache_free(c->thread->suffix_cache, *(c->suffixcurr));
        }    
    }    
*/
    if (c->write_and_free) {
        free(c->write_and_free);
        c->write_and_free = 0; 
    }    
/*
    if (c->sasl_conn) {
        assert(settings.sasl);
        sasl_dispose(&c->sasl_conn);
        c->sasl_conn = NULL;
    } 
*/       
}

/*
* Frees a connection.
*/
void conn_free(conn *c) {
    if (c) {
        if (c->hdrbuf)
            free(c->hdrbuf);
        if (c->msglist)
            free(c->msglist);
        if (c->rbuf)
            free(c->rbuf);
        if (c->wbuf)
            free(c->wbuf);
//        if (c->ilist)
//            free(c->ilist);
        if (c->suffixlist)
            free(c->suffixlist);
        if (c->iov)
            free(c->iov);
        free(c);
    }
}

static void conn_close(conn *c) {
    assert(c != NULL);

    /* delete the event, the socket and the conn */
    event_del(&c->event);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d connection closed.\n", c->sfd);

    close(c->sfd);
//    accept_new_conns(true);
    conn_cleanup(c);

    /* if the connection has big buffers, just free it */
    if (c->rsize > READ_BUFFER_HIGHWAT || conn_add_to_freelist(c)) {
        conn_free(c);
    }

    STATS_LOCK();
    stats.curr_conns--;
    STATS_UNLOCK();

    return;
}

/*
* Shrinks a connection's buffers if they're too big.  This prevents
* periodic large "get" requests from permanently chewing lots of server
* memory.
*
* This should only be called in between requests since it can wipe output
* buffers!
*/
static void conn_shrink(conn *c) {
    assert(c != NULL);

    if (IS_UDP(c->transport))
        return;

    if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
        char *newbuf;

        if (c->rcurr != c->rbuf)
            memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

        newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);

        if (newbuf) {
            c->rbuf = newbuf;
            c->rsize = DATA_BUFFER_SIZE;
        }
        /* TODO check other branch... */
        c->rcurr = c->rbuf;
    }
/*
    if (c->isize > ITEM_LIST_HIGHWAT) {
        item **newbuf = (item**) realloc((void *)c->ilist, ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
        if (newbuf) {
            c->ilist = newbuf;
            c->isize = ITEM_LIST_INITIAL;
        }
*/        
        /* TODO check error condition? */
//    }

    if (c->msgsize > MSG_LIST_HIGHWAT) {
        struct msghdr *newbuf = (struct msghdr *) realloc((void *)c->msglist, MSG_LIST_INITIAL * sizeof(c->msglist[0]));
        if (newbuf) {
            c->msglist = newbuf;
            c->msgsize = MSG_LIST_INITIAL;
        }
        /* TODO check error condition? */
    }

    if (c->iovsize > IOV_LIST_HIGHWAT) {
        struct iovec *newbuf = (struct iovec *) realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
        if (newbuf) {
            c->iov = newbuf;
            c->iovsize = IOV_LIST_INITIAL;
        }
        /* TODO check return value */
    }
}

/*
* Sets a socket's send buffer size to the maximum allowed by the system.
*/
static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
        if (settings.verbose > 0)
            perror("getsockopt(SO_SNDBUF)");
        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
}


/*
#ifndef HAVE_SIGIGNORE
static int sigignore(int sig) {
    struct sigaction sa = { .sa_handler = SIG_IGN, .sa_flags = 0 };

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
        return -1;
    }
    return 0;
}
#endif
*/

static void server_socket_unix(char* name) {
    int sfd;
    struct sockaddr_un server;

    if((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        goto err;

    server.sun_family = AF_UNIX;
    strcpy(server.sun_path, name);
    unlink(name);

    if (bind(sfd, (struct sockaddr *) &server, sizeof(struct sockaddr_un)))
        goto err;

    if (listen(sfd, settings.backlog) == -1)
        goto err;

    conn *listen_conn_add;

    if (!(listen_conn_add = conn_new(sfd, conn_listening,
                    EV_READ | EV_PERSIST, 1,
                    tcp_transport, main_base))) {
        fprintf(stderr, "failed to create listening connection\n");
        exit(EXIT_FAILURE);
    }

    listen_conn_add->next = listen_conn;
    listen_conn = listen_conn_add;

    fprintf(stderr, "zs_test_engine: Listening on socket: %s\n", name);
    return;

err:
    fprintf(stderr, "error opening unix socket: %s: %s\n", name, strerror(errno));
    exit(1);
}

/**
* Create a socket and bind it to a specific port number
* @param port the port number to bind to
* @param transport the transport protocol (TCP / UDP)
* @param portnumber_file A filepointer to write the port numbers to
*        when they are successfully added to the list of ports we
*        listen on.
*/
static int server_socket(int port, enum network_transport transport,
        FILE *portnumber_file) {
    int sfd; 
    struct linger ling = {0, 0};
    struct addrinfo *ai; 
    struct addrinfo *next;
    struct addrinfo hints = { .ai_flags = AI_PASSIVE,
                              .ai_family = AF_UNSPEC };
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0; 
    int flags = 1;

    hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM;

    if (port == -1) {
        port = 0; 
    }    
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    error= getaddrinfo(settings.inter, port_buf, &hints, &ai);
    if (error != 0) { 
        if (error != EAI_SYSTEM)
            fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
        else 
            perror("getaddrinfo()");
        return 1;
    }    

    for (next= ai; next; next= next->ai_next) {
        conn *listen_conn_add;
        if ((sfd = new_socket(next)) == -1) {
            /* getaddrinfo can return "junk" addresses,
            * we make sure at least one works before erroring.
            */
            continue;
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
                close(sfd);
                continue;
            }
        }
#endif

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        if (IS_UDP(transport)) {
            maximize_sndbuf(sfd);
        } else {
            error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");
        }

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
            if (errno != EADDRINUSE) {
                perror("bind()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            close(sfd);
            continue;
        } else {
            success++;
            if (!IS_UDP(transport) && listen(sfd, settings.backlog) == -1) {
                perror("listen()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            if (portnumber_file != NULL &&
                    (next->ai_addr->sa_family == AF_INET ||
                     next->ai_addr->sa_family == AF_INET6)) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                    if (next->ai_addr->sa_family == AF_INET) {
                        fprintf(portnumber_file, "%s INET: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in.sin_port));
                    } else {
                        fprintf(portnumber_file, "%s INET6: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
        }

        if (IS_UDP(transport)) {
            int c;

            for (c = 0; c < settings.num_threads; c++) {
                /* this is guaranteed to hit all threads because we round-robin */
                dispatch_conn_new(sfd, conn_read, EV_READ | EV_PERSIST,
                        UDP_READ_BUFFER_SIZE, transport);
            }
        } else {
            if (!(listen_conn_add = conn_new(sfd, conn_listening,
                            EV_READ | EV_PERSIST, 1,
                            transport, main_base))) {
                fprintf(stderr, "failed to create listening connection\n");
                exit(EXIT_FAILURE);
            }
            listen_conn_add->next = listen_conn;
            listen_conn = listen_conn_add;
        }
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

int main (int argc, char **argv) {
    char buf[64];

    srand(0);
    
    /* init settings */
    settings_init();

    read_options(argc, argv);

    if(getenv("ZS_PROPERTY_FILE"))
    {
        ZSLoadProperties(getenv("ZS_PROPERTY_FILE"));
        ZSSetProperty("ZS_TEST_MODE", "0");
        unsetenv("ZS_PROPERTY_FILE");
        //free(filename);
    }

    if(settings.zs_reformat >= 0)
    {
        snprintf(buf, sizeof(buf), "%d", settings.zs_reformat ? 1 : 0);
        ZSSetProperty("ZS_REFORMAT", buf);
    }

    if(getenv("ZS_TEST_FRAMEWORK_SHARED"))
    {
        snprintf(buf, sizeof(buf), "%d", settings.zs_admin_port);
        ZSSetProperty("ZS_ADMIN_PORT", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_log_flush_dir);
        ZSSetProperty("ZS_LOG_FLUSH_DIR", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_crash_dir);
        ZSSetProperty("ZS_CRASH_DIR", buf);

        snprintf(buf, sizeof(buf), "%s", settings.shmem_basedir);
        ZSSetProperty("NODE[0].SHMEM.BASEDIR", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_flash_filename);
        ZSSetProperty("ZS_FLASH_FILENAME", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_stats_file);
        ZSSetProperty("ZS_STATS_FILE", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_log_file);
        ZSSetProperty("ZS_LOG_FILE", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_flog_mode);
        ZSSetProperty("ZS_FLOG_MODE", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_flog_nvram_file);
        ZSSetProperty("ZS_FLOG_NVRAM_FILE", buf);

        snprintf(buf, sizeof(buf), "%d", settings.zs_flog_nvram_file_offset);
        ZSSetProperty("ZS_FLOG_NVRAM_FILE_OFFSET", buf);
    }

    if(getenv("ZS_TEST_FRAMEWORK_REMOTE"))
    {
        snprintf(buf, sizeof(buf), "%s", settings.zs_stats_file);
        ZSSetProperty("ZS_STATS_FILE", buf);

        snprintf(buf, sizeof(buf), "%s", settings.zs_log_file);
        ZSSetProperty("ZS_LOG_FILE", buf);
    }

#ifdef USE_ZS_API
    if (ZSInit(&zs_state) != ZS_SUCCESS) {
        fprintf(stderr, "ZS initialization failed!\n" );
        assert(0);
    }

    fprintf(stderr, "ZS was initialized successfully!\n");
#elif defined USE_SDF_API
    if (SDFInit(&sdf_state, 0, NULL) != SDF_SUCCESS) {
        fprintf(stderr, "SDF initialization failed!\n");
        assert(0);
    }

    fprintf(stderr, "SDF was initialized successfully!\n");
#endif
    
    init_random_block();

    /* initialize main thread libevent instance */
    main_base = event_init();

    stats_init();

    conn_init();

    /*   
    * ignore SIGPIPE signals; we can use errno == EPIPE if we
    * need that information
    */
    if (sigignore(SIGPIPE) == -1) {
        perror("failed to ignore SIGPIPE; sigaction");
        assert(0);
    }   

    thread_init(settings.num_threads, main_base);

    /* create the listening socket, bind it, and init */  
    errno = 0;
    if (settings.socketpath)
        server_socket_unix(settings.socketpath); 
    else if (settings.port && server_socket(settings.port, tcp_transport, NULL)) {
        fprintf(stderr, "failed to listen on TCP port %d", settings.port);
        exit(EX_OSERR);
    }

    /* enter the event loop */
    event_base_loop(main_base, 0);
    
    return 0;
}
