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
 *  Thread management for test framework.
 *
 *  Copyright 2012 Sandisk, Inc.  All rights reserved.
 *
 *  Authors:
 *      yiwen sun <yiwensun@hengtiansoft.com>
 *
 */
#include "zs_test_engine.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>

#define ITEMS_PER_ALLOC 64

/* An item in the connection queue. */
typedef struct conn_queue_item CQ_ITEM;

struct conn_queue_item {
    int               sfd; 
    enum conn_states  init_state;
    int               event_flags;
    int               read_buffer_size;
    enum network_transport     transport;
    CQ_ITEM          *next;
};

/* A connection queue. */
typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
};

/* Connection lock around accepting new connections */
pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Number of worker threads that have finished setting themselves up.
  */
static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;

/* Lock for global stats */
static pthread_mutex_t stats_lock;

/* Free list of CQ_ITEM structs */
static CQ_ITEM *cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;
static LIBEVENT_DISPATCHER_THREAD dispatcher_thread;
static LIBEVENT_THREAD *threads;

#ifdef USE_ZS_API
extern struct ZS_state *zs_state;
#elif defined USE_SDF_API
extern struct SDF_state *sdf_state;
#endif
#ifdef USE_ZS_API
    extern __thread struct ZS_thread_state *zs_thd_state;
#elif defined USE_SDF_API
    extern __thread struct SDF_thread_state *sdf_thd_state;
#endif

static void *worker_libevent(void *arg);
static void thread_libevent_process(int fd, short which, void *arg);

void STATS_LOCK() {
    pthread_mutex_lock(&stats_lock);
}

void STATS_UNLOCK() {
    pthread_mutex_unlock(&stats_lock);
}

/*
* Initializes a connection queue.
*/
static void cq_init(CQ *cq) {
    pthread_mutex_init(&cq->lock, NULL);
    pthread_cond_init(&cq->cond, NULL);
    cq->head = NULL;
    cq->tail = NULL;
}

/*
* Looks for an item on a connection queue, but doesn't block if there isn't
* one.
* Returns the item, or NULL if no item is available
*/
static CQ_ITEM *cq_pop(CQ *cq) {
    CQ_ITEM *item;

    pthread_mutex_lock(&cq->lock);
    item = cq->head;
    if (NULL != item) {
        cq->head = item->next;
        if (NULL == cq->head)
            cq->tail = NULL;
    }
    pthread_mutex_unlock(&cq->lock);

    return item;
}

/*
* Adds an item to a connection queue.
*/
static void cq_push(CQ *cq, CQ_ITEM *item) {
    item->next = NULL;

    pthread_mutex_lock(&cq->lock);
    if (NULL == cq->tail)
        cq->head = item;
    else
        cq->tail->next = item;
    cq->tail = item;
    pthread_cond_signal(&cq->cond);
    pthread_mutex_unlock(&cq->lock);
}

/*
* Returns a fresh connection queue item.
*/
static CQ_ITEM *cqi_new(void) {
    CQ_ITEM *item = NULL;
    pthread_mutex_lock(&cqi_freelist_lock);
    if (cqi_freelist) {
        item = cqi_freelist;
        cqi_freelist = item->next;
    }
    pthread_mutex_unlock(&cqi_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC);
        if (NULL == item)
            return NULL;

        /*
        * Link together all the new items except the first one
        * (which we'll return to the caller) for placement on
        * the freelist.
        */
        for (i = 2; i < ITEMS_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&cqi_freelist_lock);
        item[ITEMS_PER_ALLOC - 1].next = cqi_freelist;
        cqi_freelist = &item[1];
        pthread_mutex_unlock(&cqi_freelist_lock);
    }

    return item;
}


/*
* Frees a connection queue item (adds it to the freelist.)
*/
static void cqi_free(CQ_ITEM *item) {
    pthread_mutex_lock(&cqi_freelist_lock);
    item->next = cqi_freelist;
    cqi_freelist = item;
    pthread_mutex_unlock(&cqi_freelist_lock);
}

/*
* Sets whether or not we accept new connections.
*/
void accept_new_conns(const bool do_accept) {
    pthread_mutex_lock(&conn_lock);
    do_accept_new_conns(do_accept);
    pthread_mutex_unlock(&conn_lock);
}

/*
* Creates a worker thread.
*/
static void create_worker(void *(*func)(void *), void *arg) {
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    pthread_attr_init(&attr);

    if ((ret = pthread_create(&thread, &attr, func, arg)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n",
                strerror(ret));
        exit(1);
    }
}

/* Which thread we assigned a connection to most recently. */
static int last_thread = -1; 

/*
* Dispatches a new connection to another thread. This is only ever called
* from the main thread, either during initialization (for UDP) or because
* of an incoming connection.
*/
void dispatch_conn_new(int sfd, enum conn_states init_state, int event_flags,
        int read_buffer_size, enum network_transport transport) {
    CQ_ITEM *item = cqi_new();
    int tid = (last_thread + 1) % settings.num_threads;

    LIBEVENT_THREAD *thread = threads + tid;

    last_thread = tid;

    item->sfd = sfd;
    item->init_state = init_state;
    item->event_flags = event_flags;
    item->read_buffer_size = read_buffer_size;
    item->transport = transport;

    cq_push(thread->new_conn_queue, item);

    if (write(thread->notify_send_fd, "", 1) != 1) {
        perror("Writing to thread notify pipe");
    }
}

/****************************** LIBEVENT THREADS *****************************/

/*
* Set up a thread's information.
*/
static void setup_thread(LIBEVENT_THREAD *me) {
    me->base = event_init();
    if (! me->base) {
        fprintf(stderr, "Can't allocate event base\n");
        exit(1);
    }       

    /* Listen for notifications from other threads */
    event_set(&me->notify_event, me->notify_receive_fd,
            EV_READ | EV_PERSIST, thread_libevent_process, me);
    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }       

    me->new_conn_queue = malloc(sizeof(struct conn_queue));
    if (me->new_conn_queue == NULL) {
        perror("Failed to allocate memory for connection queue");
        exit(EXIT_FAILURE);
    }       
    cq_init(me->new_conn_queue);

    if (pthread_mutex_init(&me->stats.mutex, NULL) != 0) {
        perror("Failed to initialize mutex");
        exit(EXIT_FAILURE);
    }       

//    me->suffix_cache = cache_create("suffix", SUFFIX_SIZE, sizeof(char*),
//            NULL, NULL);
//    if (me->suffix_cache == NULL) {     
//        fprintf(stderr, "Failed to create suffix cache\n");
//        exit(EXIT_FAILURE); 
//    }       
}

/*
* Processes an incoming "handle a new connection" item. This is called when
* input arrives on the libevent wakeup pipe.
*/
static void thread_libevent_process(int fd, short which, void *arg) {
    LIBEVENT_THREAD *me = arg;
    CQ_ITEM *item;
    char buf[1];

    if (read(fd, buf, 1) != 1)
        if (settings.verbose > 0)
            fprintf(stderr, "Can't read from libevent pipe\n");

    item = cq_pop(me->new_conn_queue);

    if (NULL != item) {
        conn *c = conn_new(item->sfd, item->init_state, item->event_flags,
                item->read_buffer_size, item->transport, me->base);
        if (c == NULL) {
            if (IS_UDP(item->transport)) {
                fprintf(stderr, "Can't listen for events on UDP socket\n");
                exit(1);
            } else {
                if (settings.verbose > 0) {
                    fprintf(stderr, "Can't listen for events on fd %d\n",
                            item->sfd);
                }
                close(item->sfd);
            }
        } else {
            c->thread = me;
        }
        cqi_free(item);
    }
}

/*
* Initializes the thread subsystem, creating various worker threads.
*
* nthreads  Number of worker event handler threads to spawn
* main_base Event base for main thread
*/
void thread_init(int nthreads, struct event_base *main_base) {
    int         i;
//    int         power;

    threads = calloc(nthreads, sizeof(LIBEVENT_THREAD));
    if (! threads) {
        perror("Can't allocate thread descriptors");
        exit(1);
    }

    dispatcher_thread.base = main_base;
    dispatcher_thread.thread_id = pthread_self();

    for (i = 0; i < nthreads; i++) {
        int fds[2];
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }

        threads[i].notify_receive_fd = fds[0];
        threads[i].notify_send_fd = fds[1];

        setup_thread(&threads[i]);
    }

    /* Create threads after we've done all the libevent setup. */
    for (i = 0; i < nthreads; i++) {
        create_worker(worker_libevent, &threads[i]);
    }

    /* Wait for all the threads to set themselves up before returning. */
    pthread_mutex_lock(&init_lock);
    while (init_count < nthreads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }
    pthread_mutex_unlock(&init_lock);

}


/*
* Worker thread: main event loop
*/
static void *worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = arg;

    /* Any per-thread setup can happen here; thread_init() will block until
    * all threads have finished initializing.
    */

    // FIXME, sdf_thd_state should better init in thread layout
#ifdef USE_ZS_API
    if (ZS_SUCCESS != ZSInitPerThreadState(zs_state, &zs_thd_state)) {
        fprintf( stderr, "ZS thread initialization failed!\n" );
        assert( 0 );
    }
#elif defined USE_SDF_API
    sdf_thd_state = SDFInitPerThreadState(sdf_state);
#endif

    pthread_mutex_lock(&init_lock);
    init_count++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);

    event_base_loop(me->base, 0);
    return NULL;
}

