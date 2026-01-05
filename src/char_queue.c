#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <pthread.h>

#include "../include/char_queue.h"

Char_Queue init_queue(size_t capacity, size_t len_strs) {
    Char_Queue cq = {
        .data = NULL,
        .front = 0,
        .back = 0,
        .size = 0,
        .capacity = capacity,
        .len_strs = len_strs,
        .closed = true
    };

    if (pthread_mutex_init(&cq.lock, NULL) != 0 ||
        pthread_cond_init(&cq.not_empty, NULL) != 0 ||
        pthread_cond_init(&cq.not_full, NULL) != 0) {
        fprintf(stderr, "Failed to initialize mutex/cond vars\n");
        return cq;
    }

    cq.data = calloc(capacity, sizeof(char *));
    size_t entry_size = len_strs + 1;
    
    char *data_block = calloc(capacity, entry_size * sizeof(char));
    if (!cq.data || !data_block) {
        fprintf(stderr, "Could not allocate Char_Queue data!\n");   
        cq.data = NULL;
        pthread_mutex_destroy(&cq.lock);
        pthread_cond_destroy(&cq.not_empty);
        pthread_cond_destroy(&cq.not_full);
        return cq;
    }

    for (size_t i = 0; i < capacity; i++) {
        cq.data[i] = data_block + (i * entry_size);
    }

    cq.closed = false;
    return cq;
}

bool free_queue(Char_Queue *cq) {
    if (!cq || !cq->data) return false;

    pthread_mutex_destroy(&cq->lock);
    pthread_cond_destroy(&cq->not_empty);
    pthread_cond_destroy(&cq->not_full);

    if (cq->data) {
        free(cq->data[0]); // start of data_block
        free(cq->data);
    }
    cq->data = NULL;

    cq->front = 0;
    cq->back = 0;
    cq->size = 0;
    
    return true;
}

void close_queue(Char_Queue *cq) {
    pthread_mutex_lock(&cq->lock);

    cq->closed = true;

    pthread_cond_broadcast(&cq->not_empty);
    pthread_cond_broadcast(&cq->not_full);

    pthread_mutex_unlock(&cq->lock);
}

void measure_queue(Char_Queue *cq) {
    if (!cq || !cq->data) {
        printf("Queue is null!\n");
        return;
    }
    pthread_mutex_lock(&cq->lock);
    printf("Queue: %zu/%zu", cq->size, cq->capacity);
    pthread_mutex_unlock(&cq->lock);
}

static inline bool queue_empty_unlocked(Char_Queue *cq) {
    return cq->size == 0;
}

static inline bool queue_full_unlocked(Char_Queue *cq) {
    return cq->size == cq->capacity;
}

char *peek_front(Char_Queue *cq) {
    char *result = NULL;
    if (!cq || !cq->data) return result;
    pthread_mutex_lock(&cq->lock);

    if (!queue_empty_unlocked(cq)) {
        result = cq->data[cq->front];
    }

    pthread_mutex_unlock(&cq->lock);
    return result;
}

char *deque_front(Char_Queue *cq) {
    if (!cq || !cq->data) return NULL;
    pthread_mutex_lock(&cq->lock);
    
    while (queue_empty_unlocked(cq) && !cq->closed) {
        pthread_cond_wait(&cq->not_empty, &cq->lock);
    }

    if (queue_empty_unlocked(cq)) { // empty and closed
        pthread_mutex_unlock(&cq->lock);
        return NULL;
    } 

    char *removed = cq->data[cq->front];
    cq->front = (cq->front + 1) % cq->capacity;
    cq->size -= 1;
    
    pthread_cond_signal(&cq->not_full);
    pthread_mutex_unlock(&cq->lock);
    return removed;
}

bool push_back(Char_Queue *cq, const char *source) {
    if (!cq || !cq->data) return false;
    pthread_mutex_lock(&cq->lock);
    
    while (queue_full_unlocked(cq)) {
        pthread_cond_wait(&cq->not_full, &cq->lock);
    }

    if (cq->closed) {
        pthread_mutex_unlock(&cq->lock);
        return false;
    }

    strncpy(cq->data[cq->back], source, cq->len_strs);
    cq->data[cq->back][cq->len_strs] = '\0';

    cq->back = (cq->back + 1) % cq->capacity;
    cq->size += 1;
    
    pthread_cond_signal(&cq->not_empty);
    pthread_mutex_unlock(&cq->lock);
    return true;
}