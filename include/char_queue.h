#include <stdbool.h>

#ifndef CHAR_QUEUE_H
#define CHAR_QUEUE_H

typedef struct {
    char **data;
    size_t front;
    size_t back;
    size_t size;
    size_t capacity;
    size_t len_strs;
    
    bool closed;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    
} Char_Queue;

Char_Queue init_queue(size_t capacity, size_t len_strs);

bool free_queue(Char_Queue *cq);

void close_queue(Char_Queue *cq);

void measure_queue(Char_Queue *cq);

char *peek_front(Char_Queue *cq);

char *deque_front(Char_Queue *cq);

bool push_back(Char_Queue *cq, const char *source);

#endif