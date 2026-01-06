#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "../include/status_codes.h"
#include "../include/utils.h"

int ensure_envs(size_t envc, const Env_Map *envm) {
    for (size_t i = 0; i < envc; i++) {
        const char *val = getenv(envm[i].env_name);
        if (!val) {
            fprintf(stderr, "missing env %s\n", envm[i].env_name);
            return 1;
        }
        *envm[i].var_dest = val;
    }
    return STATUS_OK;
}

void update_da(Data_Aggregate *da, long double update) {
    da->sum += update;
    da->n += 1;
}

void merge_da(Data_Aggregate *da1, Data_Aggregate *da2) {
    da1->sum += da2->sum;
    da1->n += da2->n;
}

long double average_da(Data_Aggregate *da) {
    if (da->n == 0) {
        return 0.0;
    }
    return da->sum / da->n;
}

double get_time_sec() {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime failed");
        exit(EXIT_FAILURE);
    }
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

