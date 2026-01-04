#include <stdio.h>
#include <stdlib.h>

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