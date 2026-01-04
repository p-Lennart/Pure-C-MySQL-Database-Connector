#ifndef UTIL_H
#define UTIL_H

#define STATUS_OK (0)

typedef struct {
    const char *env_name;
    const char **var_dest;
} Env_Map;

int ensure_envs(size_t envc, const Env_Map *envm);

#endif
