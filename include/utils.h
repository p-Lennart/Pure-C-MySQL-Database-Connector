#ifndef UTIL_H
#define UTIL_H

typedef struct {
    const char *env_name;
    const char **var_dest;
} Env_Map;

typedef struct {
    long double sum;  
    size_t n;
} Data_Aggregate;

int ensure_envs(size_t envc, const Env_Map *envm);

void update_da(Data_Aggregate *da, long double update);

void merge_da(Data_Aggregate *da1, Data_Aggregate *da2);

long double average_da(Data_Aggregate *da);

double get_time_sec();

#endif
