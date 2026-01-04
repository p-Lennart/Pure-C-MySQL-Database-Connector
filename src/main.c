#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <mysql.h>
#include <pthread.h>

#include "../include/utils.h"
#include "../include/char_queue.h"

#define NUM_THREADS (4)

#define DATA_FIELD ("price")

#define MAX_TABLE_OPTIONS (10)
#define TABLE_NAME_CAP (25)
// #define ROW_LIMIT (100)

#define STATUS_OK (0)

typedef struct {
    const char *host;
    unsigned int port;
    const char *username;
    const char *password;
    const char *database;
} Conn_Args;

typedef struct {
    long double sum;  
    unsigned long n;
} Data_Aggregate;

typedef struct {
    size_t thread_id;
    Conn_Args *conn_args;
    char *query_string;
    _Atomic int *fail_flag;
    Data_Aggregate *result;
} Thread_Args;

void update_da(Data_Aggregate *da, long double update) {
    da->sum += update;
    da->n += 1;
}

void merge_da(Data_Aggregate *da1, Data_Aggregate *da2) {
    da1->sum += da2->sum;
    da1->n += da2->n;
}

long double average(Data_Aggregate *da) {
    if (da->n == 0) {
        return 0.0;
    }
    return da->sum / da->n;
}

int prompt_select_table(MYSQL *CONN, char (*table_name)[TABLE_NAME_CAP]) {
    MYSQL_RES *result;
    MYSQL_ROW row;

    if (mysql_query(CONN, "SHOW TABLES;")) {
        fprintf(stderr, "Query error: %s\n", mysql_error(CONN));
        return 1;
    }

    result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "Result error: %s\n", mysql_error(CONN));
        return 1;
    }

    char table_names[MAX_TABLE_OPTIONS][TABLE_NAME_CAP];
    size_t table_count = 0;

    printf("Select Table:\n");
    while ((row = mysql_fetch_row(result)) && table_count < MAX_TABLE_OPTIONS) {
        char *tname = row[0];
        printf("[%zu] %s\n", table_count, tname);
        
        strncpy(table_names[table_count], tname, TABLE_NAME_CAP - 1);
        table_names[table_count][TABLE_NAME_CAP - 1] = '\0';

        table_count += 1;
    }

    char buffer[32];
    fgets(buffer, sizeof(buffer), stdin);
    size_t sel_table = strtoul(buffer, NULL, 10);

    if (sel_table >= table_count) {
        printf("Did not select a table ∈ [0, %zu).\nDefaulting to [0] %s\n", table_count, table_names[0]);
        sel_table = 0;
    } else {
        printf("Selected [%zu] %s\n", sel_table, table_names[sel_table]);
    }
    
    strncpy(*table_name, table_names[sel_table], TABLE_NAME_CAP - 1);
    (*table_name)[TABLE_NAME_CAP - 1] = '\0';

    mysql_free_result(result);
    return STATUS_OK;
}

MYSQL *connect(const Conn_Args *conn_args) {
    MYSQL *CONN = mysql_init(NULL);
    if (CONN == NULL) {
        fprintf(stderr, "could not initialize MySQL structure\n");
        return NULL;
    }
    // printf("MySQL structure successfully initated with address %p.\n", CONN);
    
    // SingleStore cloud specific options
    const char *tls_version = "TLSv1.2";
    
    mysql_options(CONN, MYSQL_OPT_TLS_VERSION, tls_version);
    mysql_options(CONN, MYSQL_DEFAULT_AUTH, "mysql_native_password");
    
    const char *unix_socket = NULL;
    unsigned long client_flag = 0;

    if (!mysql_real_connect(
        CONN,
        conn_args->host,
        conn_args->username,
        conn_args->password,
        conn_args->database,
        conn_args->port,
        unix_socket,
        client_flag
    )) {
        fprintf(stderr, "Failed to connect, with error:\n%s\n", mysql_error(CONN));
        return NULL;
    }

    return CONN;
}

Data_Aggregate process_result(MYSQL_RES *result, Thread_Args *thread_args) {
    size_t num_fields = 0;
    size_t idx_target = -1;

    Data_Aggregate res_data = { 0.0, 0 };
    
    MYSQL_FIELD *field;
    while ((field = mysql_fetch_field(result))) {
        // printf("%s\t", field->name);
        
        if (strncmp(field->name, DATA_FIELD, strlen(DATA_FIELD)) == 0) {
            idx_target = num_fields;
        }
        num_fields += 1;
    }
    // printf("\n");

    if (idx_target == -1) {
        fprintf(stderr, "[Thread #%zu] Could not locate target data field '%s' in result\n",
            thread_args->thread_id, DATA_FIELD);
        atomic_store(thread_args->fail_flag, 1);
    }

    if (atomic_load(thread_args->fail_flag) != STATUS_OK) {
        fprintf(stderr, "[Thread #%zu] Exiting according to fail flag\n", thread_args->thread_id);
        return res_data;
    }

    // must fetch row until NULL when using use_result over store_result
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        if (atomic_load(thread_args->fail_flag) != STATUS_OK) {
            fprintf(stderr, "[Thread #%zu] Exiting according to fail flag\n", thread_args->thread_id);
            break;
        }

        // for (size_t col = 0; col < num_fields; col++) {
        //     printf("%s\t", row[col]);
        // }
        // printf("\n");

        long double row_data = strtod(row[idx_target], NULL);
        update_da(&res_data, row_data);
    }

    return res_data;
}

void *worker_routine(void *ptr) {
    Thread_Args *thread_args = (Thread_Args *)(ptr);

    MYSQL *CONN = connect(thread_args->conn_args);
    if (CONN == NULL) {
        fprintf(stderr, "[Thread #%zu] Connection failed\n", thread_args->thread_id);
        atomic_store(thread_args->fail_flag, 1);
        return NULL;
    }

    MYSQL_RES *result;
    if (mysql_query(CONN, thread_args->query_string)) {
        fprintf(stderr, "[Thread #%zu] Query error: %s\n", thread_args->thread_id, mysql_error(CONN));
        atomic_store(thread_args->fail_flag, 1);
        return NULL;
    }

    result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "[Thread #%zu] Result error: %s\n", thread_args->thread_id, mysql_error(CONN));
        atomic_store(thread_args->fail_flag, 1);
        return NULL;
    }

    // Process query result
    *(thread_args->result) = process_result(result, thread_args);

    mysql_free_result(result);

    
    printf("[Thread #%zu] n=%lu, avg=%Lf\n", 
        thread_args->thread_id, (thread_args->result)->n, average(thread_args->result));
    return NULL;
}

char *build_thread_query(size_t thread_id, char *table_name) {
    const char *query_template = 
        "SELECT date, %s FROM %s WHERE MOD(DAY(date), %d)=%zu";

    int query_len = snprintf(NULL, 0, query_template, DATA_FIELD, table_name, NUM_THREADS, thread_id);
    if (query_len < 0) {
        fprintf(stderr, "Failed length calculation for query!\n");
        return "";
    }

    char *query = calloc(query_len + 1, sizeof(char));
    if (!query) {
        fprintf(stderr, "Failed malloc for query!\n");
        return "";
    }

    snprintf(query, query_len + 1, query_template, DATA_FIELD, table_name, NUM_THREADS, thread_id);
    return query;
}

int main(int argc, char *argv[]) {
    const char *port_str;
    Conn_Args conn_args = {};

    Env_Map envm[] = { 
        { "SS_host", &(conn_args.host) },
        { "SS_port", &port_str },
        { "SS_user", &(conn_args.username) },
        { "SS_pass", &(conn_args.password) },
        { "SS_db", &(conn_args.database) }
    };

    if (ensure_envs(sizeof(envm) / sizeof(envm[0]), envm) != 0) {
        fprintf(stderr, "Env variable misconfiguration.\n");
        exit(1);
    }

    conn_args.port = atoi(port_str);
    // if 0, mysql does default port handling, no exit

    printf("All env variables successfully loaded.\n- Host: %s\n- Port: %d\n- User: %s\n- Password: %s\n- Database: %s\n",
        conn_args.host, conn_args.port, conn_args.username, conn_args.password, conn_args.database);

    if (mysql_library_init(0, NULL, NULL) != 0) {
        fprintf(stderr, "could not initialize MySQL client library\n");
        exit(1);
    }

    MYSQL *CONN = connect(&conn_args);
    if (CONN == NULL) {
        fprintf(stderr, "Connection failed\n");
        exit(1);
    }
    printf("Successfully connected to host.\n");
    printf("-------------------------------\n");

    char table_name[TABLE_NAME_CAP];
    if (prompt_select_table(CONN, &table_name) != STATUS_OK) {
        printf("Could not fetch table information.\n");
        exit(1);
    }

    mysql_close(CONN);
    
    printf("-------------------------------\n");
    pthread_t threads[NUM_THREADS];
    Thread_Args thread_args[NUM_THREADS];
    _Atomic int fail_flag = STATUS_OK;
    Data_Aggregate results[NUM_THREADS];

    for (size_t i = 0; i < NUM_THREADS; i++) {
        thread_args[i].thread_id = i;
        thread_args[i].conn_args = &conn_args;
        thread_args[i].fail_flag = &fail_flag;
        thread_args[i].result = &results[i];
        // MALLOCed, MUST FREE LATER
        thread_args[i].query_string = build_thread_query(i, table_name);
        
        pthread_create(&threads[i], NULL, worker_routine, (void *)(&thread_args[i]));
        printf("[Thread #%zu] %s\n", i, thread_args[i].query_string);
    }
    printf("-------------------------------\n");

    Data_Aggregate final_result = {0, 0};

    for (size_t i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        free(thread_args[i].query_string);
        if (atomic_load(&fail_flag) == STATUS_OK) {
            merge_da(&final_result, &results[i]);
        }
    }

    printf("-------------------------------\n");
    mysql_library_end();
    printf("MySQL client library successfully closed.\n");
    
    if (atomic_load(&fail_flag) == STATUS_OK) {
        printf("Query sequence executed as intended.\n");
        printf("Final result: %Lf\n", average(&final_result));
        exit(0);
    } else {
        printf("Query sequence did not execute as intended.\n");
        exit(1);
    }
}