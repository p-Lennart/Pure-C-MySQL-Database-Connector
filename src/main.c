#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <math.h>
#include <mysql.h>
#include <pthread.h>
    
#include "../include/utils.h"
#include "../include/char_queue.h"

#define NUM_PRODUCERS (1)
#define NUM_CONSUMERS (4)

#define QUEUE_SIZE (100)
#define MAX_DATA_LEN (25)
#define ROWS_PER_STATUS_CHECK (1000)

#define DATA_FIELD ("price")

#define MAX_TABLE_OPTIONS (10)
#define TABLE_NAME_CAP (25)

#define STATUS_ERROR (1)
#define STATUS_OK (0)
#define STATUS_FINISHED (-1)

typedef struct {
    const char *host;
    unsigned int port;
    const char *username;
    const char *password;
    const char *database;
} Conn_Args;

typedef struct {
    long double sum;  
    size_t n;
} Data_Aggregate;

typedef struct {
    size_t thread_id;
    _Atomic int *status;
    Char_Queue *queue;
    Conn_Args *conn_args;
    char *query_string;
} Producer_Args;

typedef struct {
    size_t thread_id;
    _Atomic int *status;
    Char_Queue *queue;
    Data_Aggregate *result;
} Consumer_Args;

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
        return STATUS_ERROR;
    }

    result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "Result error: %s\n", mysql_error(CONN));
        return STATUS_ERROR;
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

size_t producer_process_result(MYSQL_RES *result, Producer_Args *producer_args) {
    size_t num_fields = 0;
    size_t num_rows = 0;
    size_t idx_target = -1;

    MYSQL_FIELD *field;
    while ((field = mysql_fetch_field(result))) {
        if (strncmp(field->name, DATA_FIELD, strlen(DATA_FIELD)) == 0) {
            idx_target = num_fields;
        }
        num_fields += 1;
    }

    if (idx_target == -1) {
        fprintf(stderr, "[Producer #%zu] Could not locate target data field '%s' in result\n",
            producer_args->thread_id, DATA_FIELD);
        atomic_store(producer_args->status, STATUS_ERROR);
        return STATUS_ERROR;
    }

    if (atomic_load(producer_args->status) == STATUS_ERROR) {
        fprintf(stderr, "[Producer #%zu] Exiting according to fail flag\n", producer_args->thread_id);
        return STATUS_ERROR;
    }

    // must fetch row until NULL when using use_result over store_result
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        if (num_rows % ROWS_PER_STATUS_CHECK == 0 
            && atomic_load(producer_args->status) == STATUS_ERROR) { // check every ___ rows
            fprintf(stderr, "[Producer #%zu] Exiting according to fail flag\n", producer_args->thread_id);
            break;
        }

        push_back(producer_args->queue, row[idx_target]);
        num_rows += 1;
    }

    return num_rows;
}

void *producer_routine(void *ptr) {
    Producer_Args *thread_args = (Producer_Args *)(ptr);

    MYSQL *CONN = connect(thread_args->conn_args);
    if (CONN == NULL) {
        fprintf(stderr, "[Producer #%zu] Connection failed\n", thread_args->thread_id);
        atomic_store(thread_args->status, STATUS_ERROR);
        return NULL;
    }

    MYSQL_RES *result;
    if (mysql_query(CONN, thread_args->query_string)) {
        fprintf(stderr, "[Producer #%zu] Query error: %s\n", thread_args->thread_id, mysql_error(CONN));
        atomic_store(thread_args->status, STATUS_ERROR);
        return NULL;
    }

    result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "[Producer #%zu] Result error: %s\n", thread_args->thread_id, mysql_error(CONN));
        atomic_store(thread_args->status, STATUS_ERROR);
        return NULL;
    }

    // Process query result
    size_t rows_read = producer_process_result(result, thread_args);
    printf("[Producer #%zu] n=%zu\n", thread_args->thread_id, rows_read);

    mysql_free_result(result);
    return NULL;
}

void *consumer_routine(void *ptr) {
    Consumer_Args *consumer_args = (Consumer_Args *)(ptr);
    Data_Aggregate result = { 0, 0 };

    char *data = deque_front(consumer_args->queue);
    while (data) {
        if (result.n % ROWS_PER_STATUS_CHECK == 0 
            && atomic_load(consumer_args->status) == STATUS_ERROR) { // check every ___ rows
            fprintf(stderr, "[Consumer #%zu] Exiting according to fail flag\n", consumer_args->thread_id);
            break;
        }
        
        long double row_data = strtod(data, NULL);
        update_da(&result, row_data);
        data = deque_front(consumer_args->queue);
    }

    printf("[Consumer #%zu] n=%zu\n", consumer_args->thread_id, result.n);
    *(consumer_args->result) = result;

    return NULL;
}

char *build_thread_query(size_t thread_id, char *table_name) {
    const char *query_template = 
        "SELECT date, %s FROM %s WHERE MOD(DAY(date), %d)=%zu";

    int query_len = snprintf(NULL, 0, query_template, DATA_FIELD, table_name, NUM_PRODUCERS, thread_id);
    if (query_len < 0) {
        fprintf(stderr, "Failed length calculation for query!\n");
        return "";
    }

    char *query = calloc(query_len + 1, sizeof(char));
    if (!query) {
        fprintf(stderr, "Failed malloc for query!\n");
        return "";
    }

    snprintf(query, query_len + 1, query_template, DATA_FIELD, table_name, NUM_PRODUCERS, thread_id);
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
    
    Char_Queue data_queue = init_queue(QUEUE_SIZE, MAX_DATA_LEN);
    _Atomic int global_status = STATUS_OK;
    
    pthread_t producer_threads[NUM_PRODUCERS];
    Producer_Args producer_args[NUM_PRODUCERS];
    
    pthread_t consumer_threads[NUM_CONSUMERS];
    Consumer_Args consumer_args[NUM_CONSUMERS];
    Data_Aggregate consumer_results[NUM_CONSUMERS];

    Data_Aggregate final_result = {0, 0};
    
    for (size_t i = 0; i < NUM_PRODUCERS; i++) {
        producer_args[i].thread_id = i;
        producer_args[i].status = &global_status;
        producer_args[i].queue = &data_queue;
        producer_args[i].conn_args = &conn_args;
        producer_args[i].query_string = build_thread_query(i, table_name); // MALLOCed, MUST FREE LATER
        
        pthread_create(&producer_threads[i], NULL, producer_routine, (void *)(&producer_args[i]));
        printf("[Producer #%zu] %s\n", i, producer_args[i].query_string);
    }

    for (size_t i = 0; i < NUM_CONSUMERS; i++) {
        consumer_args[i].thread_id = i;
        consumer_args[i].status = &global_status;
        consumer_args[i].queue = &data_queue;
        consumer_args[i].result = &consumer_results[i];
        pthread_create(&consumer_threads[i], NULL, consumer_routine, (void *)(&consumer_args[i]));
        printf("[Consumer #%zu] ", i);
    }
    printf("\n");

    printf("-------------------------------\n");

    for (size_t i = 0; i < NUM_PRODUCERS; i++) {
        pthread_join(producer_threads[i], NULL);
        free(producer_args[i].query_string);
    }
    
    if (atomic_load(&global_status) == STATUS_OK) {
        atomic_store(&global_status, STATUS_FINISHED);
    }
    close_queue(&data_queue);

   for (size_t i = 0; i < NUM_CONSUMERS; i++) {
        pthread_join(consumer_threads[i], NULL);
        merge_da(&final_result, &consumer_results[i]);
    }

    free_queue(&data_queue);

    printf("-------------------------------\n");
    
    mysql_library_end();
    printf("MySQL client library successfully closed.\n");
    
    if (atomic_load(&global_status) != STATUS_ERROR) {
        printf("Query sequence executed as intended.\n");
        printf("Final result: %Lf\n", average(&final_result));
        printf("(%Lf/%zu)", final_result.sum, final_result.n);
        exit(0);
    } else {
        printf("Query sequence did not execute as intended.\n");
        exit(1);
    }
}