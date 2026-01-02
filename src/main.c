#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include <pthread.h>


#define NUM_THREADS (1)

#define MAX_TABLE_OPTIONS (10)
#define TABLE_NAME_CAP (25)

#define STATUS_OK (0)

typedef struct {
    const char *env_name;
    const char **var_dest;
} Env_Map;

typedef struct {
    const char *host;
    unsigned int port;
    const char *username;
    const char *password;
    const char *database;
} Conn_Args;

typedef struct {
    const size_t thread_id;
    const Conn_Args *conn_args;
    const char *query_string;
} Thread_Args;

int ensure_envs(size_t envc, const Env_Map *envm) {
    for (size_t i = 0; i < envc; i++) {
        const char *val = getenv(envm[i].env_name);
        if (!val) {
            fprintf(stderr, "missing env %s\n", envm[i].env_name);
            return 1;
        }
        *envm[i].var_dest = val;
    }
    return 0;
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

void print_query_result(MYSQL_RES *result) {
    size_t num_fields = 0;
    
    MYSQL_FIELD *field;
    while ((field = mysql_fetch_field(result))) {
        printf("%s\t", field->name);
        num_fields += 1;
    }
    printf("\n");
    
    // must fetch row until NULL when using use_result over store_result
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        for (size_t col = 0; col < num_fields; col++) {
            printf("%s\t", row[col]);
        }
        printf("\n");
    }
}

MYSQL *connect(const Conn_Args *conn_args) {
    MYSQL *CONN = mysql_init(NULL);
    if (CONN == NULL) {
        fprintf(stderr, "could not initialize MySQL structure\n");
        return NULL;
    }
    
    printf("MySQL structure successfully initated with address %p.\n", CONN);
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

    printf("Successfully connected to host.\n");
    printf("-------------------------------\n");

    return CONN;
}

void *worker_routine(void *ptr) {
    Thread_Args *thread_args = (Thread_Args *)(ptr);

    MYSQL *CONN = connect(thread_args->conn_args);
    if (CONN == NULL) {
        fprintf(stderr, "[Thread #%zu] Connection failed\n", thread_args->thread_id);
        return NULL;
    }

    MYSQL_RES *result;
    if (mysql_query(CONN, thread_args->query_string)) {
        fprintf(stderr, "[Thread #%zu] Query error: %s\n", thread_args->thread_id, mysql_error(CONN));
        return NULL;
    }

    result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "[Thread #%zu] Result error: %s\n", thread_args->thread_id, mysql_error(CONN));
        return NULL;
    }

    // Process query result
    print_query_result(result);

    mysql_free_result(result);
    printf("[Thread #%zu] Success.\n", thread_args->thread_id);
    return NULL;
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

    char table_name[TABLE_NAME_CAP];
    if (prompt_select_table(CONN, &table_name) != STATUS_OK) {
        printf("Could not fetch table information.\n");
        exit(1);
    }

    mysql_close(CONN);
    printf("-------------------------------\n");
    printf("MySQL connection closed.\n");
    
    const char *query_template = "SELECT * FROM %s LIMIT 20";
    
    char *query = malloc(strlen(query_template) - 2 + strlen(table_name) + 1);
    sprintf(query, query_template, table_name);
    printf("Query is: %s\n", query);

    Thread_Args thread_args = {
        0,
        &conn_args,
        query
    };

    worker_routine((void *)(&thread_args));
    
    mysql_library_end();
    free(query);
    printf("-------------------------------\n");
    printf("MySQL client library successfully closed.\n");
    exit(0);
}