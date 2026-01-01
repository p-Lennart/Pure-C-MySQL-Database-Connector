#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>

#define MAX_TABLE_OPTIONS (10)
#define TABLE_NAME_CAP (25)

typedef struct {
    const char *env_name;
    const char **var_dest;
} EnvMap;

int ensure_envs(size_t envc, const EnvMap *envm) {
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
    return 0;
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

int query_singlestore(MYSQL *CONN) {
    int status = 0;
    
    MYSQL_RES *result;
    
    char table_name[TABLE_NAME_CAP];
    if (prompt_select_table(CONN, &table_name) != 0) {
        printf("Could not fetch table information.\n");
        status = 1;
        goto cleanup;
    }

    const char *query_template = "SELECT COUNT(*) AS total_rows";
    // const char *query_template = "SELECT * FROM %s LIMIT 20";
    
    char *query = malloc(strlen(query_template) - 2 + strlen(table_name) + 1);
    sprintf(query, query_template, table_name);

    printf("Query is: %s\n", query);
    if (mysql_query(CONN, query)) {
        fprintf(stderr, "Query error: %s\n", mysql_error(CONN));
        status = 1;
        goto cleanup;
    }

    result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "Result error: %s\n", mysql_error(CONN));
        status = 1;
        goto cleanup;
    }

    // Process query result
    print_query_result(result);
    
    mysql_free_result(result);

cleanup:
    free(query);
    query = NULL;
    return status;
}

int main(int argc, char *argv[]) {
    MYSQL *CONN;

    // Params
    const char *host;
    const char *port_str;
    const char *username;
    const char *password;
    const char *database;

    EnvMap envm[] = { 
        { "SS_host", &host },
        { "SS_port", &port_str },
        { "SS_user", &username },
        { "SS_pass", &password },
        { "SS_db", &database },
    };

    if (ensure_envs(sizeof envm / sizeof envm[0], envm) != 0) {
        fprintf(stderr, "Env variable misconfiguration.\n");
        exit(1);
    }
    
    unsigned int port = atoi(port_str);
    // if 0, mysql does default port handling, no exit

    if (mysql_library_init(0, NULL, NULL) != 0) {
        fprintf(stderr, "could not initialize MySQL client library\n");
        exit(1);
    }
    
    printf("All env variables successfully loaded.\n- Host: %s\n- Port: %d\n- User: %s\n- Password: %s\n- Database: %s\n",
        host, port, username, password, database);

    // Conn
    CONN = mysql_init(NULL);
    if (CONN == NULL) {
        fprintf(stderr, "could not initialize MySQL structure\n");
        exit(1);
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
        host,
        username,
        password,
        database,
        port,
        unix_socket,
        client_flag
    )) {
        fprintf(stderr, "Failed to connect, with error:\n%s\n", mysql_error(CONN));
        exit(1);
    }

    printf("Successfully connected to host.\n");
    printf("-------------------------------\n");
    
    int result = query_singlestore(CONN);
    printf("-------------------------------\n");

    mysql_close(CONN);
    mysql_library_end();
    printf("MySQL connection and client library successfully closed.\n");
    
    if (result == 0) {
        printf("Query sequence executed as intended.\n");
        exit(0);
    } else {
        printf("Query sequence did not execute as intended.\n");
        exit(1);
    }

}