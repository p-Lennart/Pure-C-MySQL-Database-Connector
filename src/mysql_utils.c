#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/mysql_utils.h"
#include "../include/status_codes.h"

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
