#include <stdio.h>
#include <stdlib.h>
#include <mysql.h>

typedef struct {
    const char *env_name;
    const char **var_dest;
} EnvMap;

void process_row(MYSQL_ROW *row) {
    for (int i = 0; i <= 12; i++) {
        printf("%s\t", (*row)[i]);
    }
    printf("\n");
}

int ensure_envs(size_t envc, const EnvMap *envm)
{
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

int query_singlestore(MYSQL *CONN) {
    if (mysql_query(CONN, "SELECT * FROM uk_price_paid")) {
        fprintf(stderr, "Query error: %s\n", mysql_error(CONN));
        return 1;
    }

    MYSQL_RES *result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "Result error: %s\n", mysql_error(CONN));
        mysql_free_result(result);
        return 1;
    }

    // Process results  
    MYSQL_ROW row;
    printf("Table:\n");
    // must fetch row until NULL when using use_result over store_result
    while ((row = mysql_fetch_row(result))) {
        process_row(&row);
    }

    mysql_free_result(result);
    return 0;
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