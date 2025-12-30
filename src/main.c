#include <stdio.h>
#include <stdlib.h>
#include <mysql.h>

void process_row(MYSQL_ROW *row) {
    for (int i = 0; i <= 12; i++) {
        printf("%s\t", (*row)[i]);
    }
    printf("\n");
}

int main(int argc, char *argv[]) {
    MYSQL *CONN;

    // Params
    const char *host = getenv("SS_host");
    const char *port_str = getenv("SS_port");
    const char *username = getenv("SS_user");
    const char *password = getenv("SS_pass");
    const char *database = getenv("SS_db");

    if (!host) {
        fprintf(stderr, "missing host env: SS_host\n");
        exit(1);
    }

    if (!port_str) {
        fprintf(stderr, "missing port env: SS_port\n");
        exit(1);
    }
    
    unsigned int port = atoi(port_str);
    // if 0, mysql does default port handling, no exit

    if (!username) {
        fprintf(stderr, "missing username env: SS_usr\n");
        exit(1);
    }

    if (!password) {
        fprintf(stderr, "missing password env: SS_pwd\n");
        exit(1);
}

    if (!database) {
        fprintf(stderr, "missing database env: SS_db\n");
        exit(1);
    }

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
    
    // MariaDB connector will automatically check Windows System Store, do not need to manually load
    // if (argc > 1) {
    //     printf("SSL CA file specified, path %s\n", argv[1]);
    //     FILE *file = fopen(argv[1], "r");
    //     if (file != NULL) {
    //         fclose(file);
    //         mysql_options(CONN, MYSQL_OPT_SSL_CA, argv[1]);

    //     } else {
    //         printf("Failed to locate specified SSL CA file");
    //     }
    // } else {
    //     printf("No SSL CA file specified.");
    // }
    
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
    
    if (mysql_query(CONN, "SELECT * FROM uk_price_paid")) {
        fprintf(stderr, "Query error: %s\n", mysql_error(CONN));
        goto error_exit;
    }

    MYSQL_RES *result = mysql_use_result(CONN);
    if (result == NULL) {
        fprintf(stderr, "Result error: %s\n", mysql_error(CONN));
        goto error_exit;
    }

    // Process results  
    MYSQL_ROW row;
    printf("Table:\n");
    // must fetch row until NULL when using use_result over store_result
    while ((row = mysql_fetch_row(result))) {
        process_row(&row);
    }

    mysql_free_result(result);
    mysql_close(CONN);
    mysql_library_end();
    printf("MySQL connection and client library successfully closed.\n");
    exit(0);

error_exit:
    mysql_close(CONN);
    mysql_library_end();
    printf("MySQL connection and client library closed with error.\n");
    exit(1);
}