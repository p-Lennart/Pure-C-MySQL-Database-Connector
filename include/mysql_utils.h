#include <mysql.h>

#ifndef MYSQL_UTILS_H
#define MYSQL_UTILS_H

#define MAX_TABLE_OPTIONS (10)
#define TABLE_NAME_CAP (25)

typedef struct {
    const char *host;
    unsigned int port;
    const char *username;
    const char *password;
    const char *database;
} Conn_Args;

int prompt_select_table(MYSQL *CONN, char (*table_name)[TABLE_NAME_CAP]);

MYSQL *connect(const Conn_Args *conn_args);

#endif