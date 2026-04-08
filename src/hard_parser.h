#ifndef HARD_PARSER_H
#define HARD_PARSER_H

#include "soft_parser.h"

typedef enum {
    SQL_INSERT,
    SQL_SELECT,
    SQL_DELETE
} SqlType;

typedef struct {
    char table_name[MAX_IDENTIFIER_LEN];
    int column_count;
    char columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    char values[MAX_COLUMNS][MAX_VALUE_LEN];
} InsertStatement;

typedef struct {
    char column[MAX_IDENTIFIER_LEN];
    char op[4];
    char value[MAX_VALUE_LEN];
} WhereClause;

typedef struct {
    char table_name[MAX_IDENTIFIER_LEN];
    int column_count;
    char columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int has_where;
    WhereClause where;
} SelectStatement;

typedef struct {
    char table_name[MAX_IDENTIFIER_LEN];
    int has_where;
    WhereClause where;
} DeleteStatement;

typedef struct {
    SqlType type;
    union {
        InsertStatement insert;
        SelectStatement select;
        DeleteStatement delete_stmt;
    };
} SqlStatement;

/*
 * Parse a token array into a structured SQL statement.
 * Returns SUCCESS on success, FAILURE on syntax error.
 */
int hard_parse(const Token *tokens, int token_count, SqlStatement *out);

#endif
