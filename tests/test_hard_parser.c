#include "hard_parser.h"
#include "soft_parser.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    const char *sql = "SELECT name, age FROM users WHERE age >= 27;";
    Token *tokens = NULL;
    SqlStatement stmt;
    int token_count = 0;

    tokens = soft_parse(sql, &token_count);
    assert(tokens != NULL);
    assert(hard_parse(tokens, token_count, &stmt) == 0);
    assert(stmt.type == SQL_SELECT);
    assert(strcmp(stmt.select.table_name, "users") == 0);
    assert(stmt.select.column_count == 2);
    assert(strcmp(stmt.select.columns[0], "name") == 0);
    assert(strcmp(stmt.select.columns[1], "age") == 0);
    assert(stmt.select.has_where == 1);
    assert(strcmp(stmt.select.where.column, "age") == 0);
    assert(strcmp(stmt.select.where.op, ">=") == 0);
    assert(strcmp(stmt.select.where.value, "27") == 0);

    soft_free_tokens(tokens);
    printf("test_hard_parser passed\n");
    return 0;
}
