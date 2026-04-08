#include "soft_parser.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    const char *sql = "INSERT INTO users (name, note) VALUES ('Lee, Jr.', 'Hello world');";
    Token *tokens = NULL;
    int token_count = 0;

    tokens = soft_parse(sql, &token_count);
    assert(tokens != NULL);
    assert(token_count == 15);
    assert(tokens[0].type == TOKEN_KEYWORD);
    assert(strcmp(tokens[0].value, "INSERT") == 0);
    assert(tokens[10].type == TOKEN_STR_LITERAL);
    assert(strcmp(tokens[10].value, "Lee, Jr.") == 0);
    assert(tokens[12].type == TOKEN_STR_LITERAL);
    assert(strcmp(tokens[12].value, "Hello world") == 0);

    soft_free_tokens(tokens);
    printf("test_soft_parser passed\n");
    return 0;
}
