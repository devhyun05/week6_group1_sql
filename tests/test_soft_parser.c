#include "soft_parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int assert_true(int condition, const char *message) {
    if (!condition) {
        fprintf(stderr, "[FAIL] %s\n", message);
        return FAILURE;
    }
    return SUCCESS;
}

int main(void) {
    Token *tokens;
    int token_count;

    tokens = soft_parse(
        "insert INTO users (id, name, age) VALUES (1, 'Lee, Jr.', 30);",
        &token_count);
    if (assert_true(tokens != NULL, "soft_parse should return tokens") != SUCCESS ||
        assert_true(token_count == 19, "soft_parse should tokenize full INSERT") != SUCCESS ||
        assert_true(tokens[0].type == TOKEN_KEYWORD &&
                        strcmp(tokens[0].value, "INSERT") == 0,
                    "INSERT should be normalized to keyword") != SUCCESS ||
        assert_true(tokens[14].type == TOKEN_STR_LITERAL &&
                        strcmp(tokens[14].value, "Lee, Jr.") == 0,
                    "quoted comma text should stay in one token") != SUCCESS ||
        assert_true(tokens[18].type == TOKEN_SEMICOLON,
                    "statement should end with semicolon token") != SUCCESS) {
        free(tokens);
        return EXIT_FAILURE;
    }
    free(tokens);

    tokens = soft_parse("Select * FROM users WHERE age >= 27;", &token_count);
    if (assert_true(tokens != NULL, "soft_parse should support SELECT") != SUCCESS ||
        assert_true(token_count == 9, "SELECT token count should match") != SUCCESS ||
        assert_true(tokens[1].type == TOKEN_IDENTIFIER &&
                        strcmp(tokens[1].value, "*") == 0,
                    "asterisk should be tokenized for SELECT *") != SUCCESS ||
        assert_true(tokens[7].type == TOKEN_INT_LITERAL &&
                        strcmp(tokens[7].value, "27") == 0,
                    "integer literal should be parsed") != SUCCESS) {
        free(tokens);
        return EXIT_FAILURE;
    }
    free(tokens);

    tokens = soft_parse("DELETE FROM users WHERE name = 'Alice';", &token_count);
    if (assert_true(tokens != NULL, "soft_parse should support DELETE") != SUCCESS ||
        assert_true(token_count == 8, "DELETE token count should match") != SUCCESS ||
        assert_true(tokens[0].type == TOKEN_KEYWORD &&
                        strcmp(tokens[0].value, "DELETE") == 0,
                    "DELETE should be normalized to keyword") != SUCCESS ||
        assert_true(tokens[6].type == TOKEN_STR_LITERAL &&
                        strcmp(tokens[6].value, "Alice") == 0,
                    "DELETE should parse string literal in WHERE") != SUCCESS) {
        free(tokens);
        return EXIT_FAILURE;
    }
    free(tokens);

    puts("[PASS] soft parser");
    return EXIT_SUCCESS;
}
