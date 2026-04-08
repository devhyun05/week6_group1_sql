#ifndef SOFT_PARSER_H
#define SOFT_PARSER_H

#include "common.h"

typedef enum {
    TOKEN_KEYWORD,
    TOKEN_IDENTIFIER,
    TOKEN_INT_LITERAL,
    TOKEN_STR_LITERAL,
    TOKEN_OPERATOR,
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_COMMA,
    TOKEN_SEMICOLON,
    TOKEN_ASTERISK,
    TOKEN_UNKNOWN
} TokenType;

typedef struct {
    TokenType type;
    char value[MAX_TOKEN_VALUE_LEN];
} Token;

/**
 * Convert a SQL string into a dynamically allocated token array.
 * The caller must free the returned buffer with soft_free_tokens().
 */
Token *soft_parse(const char *sql, int *token_count);

/**
 * Free a token array returned by soft_parse().
 */
void soft_free_tokens(Token *tokens);

#endif
