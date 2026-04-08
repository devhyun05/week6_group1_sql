#include "hard_parser.h"

#include <stdio.h>
#include <string.h>

static int hard_parser_is_token(const Token *tokens, int token_count, int index,
                                TokenType type, const char *value) {
    if (tokens == NULL || index < 0 || index >= token_count) {
        return 0;
    }

    if (tokens[index].type != type) {
        return 0;
    }

    if (value == NULL) {
        return 1;
    }

    return strcmp(tokens[index].value, value) == 0;
}

static void hard_parser_print_error(const char *message) {
    fprintf(stderr, "Error: %s\n", message);
}

static int hard_parser_expect_keyword(const Token *tokens, int token_count,
                                      int *index, const char *keyword) {
    if (!hard_parser_is_token(tokens, token_count, *index, TOKEN_KEYWORD, keyword)) {
        hard_parser_print_error("Unexpected SQL syntax.");
        return FAILURE;
    }

    (*index)++;
    return SUCCESS;
}

static int hard_parser_expect_identifier(const Token *tokens, int token_count,
                                         int *index, char *dest, size_t dest_size) {
    if (!hard_parser_is_token(tokens, token_count, *index, TOKEN_IDENTIFIER, NULL)) {
        hard_parser_print_error("Expected identifier.");
        return FAILURE;
    }

    if (utils_safe_strcpy(dest, dest_size, tokens[*index].value) != SUCCESS) {
        hard_parser_print_error("Identifier is too long.");
        return FAILURE;
    }

    (*index)++;
    return SUCCESS;
}

static int hard_parser_expect_literal(const Token *tokens, int token_count,
                                      int *index, char *dest, size_t dest_size) {
    TokenType type;

    if (tokens == NULL || index == NULL || dest == NULL) {
        return FAILURE;
    }

    if (*index >= token_count) {
        hard_parser_print_error("Expected literal value.");
        return FAILURE;
    }

    type = tokens[*index].type;
    if (type != TOKEN_INT_LITERAL && type != TOKEN_STR_LITERAL) {
        hard_parser_print_error("Expected literal value.");
        return FAILURE;
    }

    if (utils_safe_strcpy(dest, dest_size, tokens[*index].value) != SUCCESS) {
        hard_parser_print_error("Literal value is too long.");
        return FAILURE;
    }

    (*index)++;
    return SUCCESS;
}

static int hard_parser_consume_optional_semicolon(const Token *tokens,
                                                  int token_count, int *index) {
    if (hard_parser_is_token(tokens, token_count, *index, TOKEN_SEMICOLON, ";")) {
        (*index)++;
    }

    if (*index != token_count) {
        hard_parser_print_error("Unexpected trailing tokens.");
        return FAILURE;
    }

    return SUCCESS;
}

static int hard_parser_parse_insert(const Token *tokens, int token_count,
                                    SqlStatement *out) {
    int index;
    int value_count;

    index = 0;
    value_count = 0;
    memset(out, 0, sizeof(*out));
    out->type = SQL_INSERT;

    if (hard_parser_expect_keyword(tokens, token_count, &index, "INSERT") != SUCCESS ||
        hard_parser_expect_keyword(tokens, token_count, &index, "INTO") != SUCCESS) {
        return FAILURE;
    }

    if (hard_parser_expect_identifier(tokens, token_count, &index,
                                      out->insert.table_name,
                                      sizeof(out->insert.table_name)) != SUCCESS) {
        return FAILURE;
    }

    if (!hard_parser_is_token(tokens, token_count, index, TOKEN_LPAREN, "(")) {
        hard_parser_print_error("Expected '(' after table name.");
        return FAILURE;
    }
    index++;

    while (index < token_count) {
        if (out->insert.column_count >= MAX_COLUMNS) {
            hard_parser_print_error("Too many columns in INSERT statement.");
            return FAILURE;
        }

        if (hard_parser_expect_identifier(tokens, token_count, &index,
                                          out->insert.columns[out->insert.column_count],
                                          sizeof(out->insert.columns[0])) != SUCCESS) {
            return FAILURE;
        }
        out->insert.column_count++;

        if (hard_parser_is_token(tokens, token_count, index, TOKEN_COMMA, ",")) {
            index++;
            continue;
        }
        break;
    }

    if (!hard_parser_is_token(tokens, token_count, index, TOKEN_RPAREN, ")")) {
        hard_parser_print_error("Expected ')' after column list.");
        return FAILURE;
    }
    index++;

    if (hard_parser_expect_keyword(tokens, token_count, &index, "VALUES") != SUCCESS) {
        return FAILURE;
    }

    if (!hard_parser_is_token(tokens, token_count, index, TOKEN_LPAREN, "(")) {
        hard_parser_print_error("Expected '(' before VALUES list.");
        return FAILURE;
    }
    index++;

    while (index < token_count) {
        if (value_count >= MAX_COLUMNS) {
            hard_parser_print_error("Too many values in INSERT statement.");
            return FAILURE;
        }

        if (hard_parser_expect_literal(tokens, token_count, &index,
                                       out->insert.values[value_count],
                                       sizeof(out->insert.values[0])) != SUCCESS) {
            return FAILURE;
        }
        value_count++;

        if (hard_parser_is_token(tokens, token_count, index, TOKEN_COMMA, ",")) {
            index++;
            continue;
        }
        break;
    }

    if (!hard_parser_is_token(tokens, token_count, index, TOKEN_RPAREN, ")")) {
        hard_parser_print_error("Expected ')' after VALUES list.");
        return FAILURE;
    }
    index++;

    if (out->insert.column_count != value_count) {
        hard_parser_print_error("Column count doesn't match value count.");
        return FAILURE;
    }

    return hard_parser_consume_optional_semicolon(tokens, token_count, &index);
}

static int hard_parser_parse_select_columns(const Token *tokens, int token_count,
                                            int *index, SelectStatement *stmt) {
    if (hard_parser_is_token(tokens, token_count, *index, TOKEN_IDENTIFIER, "*")) {
        stmt->column_count = 0;
        (*index)++;
        return SUCCESS;
    }

    while (*index < token_count) {
        if (stmt->column_count >= MAX_COLUMNS) {
            hard_parser_print_error("Too many columns in SELECT statement.");
            return FAILURE;
        }

        if (hard_parser_expect_identifier(tokens, token_count, index,
                                          stmt->columns[stmt->column_count],
                                          sizeof(stmt->columns[0])) != SUCCESS) {
            return FAILURE;
        }
        stmt->column_count++;

        if (hard_parser_is_token(tokens, token_count, *index, TOKEN_COMMA, ",")) {
            (*index)++;
            continue;
        }
        break;
    }

    return SUCCESS;
}

static int hard_parser_parse_where(const Token *tokens, int token_count, int *index,
                                   WhereClause *where) {
    if (hard_parser_expect_identifier(tokens, token_count, index,
                                      where->column,
                                      sizeof(where->column)) != SUCCESS) {
        return FAILURE;
    }

    if (!hard_parser_is_token(tokens, token_count, *index, TOKEN_OPERATOR, NULL)) {
        hard_parser_print_error("Expected operator in WHERE clause.");
        return FAILURE;
    }

    if (utils_safe_strcpy(where->op, sizeof(where->op),
                          tokens[*index].value) != SUCCESS) {
        hard_parser_print_error("WHERE operator is invalid.");
        return FAILURE;
    }
    (*index)++;

    if (hard_parser_expect_literal(tokens, token_count, index,
                                   where->value,
                                   sizeof(where->value)) != SUCCESS) {
        return FAILURE;
    }

    return SUCCESS;
}

static int hard_parser_parse_select(const Token *tokens, int token_count,
                                    SqlStatement *out) {
    int index;

    index = 0;
    memset(out, 0, sizeof(*out));
    out->type = SQL_SELECT;

    if (hard_parser_expect_keyword(tokens, token_count, &index, "SELECT") != SUCCESS) {
        return FAILURE;
    }

    if (hard_parser_parse_select_columns(tokens, token_count, &index,
                                         &out->select) != SUCCESS) {
        return FAILURE;
    }

    if (hard_parser_expect_keyword(tokens, token_count, &index, "FROM") != SUCCESS) {
        return FAILURE;
    }

    if (hard_parser_expect_identifier(tokens, token_count, &index,
                                      out->select.table_name,
                                      sizeof(out->select.table_name)) != SUCCESS) {
        return FAILURE;
    }

    if (hard_parser_is_token(tokens, token_count, index, TOKEN_KEYWORD, "WHERE")) {
        out->select.has_where = 1;
        index++;
        if (hard_parser_parse_where(tokens, token_count, &index,
                                    &out->select.where) != SUCCESS) {
            return FAILURE;
        }
    }

    return hard_parser_consume_optional_semicolon(tokens, token_count, &index);
}

static int hard_parser_parse_delete(const Token *tokens, int token_count,
                                    SqlStatement *out) {
    int index;

    index = 0;
    memset(out, 0, sizeof(*out));
    out->type = SQL_DELETE;

    if (hard_parser_expect_keyword(tokens, token_count, &index, "DELETE") != SUCCESS ||
        hard_parser_expect_keyword(tokens, token_count, &index, "FROM") != SUCCESS) {
        return FAILURE;
    }

    if (hard_parser_expect_identifier(tokens, token_count, &index,
                                      out->delete_stmt.table_name,
                                      sizeof(out->delete_stmt.table_name)) != SUCCESS) {
        return FAILURE;
    }

    if (hard_parser_is_token(tokens, token_count, index, TOKEN_KEYWORD, "WHERE")) {
        out->delete_stmt.has_where = 1;
        index++;
        if (hard_parser_parse_where(tokens, token_count, &index,
                                    &out->delete_stmt.where) != SUCCESS) {
            return FAILURE;
        }
    }

    return hard_parser_consume_optional_semicolon(tokens, token_count, &index);
}

int hard_parse(const Token *tokens, int token_count, SqlStatement *out) {
    if (tokens == NULL || token_count <= 0 || out == NULL) {
        hard_parser_print_error("Empty SQL statement.");
        return FAILURE;
    }

    if (hard_parser_is_token(tokens, token_count, 0, TOKEN_KEYWORD, "INSERT")) {
        return hard_parser_parse_insert(tokens, token_count, out);
    }

    if (hard_parser_is_token(tokens, token_count, 0, TOKEN_KEYWORD, "SELECT")) {
        return hard_parser_parse_select(tokens, token_count, out);
    }

    if (hard_parser_is_token(tokens, token_count, 0, TOKEN_KEYWORD, "DELETE")) {
        return hard_parser_parse_delete(tokens, token_count, out);
    }

    hard_parser_print_error("Unsupported SQL statement.");
    return FAILURE;
}
