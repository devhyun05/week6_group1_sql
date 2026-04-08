#include "hard_parser.h"

#include "utils.h"

#include <stdio.h>
#include <string.h>

/* 하드 파서는 이미 토큰화된 입력을 받아서
 * 그 토큰들이 SQL 문법 순서에 맞게 등장하는지 검사한다.
 */
static int is_token(const Token *tokens, int token_count, int index, TokenType type, const char *value) {
    if (index >= token_count) {
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

static int expect_identifier(const Token *tokens, int token_count, int *index, char *dest, size_t dest_size, const char *label) {
    if (*index >= token_count || tokens[*index].type != TOKEN_IDENTIFIER) {
        fprintf(stderr, "Error: Expected %s.\n", label);
        return FAILURE;
    }

    utils_safe_strcpy(dest, dest_size, tokens[*index].value);
    (*index)++;
    return SUCCESS;
}

static int expect_token(const Token *tokens, int token_count, int *index, TokenType type, const char *value, const char *label) {
    if (!is_token(tokens, token_count, *index, type, value)) {
        fprintf(stderr, "Error: Expected %s.\n", label);
        return FAILURE;
    }

    (*index)++;
    return SUCCESS;
}

/* VALUES (...)는 컬럼 목록을 먼저 읽은 뒤 파싱한다.
 * 그래야 INSERT의 기본 규칙인 "컬럼 수와 값 수가 같아야 한다"를
 * 정확히 검사할 수 있다.
 */
static int parse_insert_values(const Token *tokens, int token_count, int *index, InsertStatement *stmt) {
    int value_count = 0;

    while (*index < token_count &&
           (tokens[*index].type == TOKEN_INT_LITERAL || tokens[*index].type == TOKEN_STR_LITERAL)) {
        if (value_count >= MAX_COLUMNS) {
            fprintf(stderr, "Error: Too many values in INSERT statement.\n");
            return FAILURE;
        }

        utils_safe_strcpy(stmt->values[value_count], sizeof(stmt->values[value_count]), tokens[*index].value);
        value_count++;
        (*index)++;

        if (is_token(tokens, token_count, *index, TOKEN_COMMA, ",")) {
            (*index)++;
            continue;
        }
        break;
    }

    if (value_count != stmt->column_count) {
        fprintf(stderr, "Error: Column count doesn't match value count.\n");
        return FAILURE;
    }

    return SUCCESS;
}

/* 이 함수는 토큰 배열이 아래 형태와 맞는지 순서대로 확인한다.
 * INSERT INTO <table> (col, ...) VALUES (value, ...)
 * 파싱에 성공하면 InsertStatement 구조체를 채워서,
 * 이후 executor/storage가 원본 SQL 문자열을 다시 해석하지 않아도 되게 만든다.
 */
static int parse_insert(const Token *tokens, int token_count, SqlStatement *out) {
    int index = 0;
    InsertStatement *stmt = &out->insert;

    memset(stmt, 0, sizeof(*stmt));
    out->type = SQL_INSERT;

    if (expect_token(tokens, token_count, &index, TOKEN_KEYWORD, "INSERT", "'INSERT'") != SUCCESS ||
        expect_token(tokens, token_count, &index, TOKEN_KEYWORD, "INTO", "'INTO'") != SUCCESS ||
        expect_identifier(tokens, token_count, &index, stmt->table_name, sizeof(stmt->table_name), "table name") != SUCCESS ||
        expect_token(tokens, token_count, &index, TOKEN_LPAREN, "(", "'('") != SUCCESS) {
        return FAILURE;
    }

    /* 닫는 괄호 ')'가 나올 때까지 컬럼 이름을 읽는다.
     * 이미 소프트 파서가 쉼표와 식별자를 토큰으로 나눠놨기 때문에
     * 여기서는 토큰 배열을 순서대로 따라가기만 하면 된다.
     */
    while (index < token_count && tokens[index].type == TOKEN_IDENTIFIER) {
        if (stmt->column_count >= MAX_COLUMNS) {
            fprintf(stderr, "Error: Too many columns in INSERT statement.\n");
            return FAILURE;
        }

        utils_safe_strcpy(
            stmt->columns[stmt->column_count],
            sizeof(stmt->columns[stmt->column_count]),
            tokens[index].value
        );
        stmt->column_count++;
        index++;

        if (is_token(tokens, token_count, index, TOKEN_COMMA, ",")) {
            index++;
            continue;
        }
        break;
    }

    if (stmt->column_count == 0) {
        fprintf(stderr, "Error: INSERT requires at least one column.\n");
        return FAILURE;
    }

    if (expect_token(tokens, token_count, &index, TOKEN_RPAREN, ")", "')'") != SUCCESS ||
        expect_token(tokens, token_count, &index, TOKEN_KEYWORD, "VALUES", "'VALUES'") != SUCCESS ||
        expect_token(tokens, token_count, &index, TOKEN_LPAREN, "(", "'('") != SUCCESS) {
        return FAILURE;
    }

    if (parse_insert_values(tokens, token_count, &index, stmt) != SUCCESS) {
        return FAILURE;
    }

    if (expect_token(tokens, token_count, &index, TOKEN_RPAREN, ")", "')'") != SUCCESS) {
        return FAILURE;
    }

    if (is_token(tokens, token_count, index, TOKEN_SEMICOLON, ";")) {
        index++;
    }

    if (index != token_count) {
        fprintf(stderr, "Error: Unexpected token after INSERT statement.\n");
        return FAILURE;
    }

    return SUCCESS;
}

/* SELECT는 '*' 또는 명시적인 컬럼 목록 둘 다 허용한다.
 * 하드 파서는 두 경우를 모두 SelectStatement 구조체 형태로 정리해 둔다.
 */
static int parse_select_columns(const Token *tokens, int token_count, int *index, SelectStatement *stmt) {
    if (is_token(tokens, token_count, *index, TOKEN_ASTERISK, "*")) {
        stmt->column_count = 0;
        (*index)++;
        return SUCCESS;
    }

    while (*index < token_count && tokens[*index].type == TOKEN_IDENTIFIER) {
        if (stmt->column_count >= MAX_COLUMNS) {
            fprintf(stderr, "Error: Too many columns in SELECT statement.\n");
            return FAILURE;
        }

        utils_safe_strcpy(
            stmt->columns[stmt->column_count],
            sizeof(stmt->columns[stmt->column_count]),
            tokens[*index].value
        );
        stmt->column_count++;
        (*index)++;

        if (is_token(tokens, token_count, *index, TOKEN_COMMA, ",")) {
            (*index)++;
            continue;
        }
        break;
    }

    if (stmt->column_count == 0) {
        fprintf(stderr, "Error: SELECT requires '*' or at least one column.\n");
        return FAILURE;
    }

    return SUCCESS;
}

/* WHERE 절은 선택 사항이다.
 * 하지만 존재할 경우에는 반드시
 * "<식별자> <연산자> <리터럴>" 형태인지 검사하고,
 * 각 요소를 분리해서 구조체에 저장한다.
 */
static int parse_where_clause(const Token *tokens, int token_count, int *index, SelectStatement *stmt) {
    if (!is_token(tokens, token_count, *index, TOKEN_KEYWORD, "WHERE")) {
        stmt->has_where = 0;
        return SUCCESS;
    }

    stmt->has_where = 1;
    (*index)++;

    if (expect_identifier(tokens, token_count, index, stmt->where.column, sizeof(stmt->where.column), "WHERE column") != SUCCESS) {
        return FAILURE;
    }

    if (*index >= token_count || tokens[*index].type != TOKEN_OPERATOR) {
        fprintf(stderr, "Error: Expected WHERE operator.\n");
        return FAILURE;
    }

    utils_safe_strcpy(stmt->where.op, sizeof(stmt->where.op), tokens[*index].value);
    (*index)++;

    if (*index >= token_count ||
        (tokens[*index].type != TOKEN_INT_LITERAL && tokens[*index].type != TOKEN_STR_LITERAL)) {
        fprintf(stderr, "Error: Expected WHERE value.\n");
        return FAILURE;
    }

    utils_safe_strcpy(stmt->where.value, sizeof(stmt->where.value), tokens[*index].value);
    (*index)++;
    return SUCCESS;
}

/* SELECT 파싱은 아래 문법 순서를 그대로 따른다.
 * SELECT <columns> FROM <table> [WHERE ...]
 */
static int parse_select(const Token *tokens, int token_count, SqlStatement *out) {
    int index = 0;
    SelectStatement *stmt = &out->select;

    memset(stmt, 0, sizeof(*stmt));
    out->type = SQL_SELECT;

    if (expect_token(tokens, token_count, &index, TOKEN_KEYWORD, "SELECT", "'SELECT'") != SUCCESS) {
        return FAILURE;
    }

    if (parse_select_columns(tokens, token_count, &index, stmt) != SUCCESS) {
        return FAILURE;
    }

    if (expect_token(tokens, token_count, &index, TOKEN_KEYWORD, "FROM", "'FROM'") != SUCCESS ||
        expect_identifier(tokens, token_count, &index, stmt->table_name, sizeof(stmt->table_name), "table name") != SUCCESS) {
        return FAILURE;
    }

    if (parse_where_clause(tokens, token_count, &index, stmt) != SUCCESS) {
        return FAILURE;
    }

    if (is_token(tokens, token_count, index, TOKEN_SEMICOLON, ";")) {
        index++;
    }

    if (index != token_count) {
        fprintf(stderr, "Error: Unexpected token after SELECT statement.\n");
        return FAILURE;
    }

    return SUCCESS;
}

/* 하드 파서의 분기 지점이다.
 * 첫 번째 키워드를 보고 INSERT 문법으로 해석할지,
 * SELECT 문법으로 해석할지를 결정한다.
 */
int hard_parse(const Token *tokens, int token_count, SqlStatement *out) {
    if (tokens == NULL || token_count <= 0 || out == NULL) {
        fprintf(stderr, "Error: Invalid parser input.\n");
        return FAILURE;
    }

    if (tokens[0].type != TOKEN_KEYWORD) {
        fprintf(stderr, "Error: SQL statement must start with a keyword.\n");
        return FAILURE;
    }

    if (strcmp(tokens[0].value, "INSERT") == 0) {
        return parse_insert(tokens, token_count, out);
    }

    if (strcmp(tokens[0].value, "SELECT") == 0) {
        return parse_select(tokens, token_count, out);
    }

    fprintf(stderr, "Error: Unsupported SQL statement '%s'.\n", tokens[0].value);
    return FAILURE;
}
