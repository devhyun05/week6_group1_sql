#include "soft_parser.h"

#include "utils.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int is_identifier_start(char ch) {
    return isalpha((unsigned char) ch) || ch == '_';
}

static int is_identifier_char(char ch) {
    return isalnum((unsigned char) ch) || ch == '_';
}

static int append_token(Token **tokens, int *count, int *capacity, TokenType type, const char *value) {
    Token *new_tokens;

    if (*count >= *capacity) {
        *capacity *= 2;
        new_tokens = (Token *) realloc(*tokens, (size_t) (*capacity) * sizeof(Token));
        if (new_tokens == NULL) {
            fprintf(stderr, "Error: Memory allocation failed.\n");
            return FAILURE;
        }
        *tokens = new_tokens;
    }

    (*tokens)[*count].type = type;
    utils_safe_strcpy((*tokens)[*count].value, sizeof((*tokens)[*count].value), value);
    (*count)++;
    return SUCCESS;
}

/* 소프트 파서는 "이 문자열 조각이 어떤 종류의 토큰인가?"만 판단한다.
 * 아직 SQL 문법에 맞는 위치인지까지는 검사하지 않고,
 * 그 검사는 다음 단계인 하드 파서가 담당한다.
 */
static int is_keyword(const char *value) {
    static const char *keywords[] = {
        "INSERT", "INTO", "VALUES", "SELECT", "FROM", "WHERE"
    };
    size_t index;

    for (index = 0; index < sizeof(keywords) / sizeof(keywords[0]); ++index) {
        if (strcmp(value, keywords[index]) == 0) {
            return 1;
        }
    }

    return 0;
}

/* 작은따옴표로 감싼 문자열은 하나의 토큰으로 읽는다.
 * 그래야 문자열 내부의 쉼표나 공백이 SQL 구분자로 잘못 해석되지 않는다.
 */
static int parse_string_literal(const char *sql, size_t *index, Token **tokens, int *count, int *capacity) {
    char value[MAX_TOKEN_VALUE_LEN];
    size_t value_index = 0;

    (*index)++;
    while (sql[*index] != '\0' && sql[*index] != '\'') {
        if (value_index + 1 >= sizeof(value)) {
            fprintf(stderr, "Error: String literal is too long.\n");
            return FAILURE;
        }
        value[value_index++] = sql[*index];
        (*index)++;
    }

    if (sql[*index] != '\'') {
        fprintf(stderr, "Error: Unterminated string literal.\n");
        return FAILURE;
    }

    value[value_index] = '\0';
    if (append_token(tokens, count, capacity, TOKEN_STR_LITERAL, value) != SUCCESS) {
        return FAILURE;
    }

    (*index)++;
    return SUCCESS;
}

/* 정수 리터럴 인식도 소프트 파서의 역할이다.
 * 여기서는 "부호가 있을 수 있는 정수 모양인지"만 보고,
 * 더 깊은 의미 검사는 뒤 단계로 넘긴다.
 */
static int parse_number_literal(const char *sql, size_t *index, Token **tokens, int *count, int *capacity) {
    char value[MAX_TOKEN_VALUE_LEN];
    size_t value_index = 0;
    size_t start_index = *index;

    if (sql[*index] == '-' || sql[*index] == '+') {
        value[value_index++] = sql[*index];
        (*index)++;
    }

    while (isdigit((unsigned char) sql[*index])) {
        if (value_index + 1 >= sizeof(value)) {
            fprintf(stderr, "Error: Integer literal is too long.\n");
            return FAILURE;
        }
        value[value_index++] = sql[*index];
        (*index)++;
    }

    if (*index == start_index || ((*index == start_index + 1) && (value[0] == '-' || value[0] == '+'))) {
        fprintf(stderr, "Error: Invalid integer literal.\n");
        return FAILURE;
    }

    value[value_index] = '\0';
    if (append_token(tokens, count, capacity, TOKEN_INT_LITERAL, value) != SUCCESS) {
        return FAILURE;
    }

    return SUCCESS;
}

/* 키워드는 대문자로 정규화한다.
 * 그래서 "select", "SELECT", "SeLeCt"를 모두 같은 키워드로 처리할 수 있다.
 */
static int parse_identifier_or_keyword(const char *sql, size_t *index, Token **tokens, int *count, int *capacity) {
    char value[MAX_TOKEN_VALUE_LEN];
    char upper_value[MAX_TOKEN_VALUE_LEN];
    size_t value_index = 0;
    TokenType type;

    while (is_identifier_char(sql[*index])) {
        if (value_index + 1 >= sizeof(value)) {
            fprintf(stderr, "Error: Identifier is too long.\n");
            return FAILURE;
        }
        value[value_index++] = sql[*index];
        (*index)++;
    }

    value[value_index] = '\0';
    utils_safe_strcpy(upper_value, sizeof(upper_value), value);
    utils_to_upper_in_place(upper_value);
    type = is_keyword(upper_value) ? TOKEN_KEYWORD : TOKEN_IDENTIFIER;

    if (append_token(tokens, count, capacity, type, type == TOKEN_KEYWORD ? upper_value : value) != SUCCESS) {
        return FAILURE;
    }

    return SUCCESS;
}

/* 비교 연산자는 한 토큰으로 묶는다.
 * 예를 들어 >=, <=, != 를 각각 하나의 TOKEN_OPERATOR로 만들어야
 * 하드 파서가 WHERE 절을 단순하게 해석할 수 있다.
 */
static int parse_operator_token(const char *sql, size_t *index, Token **tokens, int *count, int *capacity) {
    char value[3] = {0};

    value[0] = sql[*index];
    if ((sql[*index] == '!' || sql[*index] == '<' || sql[*index] == '>') && sql[*index + 1] == '=') {
        value[1] = '=';
        (*index) += 2;
    } else {
        (*index)++;
    }

    if (append_token(tokens, count, capacity, TOKEN_OPERATOR, value) != SUCCESS) {
        return FAILURE;
    }

    return SUCCESS;
}

/* 소프트 파서는 "문자열 -> 토큰 배열" 단계다.
 * 즉, SQL 문장을 의미 단위로 자르는 역할만 하고,
 * 이 토큰들의 순서가 올바른 INSERT/SELECT 문장인지는 아직 판단하지 않는다.
 */
Token *soft_parse(const char *sql, int *token_count) {
    Token *tokens;
    int count = 0;
    int capacity = INITIAL_DYNAMIC_CAPACITY;
    size_t index = 0;

    if (token_count == NULL) {
        return NULL;
    }

    *token_count = 0;
    if (sql == NULL) {
        return NULL;
    }

    tokens = (Token *) calloc((size_t) capacity, sizeof(Token));
    if (tokens == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return NULL;
    }

    while (sql[index] != '\0') {
        if (isspace((unsigned char) sql[index])) {
            index++;
            continue;
        }

        /* 문자열 리터럴은 가장 먼저 처리한다.
         * 그래야 문자열 안에 있는 쉼표나 괄호가 SQL 문법 기호로 오해되지 않는다.
         */
        if (sql[index] == '\'') {
            if (parse_string_literal(sql, &index, &tokens, &count, &capacity) != SUCCESS) {
                free(tokens);
                return NULL;
            }
            continue;
        }

        /* 괄호, 쉼표, 세미콜론 같은 단일 구두점은 각각 하나의 토큰이 된다. */
        if (sql[index] == '(') {
            if (append_token(&tokens, &count, &capacity, TOKEN_LPAREN, "(") != SUCCESS) {
                free(tokens);
                return NULL;
            }
            index++;
            continue;
        }

        if (sql[index] == ')') {
            if (append_token(&tokens, &count, &capacity, TOKEN_RPAREN, ")") != SUCCESS) {
                free(tokens);
                return NULL;
            }
            index++;
            continue;
        }

        if (sql[index] == ',') {
            if (append_token(&tokens, &count, &capacity, TOKEN_COMMA, ",") != SUCCESS) {
                free(tokens);
                return NULL;
            }
            index++;
            continue;
        }

        if (sql[index] == ';') {
            if (append_token(&tokens, &count, &capacity, TOKEN_SEMICOLON, ";") != SUCCESS) {
                free(tokens);
                return NULL;
            }
            index++;
            continue;
        }

        if (sql[index] == '*') {
            if (append_token(&tokens, &count, &capacity, TOKEN_ASTERISK, "*") != SUCCESS) {
                free(tokens);
                return NULL;
            }
            index++;
            continue;
        }

        /* 연산자는 1글자일 수도 있고 2글자일 수도 있으므로
         * 일반 식별자보다 먼저 확인해서 하나의 연산자 토큰으로 만든다.
         */
        if (sql[index] == '=' || sql[index] == '!' || sql[index] == '<' || sql[index] == '>') {
            if (parse_operator_token(sql, &index, &tokens, &count, &capacity) != SUCCESS) {
                free(tokens);
                return NULL;
            }
            continue;
        }

        /* 숫자 리터럴은 모양만 보고 토큰화한다. */
        if (isdigit((unsigned char) sql[index]) ||
            ((sql[index] == '-' || sql[index] == '+') && isdigit((unsigned char) sql[index + 1]))) {
            if (parse_number_literal(sql, &index, &tokens, &count, &capacity) != SUCCESS) {
                free(tokens);
                return NULL;
            }
            continue;
        }

        /* 남은 단어 형태 문자열은 식별자이거나 키워드다.
         * 키워드라면 대문자로 정규화해서 저장한다.
         */
        if (is_identifier_start(sql[index])) {
            if (parse_identifier_or_keyword(sql, &index, &tokens, &count, &capacity) != SUCCESS) {
                free(tokens);
                return NULL;
            }
            continue;
        }

        fprintf(stderr, "Error: Unknown token starting at '%c'.\n", sql[index]);
        free(tokens);
        return NULL;
    }

    *token_count = count;
    return tokens;
}

void soft_free_tokens(Token *tokens) {
    free(tokens);
}
