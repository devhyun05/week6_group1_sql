#include "utils.h"

#include "common.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int is_space_char(char ch) {
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
}

char *utils_strdup(const char *src) {
    size_t length;
    char *copy;

    if (src == NULL) {
        return NULL;
    }

    length = strlen(src);
    copy = (char *) malloc(length + 1);
    if (copy == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return NULL;
    }

    memcpy(copy, src, length + 1);
    return copy;
}

void utils_safe_strcpy(char *dest, size_t dest_size, const char *src) {
    if (dest == NULL || dest_size == 0) {
        return;
    }

    if (src == NULL) {
        dest[0] = '\0';
        return;
    }

    strncpy(dest, src, dest_size - 1);
    dest[dest_size - 1] = '\0';
}

void utils_trim_in_place(char *value) {
    size_t length;
    size_t start;

    if (value == NULL) {
        return;
    }

    length = strlen(value);
    start = 0;
    while (start < length && is_space_char(value[start])) {
        start++;
    }

    while (length > start && is_space_char(value[length - 1])) {
        length--;
    }

    if (start > 0) {
        memmove(value, value + start, length - start);
    }
    value[length - start] = '\0';
}

char *utils_trim_copy(const char *value) {
    char *copy = utils_strdup(value);

    if (copy == NULL) {
        return NULL;
    }

    utils_trim_in_place(copy);
    return copy;
}

void utils_to_upper_in_place(char *value) {
    size_t index;

    if (value == NULL) {
        return;
    }

    for (index = 0; value[index] != '\0'; ++index) {
        value[index] = (char) toupper((unsigned char) value[index]);
    }
}

int utils_equals_ignore_case(const char *left, const char *right) {
    size_t index;

    if (left == NULL || right == NULL) {
        return 0;
    }

    for (index = 0; left[index] != '\0' || right[index] != '\0'; ++index) {
        if (toupper((unsigned char) left[index]) != toupper((unsigned char) right[index])) {
            return 0;
        }
        if (left[index] == '\0' || right[index] == '\0') {
            break;
        }
    }

    return left[index] == right[index];
}

int utils_is_integer(const char *value) {
    size_t index;

    if (value == NULL || value[0] == '\0') {
        return 0;
    }

    index = 0;
    if (value[index] == '-' || value[index] == '+') {
        index++;
    }

    if (value[index] == '\0') {
        return 0;
    }

    for (; value[index] != '\0'; ++index) {
        if (!isdigit((unsigned char) value[index])) {
            return 0;
        }
    }

    return 1;
}

int utils_compare_values(const char *left, const char *right) {
    long long left_value;
    long long right_value;

    if (left == NULL && right == NULL) {
        return 0;
    }
    if (left == NULL) {
        return -1;
    }
    if (right == NULL) {
        return 1;
    }

    if (utils_is_integer(left) && utils_is_integer(right)) {
        left_value = strtoll(left, NULL, 10);
        right_value = strtoll(right, NULL, 10);

        if (left_value < right_value) {
            return -1;
        }
        if (left_value > right_value) {
            return 1;
        }
        return 0;
    }

    return strcmp(left, right);
}

int utils_append_text(char **buffer, size_t *capacity, const char *text) {
    size_t current_length;
    size_t required_length;
    size_t new_capacity;
    char *new_buffer;

    if (buffer == NULL || capacity == NULL || text == NULL) {
        return FAILURE;
    }

    if (*buffer == NULL) {
        *capacity = INITIAL_DYNAMIC_CAPACITY;
        while (*capacity <= strlen(text)) {
            *capacity *= 2;
        }

        *buffer = (char *) calloc(*capacity, sizeof(char));
        if (*buffer == NULL) {
            fprintf(stderr, "Error: Memory allocation failed.\n");
            return FAILURE;
        }
    }

    current_length = strlen(*buffer);
    required_length = current_length + strlen(text) + 1;
    if (required_length > *capacity) {
        new_capacity = *capacity;
        while (required_length > new_capacity) {
            new_capacity *= 2;
        }

        new_buffer = (char *) realloc(*buffer, new_capacity);
        if (new_buffer == NULL) {
            fprintf(stderr, "Error: Memory allocation failed.\n");
            return FAILURE;
        }

        *buffer = new_buffer;
        *capacity = new_capacity;
    }

    memcpy(*buffer + current_length, text, strlen(text) + 1);
    return SUCCESS;
}

int utils_statement_complete(const char *sql) {
    int in_single_quote = 0;
    size_t index;

    if (sql == NULL) {
        return 0;
    }

    for (index = 0; sql[index] != '\0'; ++index) {
        if (sql[index] == '\'') {
            in_single_quote = !in_single_quote;
        } else if (sql[index] == ';' && !in_single_quote) {
            return 1;
        }
    }

    return 0;
}

static int append_statement(
    char ***statements,
    int *count,
    int *capacity,
    const char *start,
    size_t length
) {
    char *raw;
    char *trimmed;
    char **new_items;

    raw = (char *) malloc(length + 1);
    if (raw == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }

    memcpy(raw, start, length);
    raw[length] = '\0';
    trimmed = utils_trim_copy(raw);
    free(raw);
    raw = NULL;

    if (trimmed == NULL) {
        return FAILURE;
    }

    if (trimmed[0] == '\0') {
        free(trimmed);
        return SUCCESS;
    }

    if (*count >= *capacity) {
        *capacity *= 2;
        new_items = (char **) realloc(*statements, (size_t) (*capacity) * sizeof(char *));
        if (new_items == NULL) {
            fprintf(stderr, "Error: Memory allocation failed.\n");
            free(trimmed);
            return FAILURE;
        }
        *statements = new_items;
    }

    (*statements)[*count] = trimmed;
    (*count)++;
    return SUCCESS;
}

int utils_split_complete_sql_statements(
    const char *sql,
    char ***statements,
    int *statement_count,
    char **remainder
) {
    int in_single_quote = 0;
    int capacity = INITIAL_DYNAMIC_CAPACITY;
    size_t start_index = 0;
    size_t index;

    if (statements == NULL || statement_count == NULL || remainder == NULL) {
        return FAILURE;
    }

    *statements = NULL;
    *statement_count = 0;
    *remainder = NULL;

    if (sql == NULL) {
        return SUCCESS;
    }

    *statements = (char **) calloc((size_t) capacity, sizeof(char *));
    if (*statements == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }

    for (index = 0; sql[index] != '\0'; ++index) {
        if (sql[index] == '\'') {
            in_single_quote = !in_single_quote;
        } else if (sql[index] == ';' && !in_single_quote) {
            if (append_statement(statements, statement_count, &capacity, sql + start_index, index - start_index + 1) != SUCCESS) {
                utils_free_string_array(*statements, *statement_count);
                *statements = NULL;
                *statement_count = 0;
                return FAILURE;
            }
            start_index = index + 1;
        }
    }

    *remainder = utils_strdup(sql + start_index);
    if (*remainder == NULL) {
        utils_free_string_array(*statements, *statement_count);
        *statements = NULL;
        *statement_count = 0;
        return FAILURE;
    }

    return SUCCESS;
}

int utils_split_sql_statements(const char *sql, char ***statements, int *statement_count) {
    char **complete_statements = NULL;
    char *remainder = NULL;
    char *trimmed_remainder = NULL;
    int complete_count = 0;
    int total_count;
    char **new_items;

    if (utils_split_complete_sql_statements(sql, &complete_statements, &complete_count, &remainder) != SUCCESS) {
        return FAILURE;
    }

    trimmed_remainder = utils_trim_copy(remainder == NULL ? "" : remainder);
    free(remainder);
    remainder = NULL;

    if (trimmed_remainder == NULL) {
        utils_free_string_array(complete_statements, complete_count);
        return FAILURE;
    }

    total_count = complete_count;
    if (trimmed_remainder[0] != '\0') {
        new_items = (char **) realloc(complete_statements, (size_t) (complete_count + 1) * sizeof(char *));
        if (new_items == NULL) {
            fprintf(stderr, "Error: Memory allocation failed.\n");
            free(trimmed_remainder);
            utils_free_string_array(complete_statements, complete_count);
            return FAILURE;
        }
        complete_statements = new_items;
        complete_statements[complete_count] = trimmed_remainder;
        total_count++;
    } else {
        free(trimmed_remainder);
    }

    *statements = complete_statements;
    *statement_count = total_count;
    return SUCCESS;
}

void utils_free_string_array(char **strings, int count) {
    int index;

    if (strings == NULL) {
        return;
    }

    for (index = 0; index < count; ++index) {
        free(strings[index]);
        strings[index] = NULL;
    }

    free(strings);
}

char *utils_read_file(const char *path) {
    FILE *fp;
    long size;
    size_t read_size;
    char *buffer;

    fp = fopen(path, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Error: Could not open file '%s'.\n", path);
        return NULL;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fprintf(stderr, "Error: Failed to seek file '%s'.\n", path);
        fclose(fp);
        return NULL;
    }

    size = ftell(fp);
    if (size < 0) {
        fprintf(stderr, "Error: Failed to determine file size for '%s'.\n", path);
        fclose(fp);
        return NULL;
    }

    if (fseek(fp, 0, SEEK_SET) != 0) {
        fprintf(stderr, "Error: Failed to seek file '%s'.\n", path);
        fclose(fp);
        return NULL;
    }

    buffer = (char *) calloc((size_t) size + 1, sizeof(char));
    if (buffer == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        fclose(fp);
        return NULL;
    }

    read_size = fread(buffer, 1, (size_t) size, fp);
    buffer[read_size] = '\0';

    fclose(fp);
    return buffer;
}

static int compare_long_values(const void *left, const void *right) {
    long left_value = *(const long *) left;
    long right_value = *(const long *) right;

    if (left_value < right_value) {
        return -1;
    }
    if (left_value > right_value) {
        return 1;
    }
    return 0;
}

void utils_sort_longs(long *values, int count) {
    if (values == NULL || count <= 1) {
        return;
    }

    qsort(values, (size_t) count, sizeof(long), compare_long_values);
}
