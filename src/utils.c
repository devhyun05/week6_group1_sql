#include "utils.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *utils_strdup(const char *src) {
    size_t length;
    char *copy;

    if (src == NULL) {
        return NULL;
    }

    length = strlen(src);
    copy = (char *)malloc(length + 1);
    if (copy == NULL) {
        fprintf(stderr, "Error: Failed to allocate memory.\n");
        return NULL;
    }

    memcpy(copy, src, length + 1);
    return copy;
}

int utils_safe_strcpy(char *dest, size_t dest_size, const char *src) {
    int written;

    if (dest == NULL || src == NULL || dest_size == 0) {
        return FAILURE;
    }

    written = snprintf(dest, dest_size, "%s", src);
    if (written < 0 || (size_t)written >= dest_size) {
        return FAILURE;
    }

    return SUCCESS;
}

void utils_trim(char *text) {
    size_t length;
    size_t start;

    if (text == NULL) {
        return;
    }

    length = strlen(text);
    while (length > 0 && isspace((unsigned char)text[length - 1])) {
        text[length - 1] = '\0';
        length--;
    }

    start = 0;
    while (text[start] != '\0' && isspace((unsigned char)text[start])) {
        start++;
    }

    if (start > 0) {
        memmove(text, text + start, strlen(text + start) + 1);
    }
}

int utils_to_upper_copy(const char *src, char *dest, size_t dest_size) {
    size_t i;

    if (src == NULL || dest == NULL || dest_size == 0) {
        return FAILURE;
    }

    for (i = 0; src[i] != '\0'; i++) {
        if (i + 1 >= dest_size) {
            return FAILURE;
        }
        dest[i] = (char)toupper((unsigned char)src[i]);
    }

    dest[i] = '\0';
    return SUCCESS;
}

int utils_equals_ignore_case(const char *lhs, const char *rhs) {
    size_t i;

    if (lhs == NULL || rhs == NULL) {
        return 0;
    }

    for (i = 0; lhs[i] != '\0' && rhs[i] != '\0'; i++) {
        if (toupper((unsigned char)lhs[i]) !=
            toupper((unsigned char)rhs[i])) {
            return 0;
        }
    }

    return lhs[i] == '\0' && rhs[i] == '\0';
}

int utils_is_sql_keyword(const char *text) {
    static const char *keywords[] = {
        "INSERT", "SELECT", "DELETE", "INTO", "FROM", "WHERE", "VALUES"
    };
    size_t i;

    if (text == NULL) {
        return 0;
    }

    for (i = 0; i < sizeof(keywords) / sizeof(keywords[0]); i++) {
        if (utils_equals_ignore_case(text, keywords[i])) {
            return 1;
        }
    }

    return 0;
}

int utils_is_integer(const char *text) {
    size_t i;

    if (text == NULL || text[0] == '\0') {
        return 0;
    }

    i = 0;
    if (text[0] == '-' || text[0] == '+') {
        if (text[1] == '\0') {
            return 0;
        }
        i = 1;
    }

    for (; text[i] != '\0'; i++) {
        if (!isdigit((unsigned char)text[i])) {
            return 0;
        }
    }

    return 1;
}

long long utils_parse_integer(const char *text) {
    return strtoll(text, NULL, 10);
}

int utils_compare_values(const char *lhs, const char *rhs) {
    long long left_number;
    long long right_number;

    if (lhs == NULL || rhs == NULL) {
        return 0;
    }

    if (utils_is_integer(lhs) && utils_is_integer(rhs)) {
        left_number = utils_parse_integer(lhs);
        right_number = utils_parse_integer(rhs);

        if (left_number < right_number) {
            return -1;
        }
        if (left_number > right_number) {
            return 1;
        }
        return 0;
    }

    return strcmp(lhs, rhs);
}

char *utils_read_file(const char *path) {
    FILE *fp;
    long file_size;
    size_t read_size;
    char *buffer;

    if (path == NULL) {
        return NULL;
    }

    fp = fopen(path, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Error: Failed to open file '%s'.\n", path);
        return NULL;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fprintf(stderr, "Error: Failed to seek file '%s'.\n", path);
        fclose(fp);
        return NULL;
    }

    file_size = ftell(fp);
    if (file_size < 0) {
        fprintf(stderr, "Error: Failed to read file size for '%s'.\n", path);
        fclose(fp);
        return NULL;
    }

    if (fseek(fp, 0, SEEK_SET) != 0) {
        fprintf(stderr, "Error: Failed to rewind file '%s'.\n", path);
        fclose(fp);
        return NULL;
    }

    buffer = (char *)malloc((size_t)file_size + 1);
    if (buffer == NULL) {
        fprintf(stderr, "Error: Failed to allocate memory.\n");
        fclose(fp);
        return NULL;
    }

    read_size = fread(buffer, 1, (size_t)file_size, fp);
    if (read_size != (size_t)file_size && ferror(fp)) {
        fprintf(stderr, "Error: Failed to read file '%s'.\n", path);
        free(buffer);
        fclose(fp);
        return NULL;
    }

    buffer[read_size] = '\0';
    fclose(fp);
    return buffer;
}

int utils_append_buffer(char **buffer, size_t *length, size_t *capacity,
                        const char *suffix) {
    size_t suffix_length;
    size_t required;
    size_t new_capacity;
    char *new_buffer;

    if (buffer == NULL || length == NULL || capacity == NULL || suffix == NULL) {
        return FAILURE;
    }

    if (*buffer == NULL) {
        *capacity = strlen(suffix) + 64;
        *buffer = (char *)malloc(*capacity);
        if (*buffer == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            return FAILURE;
        }
        (*buffer)[0] = '\0';
        *length = 0;
    }

    suffix_length = strlen(suffix);
    required = *length + suffix_length + 1;
    if (required > *capacity) {
        new_capacity = *capacity;
        while (required > new_capacity) {
            new_capacity *= 2;
        }

        new_buffer = (char *)realloc(*buffer, new_capacity);
        if (new_buffer == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            return FAILURE;
        }

        *buffer = new_buffer;
        *capacity = new_capacity;
    }

    memcpy(*buffer + *length, suffix, suffix_length + 1);
    *length += suffix_length;
    return SUCCESS;
}

int utils_find_statement_terminator(const char *text, size_t start_index) {
    int in_quotes;
    size_t i;

    if (text == NULL) {
        return FAILURE;
    }

    in_quotes = 0;
    for (i = start_index; text[i] != '\0'; i++) {
        if (text[i] == '\'') {
            if (in_quotes && text[i + 1] == '\'') {
                i++;
                continue;
            }
            in_quotes = !in_quotes;
            continue;
        }

        if (!in_quotes && text[i] == ';') {
            return (int)i;
        }
    }

    return FAILURE;
}

int utils_has_statement_terminator(const char *text) {
    return utils_find_statement_terminator(text, 0) != FAILURE;
}

char *utils_substring(const char *text, size_t start, size_t length) {
    char *copy;

    if (text == NULL) {
        return NULL;
    }

    copy = (char *)malloc(length + 1);
    if (copy == NULL) {
        fprintf(stderr, "Error: Failed to allocate memory.\n");
        return NULL;
    }

    memcpy(copy, text + start, length);
    copy[length] = '\0';
    return copy;
}
