#ifndef UTILS_H
#define UTILS_H

#include <stddef.h>

/**
 * Duplicate a string with heap allocation.
 * Returns a newly allocated string that the caller must free.
 */
char *utils_strdup(const char *src);

/**
 * Copy a string into a fixed-size destination buffer safely.
 */
void utils_safe_strcpy(char *dest, size_t dest_size, const char *src);

/**
 * Remove leading and trailing ASCII whitespace from a mutable string.
 */
void utils_trim_in_place(char *value);

/**
 * Return a heap-allocated trimmed copy of the given string.
 * The caller must free the returned buffer.
 */
char *utils_trim_copy(const char *value);

/**
 * Convert a string to uppercase in place.
 */
void utils_to_upper_in_place(char *value);

/**
 * Case-insensitive string equality for ASCII SQL keywords.
 */
int utils_equals_ignore_case(const char *left, const char *right);

/**
 * Check whether a string represents an integer.
 */
int utils_is_integer(const char *value);

/**
 * Compare two SQL literal strings.
 * Numeric strings are compared numerically when both sides are integers.
 */
int utils_compare_values(const char *left, const char *right);

/**
 * Append text to a dynamic string buffer.
 * Returns SUCCESS on success, FAILURE on allocation error.
 */
int utils_append_text(char **buffer, size_t *capacity, const char *text);

/**
 * Return non-zero when the buffer contains a semicolon outside quotes.
 */
int utils_statement_complete(const char *sql);

/**
 * Split only complete SQL statements by semicolons outside quotes.
 * Returned statements must be freed with utils_free_string_array().
 * The remainder buffer must be freed by the caller.
 */
int utils_split_complete_sql_statements(
    const char *sql,
    char ***statements,
    int *statement_count,
    char **remainder
);

/**
 * Split SQL into statements. A trailing statement without a semicolon is kept.
 * Returned statements must be freed with utils_free_string_array().
 */
int utils_split_sql_statements(const char *sql, char ***statements, int *statement_count);

/**
 * Free an array of heap-allocated strings.
 */
void utils_free_string_array(char **strings, int count);

/**
 * Read an entire file into memory.
 * Returns a heap-allocated buffer that the caller must free.
 */
char *utils_read_file(const char *path);

/**
 * Sort an array of long integers in ascending order.
 */
void utils_sort_longs(long *values, int count);

#endif
