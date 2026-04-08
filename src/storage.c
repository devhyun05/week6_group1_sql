#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#else
#define _POSIX_C_SOURCE 200809L
#endif

#include "storage.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

static int storage_ensure_data_dir(void) {
    struct stat info;

    if (stat("data", &info) == 0) {
        if (S_ISDIR(info.st_mode)) {
            return SUCCESS;
        }
        fprintf(stderr, "Error: 'data' exists but is not a directory.\n");
        return FAILURE;
    }

    if (mkdir("data", 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "Error: Failed to create data directory.\n");
        return FAILURE;
    }

    return SUCCESS;
}

static int storage_build_path(const char *table_name, char *path, size_t path_size) {
    int written;

    if (table_name == NULL || path == NULL || path_size == 0) {
        return FAILURE;
    }

    written = snprintf(path, path_size, "data/%s.csv", table_name);
    if (written < 0 || (size_t)written >= path_size) {
        return FAILURE;
    }

    return SUCCESS;
}

static int storage_lock_file(FILE *fp, int operation) {
    int fd;

    if (fp == NULL) {
        return FAILURE;
    }

    fd = fileno(fp);
    if (fd < 0 || flock(fd, operation) != 0) {
        fprintf(stderr, "Error: Failed to lock table file.\n");
        return FAILURE;
    }

    return SUCCESS;
}

static int storage_append_char(char **buffer, size_t *length, size_t *capacity,
                               char value) {
    char *new_buffer;

    if (*buffer == NULL) {
        *capacity = 64;
        *buffer = (char *)malloc(*capacity);
        if (*buffer == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            return FAILURE;
        }
        *length = 0;
        (*buffer)[0] = '\0';
    } else if (*length + 2 > *capacity) {
        *capacity *= 2;
        new_buffer = (char *)realloc(*buffer, *capacity);
        if (new_buffer == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            return FAILURE;
        }
        *buffer = new_buffer;
    }

    (*buffer)[(*length)++] = value;
    (*buffer)[*length] = '\0';
    return SUCCESS;
}

static int storage_append_field(char ***fields, int *count, int *capacity,
                                const char *value) {
    char **new_fields;
    char *copy;

    if (*fields == NULL) {
        *capacity = 8;
        *fields = (char **)malloc((size_t)(*capacity) * sizeof(char *));
        if (*fields == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            return FAILURE;
        }
    } else if (*count >= *capacity) {
        *capacity *= 2;
        new_fields = (char **)realloc(*fields, (size_t)(*capacity) * sizeof(char *));
        if (new_fields == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            return FAILURE;
        }
        *fields = new_fields;
    }

    copy = utils_strdup(value);
    if (copy == NULL) {
        return FAILURE;
    }

    (*fields)[*count] = copy;
    (*count)++;
    return SUCCESS;
}

static void storage_free_field_list(char **fields, int count) {
    int i;

    if (fields == NULL) {
        return;
    }

    for (i = 0; i < count; i++) {
        free(fields[i]);
        fields[i] = NULL;
    }
    free(fields);
}

static int storage_parse_csv_line(const char *line, char ***out_fields,
                                  int *out_count) {
    char **fields;
    int field_count;
    int field_capacity;
    char *current_field;
    size_t current_length;
    size_t current_capacity;
    int in_quotes;
    size_t i;

    if (line == NULL || out_fields == NULL || out_count == NULL) {
        return FAILURE;
    }

    fields = NULL;
    field_count = 0;
    field_capacity = 0;
    current_field = NULL;
    current_length = 0;
    current_capacity = 0;
    in_quotes = 0;

    for (i = 0;; i++) {
        char current = line[i];
        int at_end = (current == '\0' || current == '\n' || current == '\r');

        if (in_quotes) {
            if (current == '"' && line[i + 1] == '"') {
                if (storage_append_char(&current_field, &current_length,
                                        &current_capacity, '"') != SUCCESS) {
                    storage_free_field_list(fields, field_count);
                    free(current_field);
                    return FAILURE;
                }
                i++;
                continue;
            }

            if (current == '"') {
                in_quotes = 0;
                continue;
            }

            if (at_end) {
                fprintf(stderr, "Error: Malformed CSV line.\n");
                storage_free_field_list(fields, field_count);
                free(current_field);
                return FAILURE;
            }

            if (storage_append_char(&current_field, &current_length,
                                    &current_capacity, current) != SUCCESS) {
                storage_free_field_list(fields, field_count);
                free(current_field);
                return FAILURE;
            }
            continue;
        }

        if (current == '"') {
            in_quotes = 1;
            continue;
        }

        if (current == ',' || at_end) {
            if (current_field == NULL) {
                current_field = utils_strdup("");
                if (current_field == NULL) {
                    storage_free_field_list(fields, field_count);
                    return FAILURE;
                }
            }

            if (storage_append_field(&fields, &field_count, &field_capacity,
                                     current_field) != SUCCESS) {
                storage_free_field_list(fields, field_count);
                free(current_field);
                return FAILURE;
            }

            free(current_field);
            current_field = NULL;
            current_length = 0;
            current_capacity = 0;

            if (at_end) {
                break;
            }
            continue;
        }

        if (storage_append_char(&current_field, &current_length,
                                &current_capacity, current) != SUCCESS) {
            storage_free_field_list(fields, field_count);
            free(current_field);
            return FAILURE;
        }
    }

    *out_fields = fields;
    *out_count = field_count;
    return SUCCESS;
}

static int storage_copy_columns(char columns[][MAX_IDENTIFIER_LEN], int col_count,
                                char **parsed_columns, int parsed_count) {
    int i;

    if (col_count < 0 || parsed_columns == NULL || parsed_count != col_count) {
        return FAILURE;
    }

    for (i = 0; i < col_count; i++) {
        if (utils_safe_strcpy(columns[i], sizeof(columns[i]),
                              parsed_columns[i]) != SUCCESS) {
            fprintf(stderr, "Error: Column name is too long.\n");
            return FAILURE;
        }
    }

    return SUCCESS;
}

static int storage_find_column_index(const char columns[][MAX_IDENTIFIER_LEN],
                                     int col_count, const char *target) {
    int i;

    if (columns == NULL || target == NULL) {
        return FAILURE;
    }

    for (i = 0; i < col_count; i++) {
        if (utils_equals_ignore_case(columns[i], target)) {
            return i;
        }
    }

    return FAILURE;
}

static int storage_validate_primary_key(FILE *fp, const char *table_name,
                                        const char columns[][MAX_IDENTIFIER_LEN],
                                        int col_count,
                                        const char *ordered_values[]) {
    int id_index;
    char line[MAX_CSV_LINE_LENGTH];
    char **parsed_fields;
    int parsed_count;

    if (fp == NULL || table_name == NULL || columns == NULL || ordered_values == NULL) {
        return FAILURE;
    }

    id_index = storage_find_column_index(columns, col_count, "id");
    if (id_index == FAILURE) {
        return SUCCESS;
    }

    if (ordered_values[id_index] == NULL || ordered_values[id_index][0] == '\0') {
        fprintf(stderr, "Error: Primary key column 'id' cannot be empty.\n");
        return FAILURE;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        if (strchr(line, '\n') == NULL && !feof(fp)) {
            fprintf(stderr, "Error: CSV row is too long.\n");
            return FAILURE;
        }

        if (line[0] == '\n' || line[0] == '\r') {
            continue;
        }

        if (storage_parse_csv_line(line, &parsed_fields, &parsed_count) != SUCCESS) {
            return FAILURE;
        }

        if (parsed_count != col_count) {
            fprintf(stderr, "Error: Corrupted row in table '%s'.\n", table_name);
            storage_free_field_list(parsed_fields, parsed_count);
            return FAILURE;
        }

        if (strcmp(parsed_fields[id_index], ordered_values[id_index]) == 0) {
            fprintf(stderr,
                    "Error: Duplicate primary key value '%s' for column 'id'.\n",
                    ordered_values[id_index]);
            storage_free_field_list(parsed_fields, parsed_count);
            return FAILURE;
        }

        storage_free_field_list(parsed_fields, parsed_count);
    }

    return SUCCESS;
}

static int storage_get_next_auto_id(FILE *fp, const char *table_name,
                                    const char columns[][MAX_IDENTIFIER_LEN],
                                    int col_count, char *buffer,
                                    size_t buffer_size) {
    int id_index;
    char line[MAX_CSV_LINE_LENGTH];
    char **parsed_fields;
    int parsed_count;
    long long next_id;
    long long current_id;

    if (fp == NULL || table_name == NULL || columns == NULL || buffer == NULL ||
        buffer_size == 0) {
        return FAILURE;
    }

    id_index = storage_find_column_index(columns, col_count, "id");
    if (id_index == FAILURE) {
        fprintf(stderr, "Error: Auto-increment requires an 'id' column.\n");
        return FAILURE;
    }

    next_id = 1;
    while (fgets(line, sizeof(line), fp) != NULL) {
        if (strchr(line, '\n') == NULL && !feof(fp)) {
            fprintf(stderr, "Error: CSV row is too long.\n");
            return FAILURE;
        }

        if (line[0] == '\n' || line[0] == '\r') {
            continue;
        }

        if (storage_parse_csv_line(line, &parsed_fields, &parsed_count) != SUCCESS) {
            return FAILURE;
        }

        if (parsed_count != col_count) {
            fprintf(stderr, "Error: Corrupted row in table '%s'.\n", table_name);
            storage_free_field_list(parsed_fields, parsed_count);
            return FAILURE;
        }

        if (!utils_is_integer(parsed_fields[id_index])) {
            fprintf(stderr,
                    "Error: Auto-increment requires integer values in column 'id'.\n");
            storage_free_field_list(parsed_fields, parsed_count);
            return FAILURE;
        }

        current_id = utils_parse_integer(parsed_fields[id_index]);
        if (current_id >= next_id) {
            next_id = current_id + 1;
        }

        storage_free_field_list(parsed_fields, parsed_count);
    }

    if (snprintf(buffer, buffer_size, "%lld", next_id) < 0) {
        return FAILURE;
    }

    return SUCCESS;
}

static int storage_write_csv_value(FILE *fp, const char *value) {
    size_t i;
    int needs_quotes;

    if (fp == NULL || value == NULL) {
        return FAILURE;
    }

    needs_quotes = 0;
    for (i = 0; value[i] != '\0'; i++) {
        if (value[i] == ',' || value[i] == '"' || value[i] == '\n' ||
            value[i] == '\r') {
            needs_quotes = 1;
            break;
        }
    }

    if (!needs_quotes) {
        if (fputs(value, fp) == EOF) {
            return FAILURE;
        }
        return SUCCESS;
    }

    if (fputc('"', fp) == EOF) {
        return FAILURE;
    }

    for (i = 0; value[i] != '\0'; i++) {
        if (value[i] == '"') {
            if (fputc('"', fp) == EOF || fputc('"', fp) == EOF) {
                return FAILURE;
            }
        } else if (fputc(value[i], fp) == EOF) {
            return FAILURE;
        }
    }

    if (fputc('"', fp) == EOF) {
        return FAILURE;
    }

    return SUCCESS;
}

static int storage_write_csv_row(FILE *fp, const char **values, int count) {
    int i;

    if (fp == NULL || values == NULL || count < 0) {
        return FAILURE;
    }

    for (i = 0; i < count; i++) {
        if (i > 0 && fputc(',', fp) == EOF) {
            return FAILURE;
        }

        if (storage_write_csv_value(fp, values[i]) != SUCCESS) {
            return FAILURE;
        }
    }

    if (fputc('\n', fp) == EOF) {
        return FAILURE;
    }

    return SUCCESS;
}

static int storage_read_header(FILE *fp, char columns[][MAX_IDENTIFIER_LEN],
                               int *col_count) {
    char line[MAX_CSV_LINE_LENGTH];
    char **parsed_columns;
    int parsed_count;
    int status;

    if (fp == NULL || columns == NULL || col_count == NULL) {
        return FAILURE;
    }

    if (fgets(line, sizeof(line), fp) == NULL) {
        *col_count = 0;
        return SUCCESS;
    }

    if (strchr(line, '\n') == NULL && !feof(fp)) {
        fprintf(stderr, "Error: CSV header is too long.\n");
        return FAILURE;
    }

    if (storage_parse_csv_line(line, &parsed_columns, &parsed_count) != SUCCESS) {
        return FAILURE;
    }

    if (parsed_count > MAX_COLUMNS) {
        fprintf(stderr, "Error: Too many columns in table header.\n");
        storage_free_field_list(parsed_columns, parsed_count);
        return FAILURE;
    }

    status = storage_copy_columns(columns, parsed_count, parsed_columns, parsed_count);
    storage_free_field_list(parsed_columns, parsed_count);
    if (status != SUCCESS) {
        return FAILURE;
    }

    *col_count = parsed_count;
    return SUCCESS;
}

static int storage_load_table_internal(const char *table_name, TableData *table,
                                       int include_offsets) {
    FILE *fp;
    char path[MAX_PATH_LEN];
    char line[MAX_CSV_LINE_LENGTH];
    char **parsed_fields;
    int parsed_count;
    int row_capacity;
    long *offsets;
    long *new_offsets;
    char ***new_rows;
    long current_offset;

    if (table_name == NULL || table == NULL) {
        return FAILURE;
    }

    memset(table, 0, sizeof(*table));
    if (storage_build_path(table_name, path, sizeof(path)) != SUCCESS) {
        fprintf(stderr, "Error: Table path is too long.\n");
        return FAILURE;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return FAILURE;
    }

    if (storage_lock_file(fp, LOCK_SH) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    if (storage_read_header(fp, table->columns, &table->col_count) != SUCCESS) {
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
        return FAILURE;
    }

    row_capacity = INITIAL_ROW_CAPACITY;
    offsets = NULL;
    table->rows = NULL;
    if (row_capacity > 0) {
        table->rows = (char ***)malloc((size_t)row_capacity * sizeof(char **));
        if (table->rows == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }
    }

    if (include_offsets) {
        offsets = (long *)malloc((size_t)row_capacity * sizeof(long));
        if (offsets == NULL) {
            fprintf(stderr, "Error: Failed to allocate memory.\n");
            free(table->rows);
            table->rows = NULL;
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }
    }

    while (1) {
        current_offset = ftell(fp);
        if (fgets(line, sizeof(line), fp) == NULL) {
            break;
        }

        if (strchr(line, '\n') == NULL && !feof(fp)) {
            fprintf(stderr, "Error: CSV row is too long.\n");
            storage_free_table(table);
            free(offsets);
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }

        if (line[0] == '\n' || line[0] == '\r') {
            continue;
        }

        if (storage_parse_csv_line(line, &parsed_fields, &parsed_count) != SUCCESS) {
            storage_free_table(table);
            free(offsets);
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }

        if (parsed_count != table->col_count) {
            fprintf(stderr, "Error: Corrupted row in table '%s'.\n", table_name);
            storage_free_field_list(parsed_fields, parsed_count);
            storage_free_table(table);
            free(offsets);
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }

        if (table->row_count >= row_capacity) {
            row_capacity *= 2;
            new_rows = (char ***)realloc(table->rows,
                                         (size_t)row_capacity * sizeof(char **));
            if (new_rows == NULL) {
                fprintf(stderr, "Error: Failed to allocate memory.\n");
                storage_free_field_list(parsed_fields, parsed_count);
                storage_free_table(table);
                free(offsets);
                flock(fileno(fp), LOCK_UN);
                fclose(fp);
                return FAILURE;
            }
            table->rows = new_rows;

            if (include_offsets) {
                new_offsets = (long *)realloc(offsets,
                                              (size_t)row_capacity * sizeof(long));
                if (new_offsets == NULL) {
                    fprintf(stderr, "Error: Failed to allocate memory.\n");
                    storage_free_field_list(parsed_fields, parsed_count);
                    storage_free_table(table);
                    free(offsets);
                    flock(fileno(fp), LOCK_UN);
                    fclose(fp);
                    return FAILURE;
                }
                offsets = new_offsets;
            }
        }

        table->rows[table->row_count] = parsed_fields;
        if (include_offsets) {
            offsets[table->row_count] = current_offset;
        }
        table->row_count++;
    }

    table->offsets = offsets;
    flock(fileno(fp), LOCK_UN);
    fclose(fp);
    return SUCCESS;
}

int storage_insert(const char *table_name, const InsertStatement *stmt) {
    FILE *fp;
    char path[MAX_PATH_LEN];
    char existing_columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    char final_columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int existing_count;
    int final_count;
    int i;
    int match_index;
    int existing_id_index;
    int stmt_id_index;
    const char *ordered_values[MAX_COLUMNS];
    const char *header_values[MAX_COLUMNS];
    char auto_id_value[MAX_VALUE_LEN];
    long file_end;

    if (table_name == NULL || stmt == NULL) {
        return FAILURE;
    }

    if (storage_ensure_data_dir() != SUCCESS) {
        return FAILURE;
    }

    if (storage_build_path(table_name, path, sizeof(path)) != SUCCESS) {
        fprintf(stderr, "Error: Table path is too long.\n");
        return FAILURE;
    }

    fp = fopen(path, "a+");
    if (fp == NULL) {
        fprintf(stderr, "Error: Failed to open table '%s'.\n", table_name);
        return FAILURE;
    }

    if (storage_lock_file(fp, LOCK_EX) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    rewind(fp);
    if (storage_read_header(fp, existing_columns, &existing_count) != SUCCESS) {
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
        return FAILURE;
    }

    if (existing_count == 0) {
        stmt_id_index = storage_find_column_index(
            (const char (*)[MAX_IDENTIFIER_LEN])stmt->columns,
            stmt->column_count, "id");
        final_count = stmt->column_count;

        if (stmt_id_index == FAILURE) {
            if (stmt->column_count + 1 > MAX_COLUMNS) {
                fprintf(stderr, "Error: Too many columns for auto-increment id.\n");
                flock(fileno(fp), LOCK_UN);
                fclose(fp);
                return FAILURE;
            }

            if (utils_safe_strcpy(final_columns[0], sizeof(final_columns[0]), "id") != SUCCESS ||
                utils_safe_strcpy(auto_id_value, sizeof(auto_id_value), "1") != SUCCESS) {
                fprintf(stderr, "Error: Failed to prepare auto-increment id.\n");
                flock(fileno(fp), LOCK_UN);
                fclose(fp);
                return FAILURE;
            }

            header_values[0] = final_columns[0];
            ordered_values[0] = auto_id_value;
            final_count = stmt->column_count + 1;

            for (i = 0; i < stmt->column_count; i++) {
                if (utils_safe_strcpy(final_columns[i + 1], sizeof(final_columns[i + 1]),
                                      stmt->columns[i]) != SUCCESS) {
                    fprintf(stderr, "Error: Column name is too long.\n");
                    flock(fileno(fp), LOCK_UN);
                    fclose(fp);
                    return FAILURE;
                }
                header_values[i + 1] = final_columns[i + 1];
                ordered_values[i + 1] = stmt->values[i];
            }
        } else {
            for (i = 0; i < stmt->column_count; i++) {
                if (utils_safe_strcpy(final_columns[i], sizeof(final_columns[i]),
                                      stmt->columns[i]) != SUCCESS) {
                    fprintf(stderr, "Error: Column name is too long.\n");
                    flock(fileno(fp), LOCK_UN);
                    fclose(fp);
                    return FAILURE;
                }
                header_values[i] = final_columns[i];
                ordered_values[i] = stmt->values[i];
            }
        }

        if (storage_validate_primary_key(fp, table_name, final_columns,
                                         final_count, ordered_values) != SUCCESS) {
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }

        rewind(fp);
        if (storage_write_csv_row(fp, header_values, final_count) != SUCCESS ||
            storage_write_csv_row(fp, ordered_values, final_count) != SUCCESS) {
            fprintf(stderr, "Error: Failed to write table '%s'.\n", table_name);
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }
    } else {
        existing_id_index = storage_find_column_index(existing_columns, existing_count, "id");
        stmt_id_index = storage_find_column_index(
            (const char (*)[MAX_IDENTIFIER_LEN])stmt->columns,
            stmt->column_count, "id");

        if ((existing_id_index == FAILURE && existing_count != stmt->column_count) ||
            (existing_id_index != FAILURE && stmt_id_index != FAILURE &&
             existing_count != stmt->column_count) ||
            (existing_id_index != FAILURE && stmt_id_index == FAILURE &&
             existing_count != stmt->column_count + 1)) {
            fprintf(stderr, "Error: Column count doesn't match table schema.\n");
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }

        for (i = 0; i < existing_count; i++) {
            if (i == existing_id_index && stmt_id_index == FAILURE) {
                continue;
            }

            match_index = storage_find_column_index(
                (const char (*)[MAX_IDENTIFIER_LEN])stmt->columns,
                stmt->column_count, existing_columns[i]);
            if (match_index == FAILURE) {
                fprintf(stderr, "Error: Column '%s' doesn't exist in table schema.\n",
                        existing_columns[i]);
                flock(fileno(fp), LOCK_UN);
                fclose(fp);
                return FAILURE;
            }
        }

        if (existing_id_index != FAILURE && stmt_id_index == FAILURE) {
            if (storage_get_next_auto_id(fp, table_name, existing_columns,
                                         existing_count, auto_id_value,
                                         sizeof(auto_id_value)) != SUCCESS) {
                flock(fileno(fp), LOCK_UN);
                fclose(fp);
                return FAILURE;
            }
        }

        for (i = 0; i < existing_count; i++) {
            if (i == existing_id_index && stmt_id_index == FAILURE) {
                ordered_values[i] = auto_id_value;
                continue;
            }

            match_index = storage_find_column_index(
                (const char (*)[MAX_IDENTIFIER_LEN])stmt->columns,
                stmt->column_count, existing_columns[i]);
            ordered_values[i] = stmt->values[match_index];
        }

        if (!(existing_id_index != FAILURE && stmt_id_index == FAILURE)) {
            if (storage_validate_primary_key(fp, table_name, existing_columns,
                                             existing_count, ordered_values) != SUCCESS) {
                flock(fileno(fp), LOCK_UN);
                fclose(fp);
                return FAILURE;
            }
        }

        if (fseek(fp, 0, SEEK_END) != 0) {
            fprintf(stderr, "Error: Failed to append to table '%s'.\n", table_name);
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }

        file_end = ftell(fp);
        if (file_end < 0) {
            fprintf(stderr, "Error: Failed to append to table '%s'.\n", table_name);
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }

        if (storage_write_csv_row(fp, ordered_values, existing_count) != SUCCESS) {
            fprintf(stderr, "Error: Failed to write table '%s'.\n", table_name);
            flock(fileno(fp), LOCK_UN);
            fclose(fp);
            return FAILURE;
        }
    }

    fflush(fp);
    flock(fileno(fp), LOCK_UN);
    fclose(fp);
    return SUCCESS;
}

char ***storage_select(const char *table_name, int *row_count, int *col_count) {
    TableData table;

    if (row_count == NULL || col_count == NULL) {
        return NULL;
    }

    if (storage_load_table(table_name, &table) != SUCCESS) {
        return NULL;
    }

    *row_count = table.row_count;
    *col_count = table.col_count;
    free(table.offsets);
    table.offsets = NULL;
    return table.rows;
}

int storage_get_columns(const char *table_name, char columns[][MAX_IDENTIFIER_LEN],
                        int *col_count) {
    FILE *fp;
    char path[MAX_PATH_LEN];
    int status;

    if (table_name == NULL || columns == NULL || col_count == NULL) {
        return FAILURE;
    }

    if (storage_build_path(table_name, path, sizeof(path)) != SUCCESS) {
        fprintf(stderr, "Error: Table path is too long.\n");
        return FAILURE;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return FAILURE;
    }

    if (storage_lock_file(fp, LOCK_SH) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    status = storage_read_header(fp, columns, col_count);
    flock(fileno(fp), LOCK_UN);
    fclose(fp);
    return status;
}

int storage_load_table(const char *table_name, TableData *table) {
    return storage_load_table_internal(table_name, table, 1);
}

int storage_read_row_at_offset(const char *table_name, long offset, int expected_col_count,
                               char ***out_row) {
    FILE *fp;
    char path[MAX_PATH_LEN];
    char line[MAX_CSV_LINE_LENGTH];
    char **parsed_fields;
    int parsed_count;

    if (table_name == NULL || out_row == NULL) {
        return FAILURE;
    }

    if (storage_build_path(table_name, path, sizeof(path)) != SUCCESS) {
        fprintf(stderr, "Error: Table path is too long.\n");
        return FAILURE;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return FAILURE;
    }

    if (storage_lock_file(fp, LOCK_SH) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    if (fseek(fp, offset, SEEK_SET) != 0) {
        fprintf(stderr, "Error: Failed to seek row in table '%s'.\n", table_name);
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
        return FAILURE;
    }

    if (fgets(line, sizeof(line), fp) == NULL) {
        fprintf(stderr, "Error: Failed to read row in table '%s'.\n", table_name);
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
        return FAILURE;
    }

    if (storage_parse_csv_line(line, &parsed_fields, &parsed_count) != SUCCESS) {
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
        return FAILURE;
    }

    flock(fileno(fp), LOCK_UN);
    fclose(fp);

    if (expected_col_count >= 0 && parsed_count != expected_col_count) {
        fprintf(stderr, "Error: Corrupted row in table '%s'.\n", table_name);
        storage_free_field_list(parsed_fields, parsed_count);
        return FAILURE;
    }

    *out_row = parsed_fields;
    return SUCCESS;
}

void storage_free_row(char **row, int col_count) {
    int i;

    if (row == NULL) {
        return;
    }

    for (i = 0; i < col_count; i++) {
        free(row[i]);
        row[i] = NULL;
    }
    free(row);
}

void storage_free_rows(char ***rows, int row_count, int col_count) {
    int i;

    if (rows == NULL) {
        return;
    }

    for (i = 0; i < row_count; i++) {
        storage_free_row(rows[i], col_count);
    }

    free(rows);
}

void storage_free_table(TableData *table) {
    if (table == NULL) {
        return;
    }

    storage_free_rows(table->rows, table->row_count, table->col_count);
    table->rows = NULL;
    free(table->offsets);
    table->offsets = NULL;
    table->row_count = 0;
    table->col_count = 0;
}
