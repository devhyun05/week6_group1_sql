#include "storage.h"

#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int ensure_data_directory(void) {
    struct stat st;

    if (stat("data", &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            return SUCCESS;
        }
        fprintf(stderr, "Error: 'data' exists but is not a directory.\n");
        return FAILURE;
    }

    if (mkdir("data", 0755) != 0) {
        fprintf(stderr, "Error: Failed to create data directory.\n");
        return FAILURE;
    }

    return SUCCESS;
}

static void build_table_path(const char *table_name, char *path, size_t path_size) {
    snprintf(path, path_size, "data/%s.csv", table_name);
}

static int lock_file(FILE *fp, int lock_type) {
    if (flock(fileno(fp), lock_type) != 0) {
        fprintf(stderr, "Error: Failed to lock table file.\n");
        return FAILURE;
    }

    return SUCCESS;
}

static int csv_should_quote(const char *value) {
    size_t index;

    for (index = 0; value[index] != '\0'; ++index) {
        if (value[index] == ',' || value[index] == '"' || value[index] == '\n' || value[index] == '\r') {
            return 1;
        }
    }

    return 0;
}

static int write_csv_field(FILE *fp, const char *value) {
    size_t index;

    if (!csv_should_quote(value)) {
        return fprintf(fp, "%s", value) < 0 ? FAILURE : SUCCESS;
    }

    if (fputc('"', fp) == EOF) {
        return FAILURE;
    }

    for (index = 0; value[index] != '\0'; ++index) {
        if (value[index] == '"') {
            if (fputc('"', fp) == EOF || fputc('"', fp) == EOF) {
                return FAILURE;
            }
        } else if (fputc(value[index], fp) == EOF) {
            return FAILURE;
        }
    }

    return fputc('"', fp) == EOF ? FAILURE : SUCCESS;
}

static int write_csv_row(FILE *fp, char **values, int count) {
    int index;

    for (index = 0; index < count; ++index) {
        if (index > 0 && fputc(',', fp) == EOF) {
            return FAILURE;
        }

        if (write_csv_field(fp, values[index]) != SUCCESS) {
            return FAILURE;
        }
    }

    return fputc('\n', fp) == EOF ? FAILURE : SUCCESS;
}

static void free_csv_cells(char **cells, int count) {
    int index;

    if (cells == NULL) {
        return;
    }

    for (index = 0; index < count; ++index) {
        free(cells[index]);
        cells[index] = NULL;
    }

    free(cells);
}

static int push_cell(char ***cells, int *count, int *capacity, const char *value) {
    char **new_cells;
    char *copy;

    if (*count + 1 >= *capacity) {
        *capacity *= 2;
        new_cells = (char **) realloc(*cells, (size_t) (*capacity) * sizeof(char *));
        if (new_cells == NULL) {
            fprintf(stderr, "Error: Memory allocation failed.\n");
            return FAILURE;
        }
        *cells = new_cells;
    }

    copy = utils_strdup(value);
    if (copy == NULL) {
        return FAILURE;
    }

    (*cells)[*count] = copy;
    (*count)++;
    (*cells)[*count] = NULL;
    return SUCCESS;
}

static int parse_csv_line(const char *line, char ***out_cells, int *out_count) {
    int capacity = INITIAL_DYNAMIC_CAPACITY;
    int count = 0;
    int in_quotes = 0;
    char field[MAX_LINE_LENGTH];
    size_t field_index = 0;
    size_t index;
    char **cells = NULL;

    if (out_cells == NULL || out_count == NULL) {
        return FAILURE;
    }

    cells = (char **) calloc((size_t) capacity, sizeof(char *));
    if (cells == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }

    for (index = 0; line[index] != '\0'; ++index) {
        if (!in_quotes && (line[index] == '\n' || line[index] == '\r')) {
            break;
        }

        if (line[index] == '"') {
            if (in_quotes && line[index + 1] == '"') {
                if (field_index + 1 >= sizeof(field)) {
                    fprintf(stderr, "Error: CSV field is too long.\n");
                    free_csv_cells(cells, count);
                    return FAILURE;
                }
                field[field_index++] = '"';
                index++;
                continue;
            }

            in_quotes = !in_quotes;
            continue;
        }

        if (!in_quotes && line[index] == ',') {
            field[field_index] = '\0';
            if (push_cell(&cells, &count, &capacity, field) != SUCCESS) {
                free_csv_cells(cells, count);
                return FAILURE;
            }
            field_index = 0;
            continue;
        }

        if (field_index + 1 >= sizeof(field)) {
            fprintf(stderr, "Error: CSV field is too long.\n");
            free_csv_cells(cells, count);
            return FAILURE;
        }
        field[field_index++] = line[index];
    }

    if (in_quotes) {
        fprintf(stderr, "Error: Unterminated quoted CSV field.\n");
        free_csv_cells(cells, count);
        return FAILURE;
    }

    field[field_index] = '\0';
    if (push_cell(&cells, &count, &capacity, field) != SUCCESS) {
        free_csv_cells(cells, count);
        return FAILURE;
    }

    *out_cells = cells;
    *out_count = count;
    return SUCCESS;
}

static int copy_columns_from_cells(char columns[][MAX_IDENTIFIER_LEN], char **cells, int count) {
    int index;

    if (count > MAX_COLUMNS) {
        fprintf(stderr, "Error: Too many columns in table header.\n");
        return FAILURE;
    }

    for (index = 0; index < count; ++index) {
        utils_safe_strcpy(columns[index], MAX_IDENTIFIER_LEN, cells[index]);
    }

    return SUCCESS;
}

static int find_column_index(char columns[][MAX_IDENTIFIER_LEN], int col_count, const char *target) {
    int index;

    for (index = 0; index < col_count; ++index) {
        if (utils_equals_ignore_case(columns[index], target)) {
            return index;
        }
    }

    return -1;
}

static int map_insert_values(
    char **mapped_values,
    char header_columns[][MAX_IDENTIFIER_LEN],
    int header_count,
    const InsertStatement *stmt
) {
    int header_index;
    int insert_index;

    if (header_count != stmt->column_count) {
        fprintf(stderr, "Error: Column count doesn't match value count.\n");
        return FAILURE;
    }

    for (header_index = 0; header_index < header_count; ++header_index) {
        insert_index = find_column_index((char (*)[MAX_IDENTIFIER_LEN]) stmt->columns, stmt->column_count, header_columns[header_index]);
        if (insert_index < 0) {
            fprintf(stderr, "Error: Missing column '%s' in INSERT statement.\n", header_columns[header_index]);
            return FAILURE;
        }

        mapped_values[header_index] = (char *) stmt->values[insert_index];
    }

    return SUCCESS;
}

static int find_primary_key_index(char columns[][MAX_IDENTIFIER_LEN], int col_count) {
    return find_column_index(columns, col_count, "id");
}

static int check_duplicate_primary_key(
    FILE *fp,
    const char *table_name,
    int primary_key_index,
    const char *primary_key_value
) {
    char line[MAX_LINE_LENGTH];
    char **cells = NULL;
    int cell_count = 0;
    int result = SUCCESS;

    if (primary_key_value == NULL || primary_key_value[0] == '\0') {
        fprintf(stderr, "Error: Primary key column 'id' cannot be empty in table '%s'.\n", table_name);
        return FAILURE;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        if (line[0] == '\n' || line[0] == '\r' || line[0] == '\0') {
            continue;
        }

        if (parse_csv_line(line, &cells, &cell_count) != SUCCESS) {
            return FAILURE;
        }

        if (cell_count <= primary_key_index) {
            fprintf(stderr, "Error: Corrupted row in table '%s'.\n", table_name);
            free_csv_cells(cells, cell_count);
            return FAILURE;
        }

        if (utils_compare_values(cells[primary_key_index], primary_key_value) == 0) {
            fprintf(stderr, "Error: Duplicate primary key '%s' in table '%s'.\n", primary_key_value, table_name);
            result = FAILURE;
        }

        free_csv_cells(cells, cell_count);
        cells = NULL;
        cell_count = 0;

        if (result != SUCCESS) {
            return FAILURE;
        }
    }

    return SUCCESS;
}

static int append_loaded_row(StorageTable *table, char **cells, long offset) {
    char ***new_rows;
    long *new_offsets;

    new_rows = (char ***) realloc(table->rows, (size_t) (table->row_count + 1) * sizeof(char **));
    if (new_rows == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }
    table->rows = new_rows;

    new_offsets = (long *) realloc(table->offsets, (size_t) (table->row_count + 1) * sizeof(long));
    if (new_offsets == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }
    table->offsets = new_offsets;

    table->rows[table->row_count] = cells;
    table->offsets[table->row_count] = offset;
    table->row_count++;
    return SUCCESS;
}

int storage_insert(const char *table_name, const InsertStatement *stmt) {
    FILE *fp = NULL;
    char path[MAX_PATH_LENGTH];
    char line[MAX_LINE_LENGTH];
    char **header_cells = NULL;
    char fixed_columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    char *header_row_values[MAX_COLUMNS];
    char *mapped_values[MAX_COLUMNS];
    int header_count = 0;
    int primary_key_index = -1;
    int is_empty = 0;
    int result = FAILURE;
    int index;

    if (table_name == NULL || stmt == NULL) {
        return FAILURE;
    }

    if (ensure_data_directory() != SUCCESS) {
        return FAILURE;
    }

    build_table_path(table_name, path, sizeof(path));
    fp = fopen(path, "a+");
    if (fp == NULL) {
        fprintf(stderr, "Error: Failed to open table '%s'.\n", table_name);
        return FAILURE;
    }

    if (lock_file(fp, LOCK_EX) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    rewind(fp);
    if (fgets(line, sizeof(line), fp) == NULL) {
        is_empty = 1;
        header_count = stmt->column_count;
        for (index = 0; index < header_count; ++index) {
            utils_safe_strcpy(fixed_columns[index], MAX_IDENTIFIER_LEN, stmt->columns[index]);
            header_row_values[index] = fixed_columns[index];
        }
    } else {
        if (parse_csv_line(line, &header_cells, &header_count) != SUCCESS) {
            goto cleanup;
        }

        if (copy_columns_from_cells(fixed_columns, header_cells, header_count) != SUCCESS) {
            goto cleanup;
        }

        for (index = 0; index < header_count; ++index) {
            header_row_values[index] = fixed_columns[index];
        }
    }

    if (map_insert_values(mapped_values, fixed_columns, header_count, stmt) != SUCCESS) {
        goto cleanup;
    }

    primary_key_index = find_primary_key_index(fixed_columns, header_count);
    if (primary_key_index >= 0) {
        if (mapped_values[primary_key_index] == NULL || mapped_values[primary_key_index][0] == '\0') {
            fprintf(stderr, "Error: Primary key column 'id' cannot be empty in table '%s'.\n", table_name);
            goto cleanup;
        }
    }

    if (primary_key_index >= 0 && !is_empty) {
        if (check_duplicate_primary_key(fp, table_name, primary_key_index, mapped_values[primary_key_index]) != SUCCESS) {
            goto cleanup;
        }
    }

    if (is_empty) {
        clearerr(fp);
        if (fseek(fp, 0, SEEK_END) != 0) {
            fprintf(stderr, "Error: Failed to seek table '%s'.\n", table_name);
            goto cleanup;
        }
        if (write_csv_row(fp, header_row_values, header_count) != SUCCESS) {
            fprintf(stderr, "Error: Failed to write header for table '%s'.\n", table_name);
            goto cleanup;
        }
    } else if (fseek(fp, 0, SEEK_END) != 0) {
        fprintf(stderr, "Error: Failed to seek table '%s'.\n", table_name);
        goto cleanup;
    }

    if (write_csv_row(fp, mapped_values, header_count) != SUCCESS) {
        fprintf(stderr, "Error: Failed to write row to table '%s'.\n", table_name);
        goto cleanup;
    }

    if (fflush(fp) != 0) {
        fprintf(stderr, "Error: Failed to flush table '%s'.\n", table_name);
        goto cleanup;
    }

    result = SUCCESS;

cleanup:
    free_csv_cells(header_cells, header_count);
    if (fp != NULL) {
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
    }
    return result;
}

int storage_get_columns(const char *table_name, char columns[][MAX_IDENTIFIER_LEN], int *col_count) {
    FILE *fp = NULL;
    char path[MAX_PATH_LENGTH];
    char line[MAX_LINE_LENGTH];
    char **cells = NULL;
    int count = 0;
    int result = FAILURE;

    if (col_count == NULL) {
        return FAILURE;
    }

    build_table_path(table_name, path, sizeof(path));
    fp = fopen(path, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return FAILURE;
    }

    if (lock_file(fp, LOCK_SH) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    if (fgets(line, sizeof(line), fp) == NULL) {
        fprintf(stderr, "Error: Table '%s' is empty.\n", table_name);
        goto cleanup;
    }

    if (parse_csv_line(line, &cells, &count) != SUCCESS) {
        goto cleanup;
    }

    if (copy_columns_from_cells(columns, cells, count) != SUCCESS) {
        goto cleanup;
    }

    *col_count = count;
    result = SUCCESS;

cleanup:
    free_csv_cells(cells, count);
    if (fp != NULL) {
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
    }
    return result;
}

int storage_load_table(const char *table_name, StorageTable *out) {
    FILE *fp = NULL;
    char path[MAX_PATH_LENGTH];
    char line[MAX_LINE_LENGTH];
    char **header_cells = NULL;
    char **row_cells = NULL;
    long row_offset;
    int header_count = 0;
    int row_col_count = 0;
    int result = FAILURE;

    if (out == NULL) {
        return FAILURE;
    }

    memset(out, 0, sizeof(*out));
    build_table_path(table_name, path, sizeof(path));
    fp = fopen(path, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return FAILURE;
    }

    if (lock_file(fp, LOCK_SH) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    if (fgets(line, sizeof(line), fp) == NULL) {
        fprintf(stderr, "Error: Table '%s' is empty.\n", table_name);
        goto cleanup;
    }

    if (parse_csv_line(line, &header_cells, &header_count) != SUCCESS) {
        goto cleanup;
    }

    if (copy_columns_from_cells(out->columns, header_cells, header_count) != SUCCESS) {
        goto cleanup;
    }
    out->col_count = header_count;

    while (1) {
        row_offset = ftell(fp);
        if (fgets(line, sizeof(line), fp) == NULL) {
            break;
        }

        if (line[0] == '\n' || line[0] == '\r' || line[0] == '\0') {
            continue;
        }

        if (parse_csv_line(line, &row_cells, &row_col_count) != SUCCESS) {
            goto cleanup;
        }

        if (row_col_count != out->col_count) {
            fprintf(stderr, "Error: Corrupted row in table '%s'.\n", table_name);
            free_csv_cells(row_cells, row_col_count);
            row_cells = NULL;
            goto cleanup;
        }

        if (append_loaded_row(out, row_cells, row_offset) != SUCCESS) {
            free_csv_cells(row_cells, row_col_count);
            row_cells = NULL;
            goto cleanup;
        }

        row_cells = NULL;
    }

    result = SUCCESS;

cleanup:
    free_csv_cells(header_cells, header_count);
    if (fp != NULL) {
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
    }
    if (result != SUCCESS) {
        storage_free_table(out);
    }
    return result;
}

char ***storage_select(const char *table_name, int *row_count, int *col_count) {
    StorageTable table;
    char ***rows;

    if (row_count == NULL || col_count == NULL) {
        return NULL;
    }

    if (storage_load_table(table_name, &table) != SUCCESS) {
        return NULL;
    }

    *row_count = table.row_count;
    *col_count = table.col_count;
    rows = table.rows;
    free(table.offsets);
    table.offsets = NULL;
    table.rows = NULL;
    storage_free_table(&table);
    return rows;
}

int storage_fetch_rows_by_offsets(
    const char *table_name,
    const long *offsets,
    int offset_count,
    int expected_col_count,
    char ****out_rows
) {
    FILE *fp = NULL;
    char path[MAX_PATH_LENGTH];
    char line[MAX_LINE_LENGTH];
    char ***rows = NULL;
    char **cells = NULL;
    int index;
    int col_count = 0;
    int result = FAILURE;

    if (out_rows == NULL) {
        return FAILURE;
    }

    *out_rows = NULL;
    if (offset_count == 0) {
        return SUCCESS;
    }

    build_table_path(table_name, path, sizeof(path));
    fp = fopen(path, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return FAILURE;
    }

    if (lock_file(fp, LOCK_SH) != SUCCESS) {
        fclose(fp);
        return FAILURE;
    }

    rows = (char ***) calloc((size_t) offset_count, sizeof(char **));
    if (rows == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        goto cleanup;
    }

    for (index = 0; index < offset_count; ++index) {
        if (fseek(fp, offsets[index], SEEK_SET) != 0) {
            fprintf(stderr, "Error: Failed to seek table '%s'.\n", table_name);
            goto cleanup;
        }

        if (fgets(line, sizeof(line), fp) == NULL) {
            fprintf(stderr, "Error: Failed to read row from table '%s'.\n", table_name);
            goto cleanup;
        }

        if (parse_csv_line(line, &cells, &col_count) != SUCCESS) {
            goto cleanup;
        }

        if (col_count != expected_col_count) {
            fprintf(stderr, "Error: Corrupted row in table '%s'.\n", table_name);
            free_csv_cells(cells, col_count);
            cells = NULL;
            goto cleanup;
        }

        rows[index] = cells;
        cells = NULL;
    }

    *out_rows = rows;
    result = SUCCESS;

cleanup:
    if (result != SUCCESS) {
        storage_free_rows(rows, offset_count, expected_col_count);
    }
    free_csv_cells(cells, col_count);
    if (fp != NULL) {
        flock(fileno(fp), LOCK_UN);
        fclose(fp);
    }
    return result;
}

void storage_free_rows(char ***rows, int row_count, int col_count) {
    int row_index;

    if (rows == NULL) {
        return;
    }

    for (row_index = 0; row_index < row_count; ++row_index) {
        if (rows[row_index] == NULL) {
            continue;
        }

        free_csv_cells(rows[row_index], col_count);
        rows[row_index] = NULL;
    }

    free(rows);
}

void storage_free_table(StorageTable *table) {
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
