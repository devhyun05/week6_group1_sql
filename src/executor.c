#include "executor.h"

#include "index.h"
#include "storage.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int find_column_index(const char columns[][MAX_IDENTIFIER_LEN], int col_count, const char *column) {
    int index;

    for (index = 0; index < col_count; ++index) {
        if (utils_equals_ignore_case(columns[index], column)) {
            return index;
        }
    }

    return -1;
}

static void print_border(const int *widths, int col_count) {
    int col_index;
    int dash_index;

    for (col_index = 0; col_index < col_count; ++col_index) {
        printf("+");
        for (dash_index = 0; dash_index < widths[col_index] + 2; ++dash_index) {
            printf("-");
        }
    }
    printf("+\n");
}

static void print_header_row(char columns[][MAX_IDENTIFIER_LEN], const int *projection, int col_count, const int *widths) {
    int col_index;

    for (col_index = 0; col_index < col_count; ++col_index) {
        printf("| %-*s ", widths[col_index], columns[projection[col_index]]);
    }
    printf("|\n");
}

static void print_data_row(char **row, const int *projection, int col_count, const int *widths) {
    int col_index;
    const char *value;

    for (col_index = 0; col_index < col_count; ++col_index) {
        value = row[projection[col_index]];
        if (utils_is_integer(value)) {
            printf("| %*s ", widths[col_index], value);
        } else {
            printf("| %-*s ", widths[col_index], value);
        }
    }
    printf("|\n");
}

static void print_select_result(
    char columns[][MAX_IDENTIFIER_LEN],
    int col_count,
    const int *projection,
    int projection_count,
    char ***rows,
    int row_count
) {
    int widths[MAX_COLUMNS];
    int row_index;
    int col_index;
    size_t cell_length;

    (void) col_count;

    for (col_index = 0; col_index < projection_count; ++col_index) {
        widths[col_index] = (int) strlen(columns[projection[col_index]]);
    }

    for (row_index = 0; row_index < row_count; ++row_index) {
        for (col_index = 0; col_index < projection_count; ++col_index) {
            cell_length = strlen(rows[row_index][projection[col_index]]);
            if ((int) cell_length > widths[col_index]) {
                widths[col_index] = (int) cell_length;
            }
        }
    }

    print_border(widths, projection_count);
    print_header_row(columns, projection, projection_count, widths);
    print_border(widths, projection_count);
    for (row_index = 0; row_index < row_count; ++row_index) {
        print_data_row(rows[row_index], projection, projection_count, widths);
    }
    print_border(widths, projection_count);
    printf("%d %s selected.\n", row_count, row_count == 1 ? "row" : "rows");
}

static int resolve_projection(const SelectStatement *stmt, const StorageTable *table, int *projection, int *projection_count) {
    int column_index;
    int index;

    if (stmt->column_count == 0) {
        *projection_count = table->col_count;
        for (index = 0; index < table->col_count; ++index) {
            projection[index] = index;
        }
        return SUCCESS;
    }

    *projection_count = stmt->column_count;
    for (index = 0; index < stmt->column_count; ++index) {
        column_index = find_column_index(table->columns, table->col_count, stmt->columns[index]);
        if (column_index < 0) {
            fprintf(stderr, "Error: Column '%s' not found in table '%s'.\n", stmt->columns[index], stmt->table_name);
            return FAILURE;
        }
        projection[index] = column_index;
    }

    return SUCCESS;
}

static int evaluate_where(const SelectStatement *stmt, const StorageTable *table, long **offsets, int *match_count) {
    int where_column;
    EqualityIndex equality_index;
    RangeIndex range_index;
    int prefer_numeric;

    where_column = find_column_index(table->columns, table->col_count, stmt->where.column);
    if (where_column < 0) {
        fprintf(stderr, "Error: Column '%s' not found in table '%s'.\n", stmt->where.column, stmt->table_name);
        return FAILURE;
    }

    if (strcmp(stmt->where.op, "=") == 0) {
        if (index_build_equality(table->rows, table->offsets, table->row_count, where_column, &equality_index) != SUCCESS) {
            return FAILURE;
        }

        if (index_query_equals(&equality_index, stmt->where.value, offsets, match_count) != SUCCESS) {
            index_free_equality(&equality_index);
            return FAILURE;
        }

        index_free_equality(&equality_index);
        return SUCCESS;
    }

    prefer_numeric = utils_is_integer(stmt->where.value);
    if (index_build_range(table->rows, table->offsets, table->row_count, where_column, prefer_numeric, &range_index) != SUCCESS) {
        return FAILURE;
    }

    if (index_query_range(&range_index, stmt->where.op, stmt->where.value, offsets, match_count) != SUCCESS) {
        index_free_range(&range_index);
        return FAILURE;
    }

    index_free_range(&range_index);
    return SUCCESS;
}

static int execute_insert(const InsertStatement *stmt) {
    if (storage_insert(stmt->table_name, stmt) != SUCCESS) {
        return FAILURE;
    }

    printf("1 row inserted into %s.\n", stmt->table_name);
    return SUCCESS;
}

static int execute_select(const SelectStatement *stmt) {
    StorageTable table;
    char ***selected_rows = NULL;
    long *offsets = NULL;
    int projection[MAX_COLUMNS];
    int projection_count = 0;
    int match_count = 0;
    int result = FAILURE;

    if (storage_load_table(stmt->table_name, &table) != SUCCESS) {
        return FAILURE;
    }

    if (resolve_projection(stmt, &table, projection, &projection_count) != SUCCESS) {
        goto cleanup;
    }

    if (!stmt->has_where) {
        print_select_result(table.columns, table.col_count, projection, projection_count, table.rows, table.row_count);
        result = SUCCESS;
        goto cleanup;
    }

    if (evaluate_where(stmt, &table, &offsets, &match_count) != SUCCESS) {
        goto cleanup;
    }

    utils_sort_longs(offsets, match_count);
    if (storage_fetch_rows_by_offsets(stmt->table_name, offsets, match_count, table.col_count, &selected_rows) != SUCCESS) {
        goto cleanup;
    }

    print_select_result(table.columns, table.col_count, projection, projection_count, selected_rows, match_count);
    result = SUCCESS;

cleanup:
    storage_free_rows(selected_rows, match_count, table.col_count);
    free(offsets);
    offsets = NULL;
    storage_free_table(&table);
    return result;
}

int executor_execute(const SqlStatement *stmt) {
    if (stmt == NULL) {
        return FAILURE;
    }

    if (stmt->type == SQL_INSERT) {
        return execute_insert(&stmt->insert);
    }

    if (stmt->type == SQL_SELECT) {
        return execute_select(&stmt->select);
    }

    fprintf(stderr, "Error: Unsupported SQL statement type.\n");
    return FAILURE;
}
