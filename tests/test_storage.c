#include "storage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int assert_true(int condition, const char *message) {
    if (!condition) {
        fprintf(stderr, "[FAIL] %s\n", message);
        return FAILURE;
    }
    return SUCCESS;
}

static void prepare_insert(InsertStatement *stmt, const char *table_name,
                           int include_id, const char *id, const char *name,
                           const char *age) {
    memset(stmt, 0, sizeof(*stmt));
    snprintf(stmt->table_name, sizeof(stmt->table_name), "%s", table_name);
    if (include_id) {
        stmt->column_count = 3;
        snprintf(stmt->columns[0], sizeof(stmt->columns[0]), "id");
        snprintf(stmt->columns[1], sizeof(stmt->columns[1]), "name");
        snprintf(stmt->columns[2], sizeof(stmt->columns[2]), "age");
        snprintf(stmt->values[0], sizeof(stmt->values[0]), "%s", id);
        snprintf(stmt->values[1], sizeof(stmt->values[1]), "%s", name);
        snprintf(stmt->values[2], sizeof(stmt->values[2]), "%s", age);
    } else {
        stmt->column_count = 2;
        snprintf(stmt->columns[0], sizeof(stmt->columns[0]), "name");
        snprintf(stmt->columns[1], sizeof(stmt->columns[1]), "age");
        snprintf(stmt->values[0], sizeof(stmt->values[0]), "%s", name);
        snprintf(stmt->values[1], sizeof(stmt->values[1]), "%s", age);
    }
}

int main(void) {
    InsertStatement stmt;
    char columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int col_count;
    int row_count;
    char ***rows;
    TableData table;
    char **row;

    remove("data/storage_users.csv");

    prepare_insert(&stmt, "storage_users", 0, "1", "Alice", "30");
    if (assert_true(storage_insert("storage_users", &stmt) == SUCCESS,
                    "storage_insert should create table with auto id") != SUCCESS) {
        return EXIT_FAILURE;
    }

    prepare_insert(&stmt, "storage_users", 0, "2", "Lee, Jr.", "28");
    if (assert_true(storage_insert("storage_users", &stmt) == SUCCESS,
                    "storage_insert should append row with next auto id") != SUCCESS) {
        return EXIT_FAILURE;
    }

    prepare_insert(&stmt, "storage_users", 1, "2", "Duplicate", "40");
    if (assert_true(storage_insert("storage_users", &stmt) == FAILURE,
                    "storage_insert should reject duplicate id values") != SUCCESS) {
        return EXIT_FAILURE;
    }

    if (assert_true(storage_get_columns("storage_users", columns, &col_count) == SUCCESS,
                    "storage_get_columns should read header") != SUCCESS ||
        assert_true(col_count == 3, "header column count should be 3") != SUCCESS ||
        assert_true(strcmp(columns[1], "name") == 0, "second column should be name") != SUCCESS) {
        return EXIT_FAILURE;
    }

    rows = storage_select("storage_users", &row_count, &col_count);
    if (assert_true(rows != NULL, "storage_select should read rows") != SUCCESS ||
        assert_true(row_count == 2, "row count should stay 2 after duplicate reject") != SUCCESS ||
        assert_true(strcmp(rows[0][0], "1") == 0, "first row should receive id 1") != SUCCESS ||
        assert_true(strcmp(rows[1][0], "2") == 0, "second row should receive id 2") != SUCCESS ||
        assert_true(strcmp(rows[1][1], "Lee, Jr.") == 0,
                    "CSV parser should preserve commas in strings") != SUCCESS) {
        storage_free_rows(rows, row_count, col_count);
        return EXIT_FAILURE;
    }
    storage_free_rows(rows, row_count, col_count);

    if (assert_true(storage_load_table("storage_users", &table) == SUCCESS,
                    "storage_load_table should load offsets") != SUCCESS ||
        assert_true(table.offsets != NULL, "offset array should exist") != SUCCESS ||
        assert_true(table.row_count == 2, "loaded table row count should be 2") != SUCCESS) {
        storage_free_table(&table);
        return EXIT_FAILURE;
    }

    if (assert_true(storage_read_row_at_offset("storage_users", table.offsets[1],
                                               table.col_count, &row) == SUCCESS,
                    "storage_read_row_at_offset should read indexed row") != SUCCESS ||
        assert_true(strcmp(row[1], "Lee, Jr.") == 0,
                    "offset read should return second row") != SUCCESS) {
        storage_free_row(row, table.col_count);
        storage_free_table(&table);
        return EXIT_FAILURE;
    }

    storage_free_row(row, table.col_count);
    storage_free_table(&table);
    puts("[PASS] storage");
    return EXIT_SUCCESS;
}
