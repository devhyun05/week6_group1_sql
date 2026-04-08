#include "storage.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void) {
    InsertStatement stmt;
    StorageTable table;

    unlink("data/test_storage.csv");
    memset(&stmt, 0, sizeof(stmt));
    snprintf(stmt.table_name, sizeof(stmt.table_name), "%s", "test_storage");
    stmt.column_count = 3;
    snprintf(stmt.columns[0], sizeof(stmt.columns[0]), "%s", "id");
    snprintf(stmt.columns[1], sizeof(stmt.columns[1]), "%s", "name");
    snprintf(stmt.columns[2], sizeof(stmt.columns[2]), "%s", "age");
    snprintf(stmt.values[0], sizeof(stmt.values[0]), "%s", "1");
    snprintf(stmt.values[1], sizeof(stmt.values[1]), "%s", "Alice");
    snprintf(stmt.values[2], sizeof(stmt.values[2]), "%s", "30");

    assert(storage_insert("test_storage", &stmt) == 0);
    snprintf(stmt.values[1], sizeof(stmt.values[1]), "%s", "Alice Duplicate");
    assert(storage_insert("test_storage", &stmt) == -1);
    assert(storage_load_table("test_storage", &table) == 0);
    assert(table.col_count == 3);
    assert(table.row_count == 1);
    assert(strcmp(table.rows[0][1], "Alice") == 0);
    storage_free_table(&table);

    printf("test_storage passed\n");
    return 0;
}
