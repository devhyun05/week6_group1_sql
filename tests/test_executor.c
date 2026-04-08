#include "executor.h"
#include "storage.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void) {
    SqlStatement stmt;
    StorageTable table;

    unlink("data/test_executor.csv");
    memset(&stmt, 0, sizeof(stmt));
    stmt.type = SQL_INSERT;
    snprintf(stmt.insert.table_name, sizeof(stmt.insert.table_name), "%s", "test_executor");
    stmt.insert.column_count = 2;
    snprintf(stmt.insert.columns[0], sizeof(stmt.insert.columns[0]), "%s", "id");
    snprintf(stmt.insert.columns[1], sizeof(stmt.insert.columns[1]), "%s", "name");
    snprintf(stmt.insert.values[0], sizeof(stmt.insert.values[0]), "%s", "1");
    snprintf(stmt.insert.values[1], sizeof(stmt.insert.values[1]), "%s", "Bob");

    assert(executor_execute(&stmt) == 0);
    assert(storage_load_table("test_executor", &table) == 0);
    assert(table.row_count == 1);
    assert(strcmp(table.rows[0][0], "1") == 0);
    assert(strcmp(table.rows[0][1], "Bob") == 0);
    storage_free_table(&table);

    printf("test_executor passed\n");
    return 0;
}
