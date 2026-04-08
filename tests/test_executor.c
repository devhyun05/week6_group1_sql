#include "executor.h"
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

static void prepare_insert(SqlStatement *statement, const char *table_name,
                           const char *name, const char *age) {
    memset(statement, 0, sizeof(*statement));
    statement->type = SQL_INSERT;
    snprintf(statement->insert.table_name, sizeof(statement->insert.table_name),
             "%s", table_name);
    statement->insert.column_count = 2;
    snprintf(statement->insert.columns[0], sizeof(statement->insert.columns[0]), "name");
    snprintf(statement->insert.columns[1], sizeof(statement->insert.columns[1]), "age");
    snprintf(statement->insert.values[0], sizeof(statement->insert.values[0]), "%s", name);
    snprintf(statement->insert.values[1], sizeof(statement->insert.values[1]), "%s", age);
}

static void prepare_select(SqlStatement *statement, const char *table_name) {
    memset(statement, 0, sizeof(*statement));
    statement->type = SQL_SELECT;
    snprintf(statement->select.table_name, sizeof(statement->select.table_name),
             "%s", table_name);
    statement->select.column_count = 1;
    snprintf(statement->select.columns[0], sizeof(statement->select.columns[0]), "name");
    statement->select.has_where = 1;
    snprintf(statement->select.where.column, sizeof(statement->select.where.column), "age");
    snprintf(statement->select.where.op, sizeof(statement->select.where.op), ">=");
    snprintf(statement->select.where.value, sizeof(statement->select.where.value), "27");
}

static void prepare_delete(SqlStatement *statement, const char *table_name,
                           const char *name) {
    memset(statement, 0, sizeof(*statement));
    statement->type = SQL_DELETE;
    snprintf(statement->delete_stmt.table_name,
             sizeof(statement->delete_stmt.table_name), "%s", table_name);
    statement->delete_stmt.has_where = 1;
    snprintf(statement->delete_stmt.where.column,
             sizeof(statement->delete_stmt.where.column), "name");
    snprintf(statement->delete_stmt.where.op,
             sizeof(statement->delete_stmt.where.op), "=");
    snprintf(statement->delete_stmt.where.value,
             sizeof(statement->delete_stmt.where.value), "%s", name);
}

int main(void) {
    SqlStatement statement;
    char ***rows;
    int row_count;
    int col_count;

    remove("data/executor_users.csv");

    prepare_insert(&statement, "executor_users", "Alice", "30");
    if (assert_true(executor_execute(&statement) == SUCCESS,
                    "executor should insert row with auto id") != SUCCESS) {
        return EXIT_FAILURE;
    }

    prepare_insert(&statement, "executor_users", "Bob", "25");
    if (assert_true(executor_execute(&statement) == SUCCESS,
                    "executor should insert second row with auto id") != SUCCESS) {
        return EXIT_FAILURE;
    }

    prepare_delete(&statement, "executor_users", "Bob");
    if (assert_true(executor_execute(&statement) == SUCCESS,
                    "executor should delete matching rows") != SUCCESS) {
        return EXIT_FAILURE;
    }

    prepare_select(&statement, "executor_users");
    if (assert_true(executor_execute(&statement) == SUCCESS,
                    "executor should execute indexed SELECT") != SUCCESS) {
        return EXIT_FAILURE;
    }

    if (assert_true(executor_execute(&statement) == SUCCESS,
                    "executor should execute repeated SELECT consistently") != SUCCESS) {
        return EXIT_FAILURE;
    }

    rows = storage_select("executor_users", &row_count, &col_count);
    if (assert_true(rows != NULL, "storage_select should read executor table") != SUCCESS ||
        assert_true(row_count == 1, "executor delete should leave one row") != SUCCESS ||
        assert_true(strcmp(rows[0][1], "Alice") == 0,
                    "Alice should remain after deleting Bob") != SUCCESS) {
        storage_free_rows(rows, row_count, col_count);
        return EXIT_FAILURE;
    }
    storage_free_rows(rows, row_count, col_count);

    puts("[PASS] executor");
    return EXIT_SUCCESS;
}
