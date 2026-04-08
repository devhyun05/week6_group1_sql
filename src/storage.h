#ifndef STORAGE_H
#define STORAGE_H

#include "common.h"
#include "hard_parser.h"

typedef struct {
    char columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int col_count;
    char ***rows;
    long *offsets;
    int row_count;
} StorageTable;

/**
 * Append one row to a table file. Creates the table on the first insert.
 */
int storage_insert(const char *table_name, const InsertStatement *stmt);

/**
 * Read all data rows from a table file.
 * The caller must free the returned rows with storage_free_rows().
 */
char ***storage_select(const char *table_name, int *row_count, int *col_count);

/**
 * Read the header row for a table.
 */
int storage_get_columns(const char *table_name, char columns[][MAX_IDENTIFIER_LEN], int *col_count);

/**
 * Release rows returned by storage_select() or storage_fetch_rows_by_offsets().
 */
void storage_free_rows(char ***rows, int row_count, int col_count);

/**
 * Load a table with both data rows and byte offsets.
 */
int storage_load_table(const char *table_name, StorageTable *out);

/**
 * Release a StorageTable previously filled by storage_load_table().
 */
void storage_free_table(StorageTable *table);

/**
 * Read specific rows by file offsets. The caller owns the returned rows.
 */
int storage_fetch_rows_by_offsets(
    const char *table_name,
    const long *offsets,
    int offset_count,
    int expected_col_count,
    char ****out_rows
);

#endif
