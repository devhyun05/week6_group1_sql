#ifndef STORAGE_H
#define STORAGE_H

#include "hard_parser.h"

typedef struct {
    int row_count;
    int col_count;
    char columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    char ***rows;
    long *offsets;
} TableData;

/*
 * Append one row to the table CSV file.
 * Returns SUCCESS on success, FAILURE on error.
 */
int storage_insert(const char *table_name, const InsertStatement *stmt);

/*
 * Delete rows from the table CSV file.
 * When WHERE is omitted, all data rows are removed and the header is kept.
 * Returns SUCCESS on success, FAILURE on error.
 */
int storage_delete(const char *table_name, const DeleteStatement *stmt,
                   int *deleted_count);

/*
 * Read all table rows into a newly allocated 3D array.
 * Caller owns the returned memory and must free it with storage_free_rows().
 */
char ***storage_select(const char *table_name, int *row_count, int *col_count);

/*
 * Read the header row of a table CSV file.
 * Returns SUCCESS on success, FAILURE when the table cannot be read.
 */
int storage_get_columns(const char *table_name, char columns[][MAX_IDENTIFIER_LEN],
                        int *col_count);

/*
 * Load a full table including header, rows, and byte offsets.
 * Caller owns the allocated members and must free them with storage_free_table().
 */
int storage_load_table(const char *table_name, TableData *table);

/*
 * Read a single row by its byte offset.
 * Caller owns the returned row and must free it with storage_free_row().
 */
int storage_read_row_at_offset(const char *table_name, long offset, int expected_col_count,
                               char ***out_row);

/*
 * Free a single row allocated by storage_read_row_at_offset().
 */
void storage_free_row(char **row, int col_count);

/*
 * Free rows returned by storage_select().
 */
void storage_free_rows(char ***rows, int row_count, int col_count);

/*
 * Free all dynamic members inside a TableData structure.
 */
void storage_free_table(TableData *table);

#endif
