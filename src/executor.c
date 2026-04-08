#include "executor.h"

#include "index.h"
#include "storage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * 메모리에 올라온 테이블 스키마에서 컬럼 이름을 대소문자 무시로 찾는다.
 * 컬럼 인덱스를 반환하고, 없으면 FAILURE를 반환한다.
 */
static int executor_find_column_index(const char columns[][MAX_IDENTIFIER_LEN],
                                      int col_count, const char *target) {
    int i;

    for (i = 0; i < col_count; i++) {
        if (utils_equals_ignore_case(columns[i], target)) {
            return i;
        }
    }

    return FAILURE;
}

/*
 * 결과 셀 문자열 하나를 복제한다.
 * NULL 값은 빈 문자열로 처리하며 반환된 메모리는 호출자가 소유한다.
 */
static char *executor_duplicate_cell(const char *value) {
    return utils_strdup(value == NULL ? "" : value);
}

/*
 * SELECT 결과를 담을 바깥쪽 행 배열을 할당한다.
 * 성공 시 rows에 저장하고 SUCCESS를 반환한다.
 */
static int executor_allocate_result_rows(char ****rows, int row_count) {
    if (rows == NULL) {
        return FAILURE;
    }

    if (row_count <= 0) {
        *rows = NULL;
        return SUCCESS;
    }

    *rows = (char ***)malloc((size_t)row_count * sizeof(char **));
    if (*rows == NULL) {
        fprintf(stderr, "Error: Failed to allocate memory.\n");
        return FAILURE;
    }

    return SUCCESS;
}

/*
 * 원본 행에서 선택된 컬럼만 복사해 결과 행으로 만든다.
 * 새 결과 행이 모두 할당되면 SUCCESS를 반환한다.
 */
static int executor_copy_projected_row(char ***result_rows, int result_index,
                                       char **source_row,
                                       const int *selected_indices,
                                       int selected_count) {
    int i;

    result_rows[result_index] = (char **)malloc((size_t)selected_count * sizeof(char *));
    if (result_rows[result_index] == NULL) {
        fprintf(stderr, "Error: Failed to allocate memory.\n");
        return FAILURE;
    }

    for (i = 0; i < selected_count; i++) {
        result_rows[result_index][i] =
            executor_duplicate_cell(source_row[selected_indices[i]]);
        if (result_rows[result_index][i] == NULL) {
            int j;
            for (j = 0; j < i; j++) {
                free(result_rows[result_index][j]);
                result_rows[result_index][j] = NULL;
            }
            free(result_rows[result_index]);
            result_rows[result_index] = NULL;
            return FAILURE;
        }
    }

    return SUCCESS;
}

/*
 * executor 내부 헬퍼가 만든 조회 결과 테이블을 해제한다.
 */
static void executor_free_result_rows(char ***rows, int row_count, int col_count) {
    storage_free_rows(rows, row_count, col_count);
}

/*
 * SELECT 표 출력용 가로 경계선을 한 줄 출력한다.
 */
static void executor_print_border(const int *widths, int col_count) {
    int i;
    int j;

    for (i = 0; i < col_count; i++) {
        putchar('+');
        for (j = 0; j < widths[i] + 2; j++) {
            putchar('-');
        }
    }
    puts("+");
}

/*
 * 표시 폭을 고려해 MySQL 스타일 표 형태로 조회 결과를 출력한다.
 */
static void executor_print_table(char headers[][MAX_IDENTIFIER_LEN], int header_count,
                                 char ***rows, int row_count) {
    int widths[MAX_COLUMNS];
    int i;
    int j;
    int cell_width;

    for (i = 0; i < header_count; i++) {
        widths[i] = utils_display_width(headers[i]);
    }

    for (i = 0; i < row_count; i++) {
        for (j = 0; j < header_count; j++) {
            cell_width = utils_display_width(rows[i][j]);
            if (cell_width > widths[j]) {
                widths[j] = cell_width;
            }
        }
    }

    executor_print_border(widths, header_count);
    for (i = 0; i < header_count; i++) {
        printf("| ");
        utils_print_padded(stdout, headers[i], widths[i]);
        putchar(' ');
    }
    puts("|");
    executor_print_border(widths, header_count);

    for (i = 0; i < row_count; i++) {
        for (j = 0; j < header_count; j++) {
            printf("| ");
            utils_print_padded(stdout, rows[i][j], widths[j]);
            putchar(' ');
        }
        puts("|");
    }

    executor_print_border(widths, header_count);
}

/*
 * 두 행 오프셋을 비교해 인덱스 결과를 파일 순서로 다시 정렬한다.
 */
static int executor_compare_offsets(const void *lhs, const void *rhs) {
    long left;
    long right;

    left = *(const long *)lhs;
    right = *(const long *)rhs;

    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

/*
 * SELECT 대상 컬럼을 원본 테이블 인덱스와 출력 헤더로 변환한다.
 * 요청된 컬럼이 모두 존재하면 SUCCESS를 반환한다.
 */
static int executor_prepare_projection(const SelectStatement *stmt,
                                       const TableData *table,
                                       int selected_indices[],
                                       char headers[][MAX_IDENTIFIER_LEN],
                                       int *selected_count) {
    int i;
    int column_index;

    if (stmt == NULL || table == NULL || selected_indices == NULL ||
        headers == NULL || selected_count == NULL) {
        return FAILURE;
    }

    if (stmt->column_count == 0) {
        for (i = 0; i < table->col_count; i++) {
            selected_indices[i] = i;
            if (utils_safe_strcpy(headers[i], sizeof(headers[i]),
                                  table->columns[i]) != SUCCESS) {
                fprintf(stderr, "Error: Column name is too long.\n");
                return FAILURE;
            }
        }
        *selected_count = table->col_count;
        return SUCCESS;
    }

    for (i = 0; i < stmt->column_count; i++) {
        column_index = executor_find_column_index(table->columns, table->col_count,
                                                  stmt->columns[i]);
        if (column_index == FAILURE) {
            fprintf(stderr, "Error: Column '%s' not found.\n", stmt->columns[i]);
            return FAILURE;
        }

        selected_indices[i] = column_index;
        if (utils_safe_strcpy(headers[i], sizeof(headers[i]),
                              table->columns[column_index]) != SUCCESS) {
            fprintf(stderr, "Error: Column name is too long.\n");
            return FAILURE;
        }
    }

    *selected_count = stmt->column_count;
    return SUCCESS;
}

/*
 * WHERE가 없는 SELECT를 위해 모든 행을 결과 행 배열로 복사한다.
 * 성공 시 out_rows의 소유권은 호출자에게 있다.
 */
static int executor_collect_all_rows(const TableData *table,
                                     const int *selected_indices, int selected_count,
                                     char ****out_rows, int *out_row_count) {
    int i;
    char ***result_rows;

    if (table == NULL || selected_indices == NULL || out_rows == NULL ||
        out_row_count == NULL) {
        return FAILURE;
    }

    if (executor_allocate_result_rows(&result_rows, table->row_count) != SUCCESS) {
        return FAILURE;
    }

    for (i = 0; i < table->row_count; i++) {
        if (executor_copy_projected_row(result_rows, i, table->rows[i],
                                        selected_indices, selected_count) != SUCCESS) {
            executor_free_result_rows(result_rows, i, selected_count);
            return FAILURE;
        }
    }

    *out_rows = result_rows;
    *out_row_count = table->row_count;
    return SUCCESS;
}

/*
 * 인메모리 인덱스를 만들고 일치하는 오프셋만 찾아 필요한 행만 읽는다.
 * 성공 시 out_rows의 소유권은 호출자에게 있다.
 */
static int executor_collect_indexed_rows(const SelectStatement *stmt,
                                         const TableData *table,
                                         const int *selected_indices,
                                         int selected_count,
                                         char ****out_rows, int *out_row_count) {
    int where_column_index;
    TableIndex index;
    long *offsets;
    int match_count;
    int i;
    char ***result_rows;
    char **full_row;

    if (stmt == NULL || table == NULL || selected_indices == NULL ||
        out_rows == NULL || out_row_count == NULL) {
        return FAILURE;
    }

    where_column_index = executor_find_column_index(table->columns, table->col_count,
                                                    stmt->where.column);
    if (where_column_index == FAILURE) {
        fprintf(stderr, "Error: Column '%s' not found.\n", stmt->where.column);
        return FAILURE;
    }

    if (index_build(table, where_column_index, &index) != SUCCESS) {
        return FAILURE;
    }

    offsets = NULL;
    match_count = 0;
    if (strcmp(stmt->where.op, "=") == 0) {
        if (index_query_equals(&index, stmt->where.value, &offsets,
                               &match_count) != SUCCESS) {
            index_free(&index);
            return FAILURE;
        }
    } else {
        if (index_query_range(&index, stmt->where.op, stmt->where.value,
                              &offsets, &match_count) != SUCCESS) {
            index_free(&index);
            return FAILURE;
        }
    }

    if (match_count > 1) {
        qsort(offsets, (size_t)match_count, sizeof(long), executor_compare_offsets);
    }

    if (executor_allocate_result_rows(&result_rows, match_count) != SUCCESS) {
        free(offsets);
        index_free(&index);
        return FAILURE;
    }

    for (i = 0; i < match_count; i++) {
        if (storage_read_row_at_offset(stmt->table_name, offsets[i], table->col_count,
                                       &full_row) != SUCCESS) {
            executor_free_result_rows(result_rows, i, selected_count);
            free(offsets);
            index_free(&index);
            return FAILURE;
        }

        if (executor_copy_projected_row(result_rows, i, full_row, selected_indices,
                                        selected_count) != SUCCESS) {
            storage_free_row(full_row, table->col_count);
            executor_free_result_rows(result_rows, i, selected_count);
            free(offsets);
            index_free(&index);
            return FAILURE;
        }

        storage_free_row(full_row, table->col_count);
    }

    free(offsets);
    index_free(&index);
    *out_rows = result_rows;
    *out_row_count = match_count;
    return SUCCESS;
}

/*
 * INSERT 문 하나를 스토리지 계층으로 실행하고 결과 메시지를 출력한다.
 */
static int executor_execute_insert(const InsertStatement *stmt) {
    if (stmt == NULL) {
        return FAILURE;
    }

    if (storage_insert(stmt->table_name, stmt) != SUCCESS) {
        return FAILURE;
    }

    printf("1 row inserted into %s.\n", stmt->table_name);
    return SUCCESS;
}

/*
 * SELECT 문 하나를 실행하고 표 형태로 출력한 뒤 결과 메모리를 정리한다.
 */
static int executor_execute_select(const SelectStatement *stmt) {
    TableData table;
    int selected_indices[MAX_COLUMNS];
    char headers[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int selected_count;
    char ***result_rows;
    int result_row_count;
    int status;

    if (stmt == NULL) {
        return FAILURE;
    }

    if (storage_load_table(stmt->table_name, &table) != SUCCESS) {
        return FAILURE;
    }

    status = executor_prepare_projection(stmt, &table, selected_indices, headers,
                                         &selected_count);
    if (status != SUCCESS) {
        storage_free_table(&table);
        return FAILURE;
    }

    result_rows = NULL;
    result_row_count = 0;
    if (!stmt->has_where) {
        status = executor_collect_all_rows(&table, selected_indices, selected_count,
                                           &result_rows, &result_row_count);
    } else {
        status = executor_collect_indexed_rows(stmt, &table, selected_indices,
                                               selected_count, &result_rows,
                                               &result_row_count);
    }

    if (status != SUCCESS) {
        storage_free_table(&table);
        return FAILURE;
    }

    executor_print_table(headers, selected_count, result_rows, result_row_count);
    printf("%d row%s selected.\n", result_row_count,
           result_row_count == 1 ? "" : "s");

    executor_free_result_rows(result_rows, result_row_count, selected_count);
    storage_free_table(&table);
    return SUCCESS;
}

/*
 * DELETE 문 하나를 실행하고 삭제된 행 수를 출력한다.
 */
static int executor_execute_delete(const DeleteStatement *stmt) {
    int deleted_count;

    if (stmt == NULL) {
        return FAILURE;
    }

    deleted_count = 0;
    if (storage_delete(stmt->table_name, stmt, &deleted_count) != SUCCESS) {
        return FAILURE;
    }

    printf("%d row%s deleted from %s.\n", deleted_count,
           deleted_count == 1 ? "" : "s", stmt->table_name);
    return SUCCESS;
}

/*
 * 파싱된 SQL 문을 받아 statement.type에 따라 INSERT, SELECT, DELETE로 분기한다.
 */
int executor_execute(const SqlStatement *statement) {
    if (statement == NULL) {
        return FAILURE;
    }

    switch (statement->type) {
        case SQL_INSERT:
            return executor_execute_insert(&statement->insert);
        case SQL_SELECT:
            return executor_execute_select(&statement->select);
        case SQL_DELETE:
            return executor_execute_delete(&statement->delete_stmt);
        default:
            fprintf(stderr, "Error: Unsupported SQL statement type.\n");
            return FAILURE;
    }
}
