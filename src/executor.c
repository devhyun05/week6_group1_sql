#include "executor.h"

#include "index.h"
#include "storage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXECUTOR_TABLE_CACHE_LIMIT 8
#define EXECUTOR_INDEX_CACHE_LIMIT 16

typedef struct {
    int in_use;
    unsigned long last_used_tick;
    char table_name[MAX_IDENTIFIER_LEN];
    TableData table;
} ExecutorTableCacheEntry;

typedef struct {
    int in_use;
    unsigned long last_used_tick;
    char table_name[MAX_IDENTIFIER_LEN];
    char column_name[MAX_IDENTIFIER_LEN];
    TableIndex index;
} ExecutorIndexCacheEntry;

static ExecutorTableCacheEntry executor_table_cache[EXECUTOR_TABLE_CACHE_LIMIT];
static ExecutorIndexCacheEntry executor_index_cache[EXECUTOR_INDEX_CACHE_LIMIT];
static unsigned long executor_cache_tick = 0;
static int executor_table_cache_hit_count = 0;
static int executor_index_cache_hit_count = 0;

/*
 * 캐시 엔트리를 최근 사용 시점으로 갱신한다.
 */
static void executor_touch_cache(unsigned long *last_used_tick) {
    executor_cache_tick++;
    *last_used_tick = executor_cache_tick;
}

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
 * 테이블 캐시 엔트리 하나를 비우고 소유한 메모리를 해제한다.
 */
static void executor_clear_table_cache_entry(ExecutorTableCacheEntry *entry) {
    if (entry == NULL || !entry->in_use) {
        return;
    }

    storage_free_table(&entry->table);
    memset(entry, 0, sizeof(*entry));
}

/*
 * 인덱스 캐시 엔트리 하나를 비우고 소유한 메모리를 해제한다.
 */
static void executor_clear_index_cache_entry(ExecutorIndexCacheEntry *entry) {
    if (entry == NULL || !entry->in_use) {
        return;
    }

    index_free(&entry->index);
    memset(entry, 0, sizeof(*entry));
}

/*
 * 테이블 캐시에 새 엔트리를 둘 슬롯을 고른다.
 * 비어 있는 슬롯이 우선이며, 없으면 가장 오래 사용하지 않은 슬롯을 반환한다.
 */
static int executor_choose_table_cache_slot(void) {
    int i;
    int candidate;

    candidate = 0;
    for (i = 0; i < EXECUTOR_TABLE_CACHE_LIMIT; i++) {
        if (!executor_table_cache[i].in_use) {
            return i;
        }

        if (executor_table_cache[i].last_used_tick <
            executor_table_cache[candidate].last_used_tick) {
            candidate = i;
        }
    }

    return candidate;
}

/*
 * 인덱스 캐시에 새 엔트리를 둘 슬롯을 고른다.
 * 비어 있는 슬롯이 우선이며, 없으면 가장 오래 사용하지 않은 슬롯을 반환한다.
 */
static int executor_choose_index_cache_slot(void) {
    int i;
    int candidate;

    candidate = 0;
    for (i = 0; i < EXECUTOR_INDEX_CACHE_LIMIT; i++) {
        if (!executor_index_cache[i].in_use) {
            return i;
        }

        if (executor_index_cache[i].last_used_tick <
            executor_index_cache[candidate].last_used_tick) {
            candidate = i;
        }
    }

    return candidate;
}

/*
 * 같은 테이블을 참조하는 테이블 캐시와 인덱스 캐시를 모두 무효화한다.
 */
static void executor_invalidate_table_cache(const char *table_name) {
    int i;

    if (table_name == NULL) {
        return;
    }

    for (i = 0; i < EXECUTOR_TABLE_CACHE_LIMIT; i++) {
        if (executor_table_cache[i].in_use &&
            utils_equals_ignore_case(executor_table_cache[i].table_name,
                                     table_name)) {
            executor_clear_table_cache_entry(&executor_table_cache[i]);
        }
    }

    for (i = 0; i < EXECUTOR_INDEX_CACHE_LIMIT; i++) {
        if (executor_index_cache[i].in_use &&
            utils_equals_ignore_case(executor_index_cache[i].table_name,
                                     table_name)) {
            executor_clear_index_cache_entry(&executor_index_cache[i]);
        }
    }
}

/*
 * 같은 실행 중이면 메모리의 테이블 캐시를 재사용하고,
 * 없으면 storage 계층에서 한 번 읽어 캐시에 넣는다.
 */
static int executor_get_cached_table(const char *table_name,
                                     const TableData **out_table) {
    int i;
    int slot;

    if (table_name == NULL || out_table == NULL) {
        return FAILURE;
    }

    for (i = 0; i < EXECUTOR_TABLE_CACHE_LIMIT; i++) {
        if (!executor_table_cache[i].in_use) {
            continue;
        }

        if (utils_equals_ignore_case(executor_table_cache[i].table_name,
                                     table_name)) {
            executor_touch_cache(&executor_table_cache[i].last_used_tick);
            executor_table_cache_hit_count++;
            *out_table = &executor_table_cache[i].table;
            return SUCCESS;
        }
    }

    slot = executor_choose_table_cache_slot();
    executor_clear_table_cache_entry(&executor_table_cache[slot]);
    if (storage_load_table(table_name, &executor_table_cache[slot].table) != SUCCESS) {
        return FAILURE;
    }

    if (utils_safe_strcpy(executor_table_cache[slot].table_name,
                          sizeof(executor_table_cache[slot].table_name),
                          table_name) != SUCCESS) {
        fprintf(stderr, "Error: Table name is too long.\n");
        executor_clear_table_cache_entry(&executor_table_cache[slot]);
        return FAILURE;
    }

    executor_table_cache[slot].in_use = 1;
    executor_touch_cache(&executor_table_cache[slot].last_used_tick);
    *out_table = &executor_table_cache[slot].table;
    return SUCCESS;
}

/*
 * 같은 실행 중 같은 테이블·컬럼 조합이면 기존 인덱스를 재사용하고,
 * 없으면 한 번 생성해 캐시에 넣는다.
 */
static int executor_get_cached_index(const char *table_name, const char *column_name,
                                     const TableData *table, TableIndex **out_index) {
    int i;
    int slot;
    int column_index;

    if (table_name == NULL || column_name == NULL || table == NULL ||
        out_index == NULL) {
        return FAILURE;
    }

    for (i = 0; i < EXECUTOR_INDEX_CACHE_LIMIT; i++) {
        if (!executor_index_cache[i].in_use) {
            continue;
        }

        if (utils_equals_ignore_case(executor_index_cache[i].table_name, table_name) &&
            utils_equals_ignore_case(executor_index_cache[i].column_name, column_name)) {
            executor_touch_cache(&executor_index_cache[i].last_used_tick);
            executor_index_cache_hit_count++;
            *out_index = &executor_index_cache[i].index;
            return SUCCESS;
        }
    }

    column_index = executor_find_column_index(table->columns, table->col_count,
                                              column_name);
    if (column_index == FAILURE) {
        fprintf(stderr, "Error: Column '%s' not found.\n", column_name);
        return FAILURE;
    }

    slot = executor_choose_index_cache_slot();
    executor_clear_index_cache_entry(&executor_index_cache[slot]);
    if (index_build(table, column_index, &executor_index_cache[slot].index) != SUCCESS) {
        return FAILURE;
    }

    if (utils_safe_strcpy(executor_index_cache[slot].table_name,
                          sizeof(executor_index_cache[slot].table_name),
                          table_name) != SUCCESS ||
        utils_safe_strcpy(executor_index_cache[slot].column_name,
                          sizeof(executor_index_cache[slot].column_name),
                          column_name) != SUCCESS) {
        fprintf(stderr, "Error: Identifier is too long.\n");
        executor_clear_index_cache_entry(&executor_index_cache[slot]);
        return FAILURE;
    }

    executor_index_cache[slot].in_use = 1;
    executor_touch_cache(&executor_index_cache[slot].last_used_tick);
    *out_index = &executor_index_cache[slot].index;
    return SUCCESS;
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
 * 재사용 가능한 인덱스를 이용해 일치하는 오프셋만 찾아 필요한 행만 읽는다.
 * 성공 시 out_rows의 소유권은 호출자에게 있다.
 */
static int executor_collect_indexed_rows(const SelectStatement *stmt,
                                         const TableData *table,
                                         const TableIndex *index,
                                         const int *selected_indices,
                                         int selected_count,
                                         char ****out_rows, int *out_row_count) {
    long *offsets;
    int match_count;
    int i;
    char ***result_rows;
    char **full_row;

    if (stmt == NULL || table == NULL || index == NULL ||
        selected_indices == NULL || out_rows == NULL || out_row_count == NULL) {
        return FAILURE;
    }

    offsets = NULL;
    match_count = 0;
    if (strcmp(stmt->where.op, "=") == 0) {
        if (index_query_equals(index, stmt->where.value, &offsets,
                               &match_count) != SUCCESS) {
            return FAILURE;
        }
    } else {
        if (index_query_range(index, stmt->where.op, stmt->where.value,
                              &offsets, &match_count) != SUCCESS) {
            return FAILURE;
        }
    }

    if (match_count > 1) {
        qsort(offsets, (size_t)match_count, sizeof(long), executor_compare_offsets);
    }

    if (executor_allocate_result_rows(&result_rows, match_count) != SUCCESS) {
        free(offsets);
        return FAILURE;
    }

    for (i = 0; i < match_count; i++) {
        if (storage_read_row_at_offset(stmt->table_name, offsets[i], table->col_count,
                                       &full_row) != SUCCESS) {
            executor_free_result_rows(result_rows, i, selected_count);
            free(offsets);
            return FAILURE;
        }

        if (executor_copy_projected_row(result_rows, i, full_row, selected_indices,
                                        selected_count) != SUCCESS) {
            storage_free_row(full_row, table->col_count);
            executor_free_result_rows(result_rows, i, selected_count);
            free(offsets);
            return FAILURE;
        }

        storage_free_row(full_row, table->col_count);
    }

    free(offsets);
    *out_rows = result_rows;
    *out_row_count = match_count;
    return SUCCESS;
}

/*
 * INSERT 문 하나를 스토리지 계층으로 실행하고 결과 메시지를 출력한다.
 * 성공하면 해당 테이블의 재사용 캐시를 무효화한다.
 */
static int executor_execute_insert(const InsertStatement *stmt) {
    if (stmt == NULL) {
        return FAILURE;
    }

    if (storage_insert(stmt->table_name, stmt) != SUCCESS) {
        return FAILURE;
    }

    executor_invalidate_table_cache(stmt->table_name);
    printf("1 row inserted into %s.\n", stmt->table_name);
    return SUCCESS;
}

/*
 * SELECT 문 하나를 실행하고 표 형태로 출력한 뒤 결과 메모리를 정리한다.
 * 같은 실행 안에서는 테이블과 컬럼 인덱스를 재사용한다.
 */
static int executor_execute_select(const SelectStatement *stmt) {
    const TableData *table;
    TableIndex *index;
    int selected_indices[MAX_COLUMNS];
    char headers[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int selected_count;
    char ***result_rows;
    int result_row_count;
    int status;

    if (stmt == NULL) {
        return FAILURE;
    }

    if (executor_get_cached_table(stmt->table_name, &table) != SUCCESS) {
        return FAILURE;
    }

    status = executor_prepare_projection(stmt, table, selected_indices, headers,
                                         &selected_count);
    if (status != SUCCESS) {
        return FAILURE;
    }

    result_rows = NULL;
    result_row_count = 0;
    if (!stmt->has_where) {
        status = executor_collect_all_rows(table, selected_indices, selected_count,
                                           &result_rows, &result_row_count);
    } else {
        if (executor_get_cached_index(stmt->table_name, stmt->where.column,
                                      table, &index) != SUCCESS) {
            return FAILURE;
        }

        status = executor_collect_indexed_rows(stmt, table, index,
                                               selected_indices, selected_count,
                                               &result_rows, &result_row_count);
    }

    if (status != SUCCESS) {
        return FAILURE;
    }

    executor_print_table(headers, selected_count, result_rows, result_row_count);
    printf("%d row%s selected.\n", result_row_count,
           result_row_count == 1 ? "" : "s");

    executor_free_result_rows(result_rows, result_row_count, selected_count);
    return SUCCESS;
}

/*
 * DELETE 문 하나를 실행하고 삭제된 행 수를 출력한다.
 * 성공하면 해당 테이블의 재사용 캐시를 무효화한다.
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

    executor_invalidate_table_cache(stmt->table_name);
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

/*
 * 실행기 런타임 캐시를 모두 해제하고 통계를 초기화한다.
 */
void executor_reset_runtime_state(void) {
    int i;

    for (i = 0; i < EXECUTOR_TABLE_CACHE_LIMIT; i++) {
        executor_clear_table_cache_entry(&executor_table_cache[i]);
    }

    for (i = 0; i < EXECUTOR_INDEX_CACHE_LIMIT; i++) {
        executor_clear_index_cache_entry(&executor_index_cache[i]);
    }

    executor_cache_tick = 0;
    executor_table_cache_hit_count = 0;
    executor_index_cache_hit_count = 0;
}

/*
 * 마지막 초기화 이후 발생한 테이블 캐시 히트 수를 반환한다.
 */
int executor_get_table_cache_hit_count(void) {
    return executor_table_cache_hit_count;
}

/*
 * 마지막 초기화 이후 발생한 인덱스 캐시 히트 수를 반환한다.
 */
int executor_get_index_cache_hit_count(void) {
    return executor_index_cache_hit_count;
}
