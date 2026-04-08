#include "executor.h"

#include "index.h"
#include "storage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXECUTOR_LIBRARY_CACHE_LIMIT 32
#define EXECUTOR_MAX_PLAN_CANDIDATES 4

typedef enum {
    EXECUTOR_PLAN_INSERT_WRITE,
    EXECUTOR_PLAN_SELECT_FULL_SCAN,
    EXECUTOR_PLAN_SELECT_INDEX_EQUALS,
    EXECUTOR_PLAN_SELECT_INDEX_RANGE,
    EXECUTOR_PLAN_DELETE_REWRITE
} ExecutorPlanType;

typedef struct {
    SqlType statement_type;
    char table_name[MAX_IDENTIFIER_LEN];
    int has_where;
    char where_column[MAX_IDENTIFIER_LEN];
    char where_op[4];
    char where_value[MAX_VALUE_LEN];
} ExecutorQuery;

typedef struct {
    ExecutorPlanType type;
    int estimated_cost;
} ExecutorPlan;

typedef struct {
    int in_use;
    unsigned long last_used_tick;
    char signature[MAX_SQL_LENGTH];
    ExecutorPlan plan;
} ExecutorLibraryCacheEntry;

static ExecutorLibraryCacheEntry executor_library_cache[EXECUTOR_LIBRARY_CACHE_LIMIT];
static unsigned long executor_library_cache_tick = 0;
static int executor_library_cache_entry_count = 0;
static int executor_library_cache_hit_count = 0;

static int executor_find_column_index(const char columns[][MAX_IDENTIFIER_LEN],
                                      int col_count, const char *target);

/*
 * 실행할 SQL 문에서 테이블 이름 필드가 비어 있지 않은지 확인한다.
 * 유효한 테이블 이름 포인터를 반환하고, 없으면 NULL을 반환한다.
 */
static const char *executor_get_table_name(const SqlStatement *statement) {
    if (statement == NULL) {
        return NULL;
    }

    switch (statement->type) {
        case SQL_INSERT:
            return statement->insert.table_name;
        case SQL_SELECT:
            return statement->select.table_name;
        case SQL_DELETE:
            return statement->delete_stmt.table_name;
        default:
            return NULL;
    }
}

/*
 * 라이브러리 캐시용 SQL 식별 문자열 뒤에 구분자와 값을 덧붙인다.
 */
static int executor_append_signature_piece(char **buffer, size_t *length,
                                           size_t *capacity, const char *label,
                                           const char *value) {
    if (utils_append_buffer(buffer, length, capacity, label) != SUCCESS ||
        utils_append_buffer(buffer, length, capacity,
                            value == NULL ? "" : value) != SUCCESS ||
        utils_append_buffer(buffer, length, capacity, "|") != SUCCESS) {
        return FAILURE;
    }

    return SUCCESS;
}

/*
 * SqlStatement를 기준으로 라이브러리 캐시 조회용 시그니처를 만든다.
 */
static int executor_build_statement_signature(const SqlStatement *statement,
                                              char *signature,
                                              size_t signature_size) {
    char *buffer;
    size_t length;
    size_t capacity;
    int i;
    int status;

    if (statement == NULL || signature == NULL || signature_size == 0) {
        return FAILURE;
    }

    buffer = NULL;
    length = 0;
    capacity = 0;
    status = FAILURE;

    switch (statement->type) {
        case SQL_INSERT:
            if (executor_append_signature_piece(&buffer, &length, &capacity,
                                               "TYPE=", "INSERT") != SUCCESS ||
                executor_append_signature_piece(&buffer, &length, &capacity,
                                               "TABLE=", statement->insert.table_name) != SUCCESS) {
                goto cleanup;
            }

            for (i = 0; i < statement->insert.column_count; i++) {
                if (executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "COL=", statement->insert.columns[i]) != SUCCESS ||
                    executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "VAL=", statement->insert.values[i]) != SUCCESS) {
                    goto cleanup;
                }
            }
            break;
        case SQL_SELECT:
            if (executor_append_signature_piece(&buffer, &length, &capacity,
                                               "TYPE=", "SELECT") != SUCCESS ||
                executor_append_signature_piece(&buffer, &length, &capacity,
                                               "TABLE=", statement->select.table_name) != SUCCESS) {
                goto cleanup;
            }

            if (statement->select.column_count == 0) {
                if (executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "COL=", "*") != SUCCESS) {
                    goto cleanup;
                }
            } else {
                for (i = 0; i < statement->select.column_count; i++) {
                    if (executor_append_signature_piece(&buffer, &length, &capacity,
                                                       "COL=", statement->select.columns[i]) != SUCCESS) {
                        goto cleanup;
                    }
                }
            }

            if (statement->select.has_where) {
                if (executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "WHERE_COL=",
                                                   statement->select.where.column) != SUCCESS ||
                    executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "WHERE_OP=",
                                                   statement->select.where.op) != SUCCESS ||
                    executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "WHERE_VAL=",
                                                   statement->select.where.value) != SUCCESS) {
                    goto cleanup;
                }
            }
            break;
        case SQL_DELETE:
            if (executor_append_signature_piece(&buffer, &length, &capacity,
                                               "TYPE=", "DELETE") != SUCCESS ||
                executor_append_signature_piece(&buffer, &length, &capacity,
                                               "TABLE=", statement->delete_stmt.table_name) != SUCCESS) {
                goto cleanup;
            }

            if (statement->delete_stmt.has_where) {
                if (executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "WHERE_COL=",
                                                   statement->delete_stmt.where.column) != SUCCESS ||
                    executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "WHERE_OP=",
                                                   statement->delete_stmt.where.op) != SUCCESS ||
                    executor_append_signature_piece(&buffer, &length, &capacity,
                                                   "WHERE_VAL=",
                                                   statement->delete_stmt.where.value) != SUCCESS) {
                    goto cleanup;
                }
            }
            break;
        default:
            goto cleanup;
    }

    if (utils_safe_strcpy(signature, signature_size, buffer == NULL ? "" : buffer) != SUCCESS) {
        fprintf(stderr, "Error: Statement signature is too long.\n");
        goto cleanup;
    }

    status = SUCCESS;

cleanup:
    free(buffer);
    return status;
}

/*
 * 현재 프로젝트 수준에서 가능한 객체 접근 권한을 검사한다.
 * 사용자 권한 모델은 없으므로, 접근 대상 유효성과 테이블 존재 여부만 확인한다.
 */
static int executor_check_object_privileges(const SqlStatement *statement) {
    const char *table_name;
    char columns[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int col_count;

    table_name = executor_get_table_name(statement);
    if (table_name == NULL || table_name[0] == '\0') {
        fprintf(stderr, "Error: Table name is missing.\n");
        return FAILURE;
    }

    if (statement->type == SQL_INSERT) {
        return SUCCESS;
    }

    return storage_get_columns(table_name, columns, &col_count);
}

/*
 * 캐시된 실행 계획이 있으면 plan에 복사하고 1을 반환한다.
 * 캐시에 없으면 0을 반환한다.
 */
static int executor_lookup_library_cache(const char *signature, ExecutorPlan *plan) {
    int i;

    if (signature == NULL || plan == NULL) {
        return 0;
    }

    executor_library_cache_tick++;
    for (i = 0; i < EXECUTOR_LIBRARY_CACHE_LIMIT; i++) {
        if (!executor_library_cache[i].in_use) {
            continue;
        }

        if (strcmp(executor_library_cache[i].signature, signature) == 0) {
            executor_library_cache[i].last_used_tick = executor_library_cache_tick;
            *plan = executor_library_cache[i].plan;
            executor_library_cache_hit_count++;
            return 1;
        }
    }

    return 0;
}

/*
 * 새로 선택한 실행 계획을 라이브러리 캐시에 저장한다.
 * 캐시가 가득 찼으면 가장 오래 사용하지 않은 엔트리를 덮어쓴다.
 */
static void executor_store_library_cache(const char *signature,
                                         const ExecutorPlan *plan) {
    int i;
    int target_index;
    unsigned long oldest_tick;

    if (signature == NULL || plan == NULL) {
        return;
    }

    executor_library_cache_tick++;
    target_index = FAILURE;
    oldest_tick = 0;

    for (i = 0; i < EXECUTOR_LIBRARY_CACHE_LIMIT; i++) {
        if (executor_library_cache[i].in_use &&
            strcmp(executor_library_cache[i].signature, signature) == 0) {
            target_index = i;
            break;
        }

        if (!executor_library_cache[i].in_use) {
            target_index = i;
            break;
        }

        if (target_index == FAILURE ||
            executor_library_cache[i].last_used_tick < oldest_tick) {
            oldest_tick = executor_library_cache[i].last_used_tick;
            target_index = i;
        }
    }

    if (target_index == FAILURE) {
        return;
    }

    if (!executor_library_cache[target_index].in_use) {
        executor_library_cache_entry_count++;
    }

    if (utils_safe_strcpy(executor_library_cache[target_index].signature,
                          sizeof(executor_library_cache[target_index].signature),
                          signature) != SUCCESS) {
        return;
    }

    executor_library_cache[target_index].in_use = 1;
    executor_library_cache[target_index].last_used_tick = executor_library_cache_tick;
    executor_library_cache[target_index].plan = *plan;
}

/*
 * SqlStatement를 계획 생성용 Query 구조체로 정규화한다.
 */
static int executor_transform_query(const SqlStatement *statement,
                                    ExecutorQuery *query) {
    if (statement == NULL || query == NULL) {
        return FAILURE;
    }

    memset(query, 0, sizeof(*query));
    query->statement_type = statement->type;
    if (utils_safe_strcpy(query->table_name, sizeof(query->table_name),
                          executor_get_table_name(statement)) != SUCCESS) {
        fprintf(stderr, "Error: Table name is too long.\n");
        return FAILURE;
    }

    if (statement->type == SQL_SELECT && statement->select.has_where) {
        query->has_where = 1;
        if (utils_safe_strcpy(query->where_column, sizeof(query->where_column),
                              statement->select.where.column) != SUCCESS ||
            utils_safe_strcpy(query->where_op, sizeof(query->where_op),
                              statement->select.where.op) != SUCCESS ||
            utils_safe_strcpy(query->where_value, sizeof(query->where_value),
                              statement->select.where.value) != SUCCESS) {
            fprintf(stderr, "Error: WHERE clause is too long.\n");
            return FAILURE;
        }
    } else if (statement->type == SQL_DELETE && statement->delete_stmt.has_where) {
        query->has_where = 1;
        if (utils_safe_strcpy(query->where_column, sizeof(query->where_column),
                              statement->delete_stmt.where.column) != SUCCESS ||
            utils_safe_strcpy(query->where_op, sizeof(query->where_op),
                              statement->delete_stmt.where.op) != SUCCESS ||
            utils_safe_strcpy(query->where_value, sizeof(query->where_value),
                              statement->delete_stmt.where.value) != SUCCESS) {
            fprintf(stderr, "Error: WHERE clause is too long.\n");
            return FAILURE;
        }
    }

    return SUCCESS;
}

/*
 * 변환된 쿼리로부터 가능한 실행 계획 후보를 만든다.
 */
static int executor_generate_plan_candidates(const ExecutorQuery *query,
                                             ExecutorPlan candidates[],
                                             int *candidate_count) {
    if (query == NULL || candidates == NULL || candidate_count == NULL) {
        return FAILURE;
    }

    *candidate_count = 0;
    switch (query->statement_type) {
        case SQL_INSERT:
            candidates[0].type = EXECUTOR_PLAN_INSERT_WRITE;
            candidates[0].estimated_cost = 1;
            *candidate_count = 1;
            return SUCCESS;
        case SQL_DELETE:
            candidates[0].type = EXECUTOR_PLAN_DELETE_REWRITE;
            candidates[0].estimated_cost = query->has_where ? 8 : 12;
            *candidate_count = 1;
            return SUCCESS;
        case SQL_SELECT:
            candidates[*candidate_count].type = EXECUTOR_PLAN_SELECT_FULL_SCAN;
            candidates[*candidate_count].estimated_cost =
                query->has_where ? 10 : 4;
            (*candidate_count)++;

            if (query->has_where) {
                candidates[*candidate_count].type =
                    strcmp(query->where_op, "=") == 0 ?
                    EXECUTOR_PLAN_SELECT_INDEX_EQUALS :
                    EXECUTOR_PLAN_SELECT_INDEX_RANGE;
                candidates[*candidate_count].estimated_cost =
                    strcmp(query->where_op, "=") == 0 ? 3 : 5;
                (*candidate_count)++;
            }
            return SUCCESS;
        default:
            fprintf(stderr, "Error: Unsupported SQL statement type.\n");
            return FAILURE;
    }
}

/*
 * 실행 계획 후보 중 비용이 가장 낮은 계획 하나를 선택한다.
 */
static int executor_select_lowest_cost_plan(const ExecutorPlan candidates[],
                                            int candidate_count,
                                            ExecutorPlan *selected_plan) {
    int i;
    int best_index;

    if (candidates == NULL || selected_plan == NULL || candidate_count <= 0) {
        return FAILURE;
    }

    best_index = 0;
    for (i = 1; i < candidate_count; i++) {
        if (candidates[i].estimated_cost < candidates[best_index].estimated_cost) {
            best_index = i;
        }
    }

    *selected_plan = candidates[best_index];
    return SUCCESS;
}

/*
 * WHERE 조건을 한 행에 적용해 일치 여부를 반환한다.
 * 조건이 참이면 1, 거짓이면 0, 오류면 FAILURE를 반환한다.
 */
static int executor_row_matches_where(const TableData *table, char **row,
                                      const WhereClause *where) {
    int column_index;
    int comparison;

    if (table == NULL || row == NULL || where == NULL) {
        return FAILURE;
    }

    column_index = executor_find_column_index(table->columns, table->col_count,
                                              where->column);
    if (column_index == FAILURE) {
        fprintf(stderr, "Error: Column '%s' not found.\n", where->column);
        return FAILURE;
    }

    comparison = utils_compare_values(row[column_index], where->value);
    if (strcmp(where->op, "=") == 0) {
        return comparison == 0;
    }
    if (strcmp(where->op, "!=") == 0) {
        return comparison != 0;
    }
    if (strcmp(where->op, ">") == 0) {
        return comparison > 0;
    }
    if (strcmp(where->op, "<") == 0) {
        return comparison < 0;
    }
    if (strcmp(where->op, ">=") == 0) {
        return comparison >= 0;
    }
    if (strcmp(where->op, "<=") == 0) {
        return comparison <= 0;
    }

    fprintf(stderr, "Error: Unsupported WHERE operator '%s'.\n", where->op);
    return FAILURE;
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
                                       char **source_row, const int *selected_indices,
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
    long left = *(const long *)lhs;
    long right = *(const long *)rhs;

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
 * 전체 스캔으로 WHERE 조건을 평가해 필요한 행만 결과 배열로 복사한다.
 * 성공 시 out_rows의 소유권은 호출자에게 있다.
 */
static int executor_collect_filtered_rows(const SelectStatement *stmt,
                                          const TableData *table,
                                          const int *selected_indices,
                                          int selected_count,
                                          char ****out_rows,
                                          int *out_row_count) {
    int i;
    int result_index;
    int matches;
    char ***result_rows;

    if (executor_allocate_result_rows(&result_rows, table->row_count) != SUCCESS) {
        return FAILURE;
    }

    result_index = 0;
    for (i = 0; i < table->row_count; i++) {
        matches = executor_row_matches_where(table, table->rows[i], &stmt->where);
        if (matches == FAILURE) {
            executor_free_result_rows(result_rows, result_index, selected_count);
            return FAILURE;
        }

        if (!matches) {
            continue;
        }

        if (executor_copy_projected_row(result_rows, result_index, table->rows[i],
                                        selected_indices, selected_count) != SUCCESS) {
            executor_free_result_rows(result_rows, result_index, selected_count);
            return FAILURE;
        }
        result_index++;
    }

    *out_rows = result_rows;
    *out_row_count = result_index;
    return SUCCESS;
}

/*
 * 일시적인 인덱스를 만들고 일치하는 오프셋만 찾아 필요한 행만 읽는다.
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
        if (index_query_equals(&index, stmt->where.value, &offsets, &match_count) != SUCCESS) {
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
    if (storage_insert(stmt->table_name, stmt) != SUCCESS) {
        return FAILURE;
    }

    printf("1 row inserted into %s.\n", stmt->table_name);
    return SUCCESS;
}

/*
 * SELECT 문 하나를 실행하고 표 형태로 출력한 뒤 결과 메모리를 정리한다.
 */
static int executor_execute_select(const SelectStatement *stmt,
                                   const ExecutorPlan *plan) {
    TableData table;
    int selected_indices[MAX_COLUMNS];
    char headers[MAX_COLUMNS][MAX_IDENTIFIER_LEN];
    int selected_count;
    char ***result_rows;
    int result_row_count;
    int status;

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
    } else if (plan != NULL &&
               (plan->type == EXECUTOR_PLAN_SELECT_INDEX_EQUALS ||
                plan->type == EXECUTOR_PLAN_SELECT_INDEX_RANGE)) {
        status = executor_collect_indexed_rows(stmt, &table, selected_indices,
                                               selected_count, &result_rows,
                                               &result_row_count);
    } else {
        status = executor_collect_filtered_rows(stmt, &table, selected_indices,
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

    deleted_count = 0;
    if (storage_delete(stmt->table_name, stmt, &deleted_count) != SUCCESS) {
        return FAILURE;
    }

    printf("%d row%s deleted from %s.\n", deleted_count,
           deleted_count == 1 ? "" : "s", stmt->table_name);
    return SUCCESS;
}

/*
 * 선택된 실행 계획에 따라 실제 실행 함수를 호출한다.
 */
static int executor_execute_plan(const SqlStatement *statement,
                                 const ExecutorPlan *plan) {
    if (statement == NULL || plan == NULL) {
        return FAILURE;
    }

    switch (statement->type) {
        case SQL_INSERT:
            return executor_execute_insert(&statement->insert);
        case SQL_SELECT:
            return executor_execute_select(&statement->select, plan);
        case SQL_DELETE:
            return executor_execute_delete(&statement->delete_stmt);
        default:
            fprintf(stderr, "Error: Unsupported SQL statement type.\n");
            return FAILURE;
    }
}

/*
 * 파싱된 SQL 문에 대해 권한 검사, 라이브러리 캐시 확인, 쿼리 변환,
 * 실행 계획 생성, 최소 비용 계획 선택 순서로 진행한 뒤 실행한다.
 */
int executor_execute(const SqlStatement *statement) {
    char signature[MAX_SQL_LENGTH];
    ExecutorQuery query;
    ExecutorPlan cached_plan;
    ExecutorPlan selected_plan;
    ExecutorPlan candidates[EXECUTOR_MAX_PLAN_CANDIDATES];
    int candidate_count;

    if (statement == NULL) {
        return FAILURE;
    }

    if (executor_check_object_privileges(statement) != SUCCESS) {
        return FAILURE;
    }

    if (executor_build_statement_signature(statement, signature,
                                           sizeof(signature)) != SUCCESS) {
        return FAILURE;
    }

    if (executor_lookup_library_cache(signature, &cached_plan)) {
        return executor_execute_plan(statement, &cached_plan);
    }

    if (executor_transform_query(statement, &query) != SUCCESS) {
        return FAILURE;
    }

    if (executor_generate_plan_candidates(&query, candidates,
                                          &candidate_count) != SUCCESS) {
        return FAILURE;
    }

    if (executor_select_lowest_cost_plan(candidates, candidate_count,
                                         &selected_plan) != SUCCESS) {
        return FAILURE;
    }

    executor_store_library_cache(signature, &selected_plan);
    return executor_execute_plan(statement, &selected_plan);
}

/*
 * 실행기 런타임 캐시 상태를 초기화한다.
 */
void executor_reset_runtime_state(void) {
    memset(executor_library_cache, 0, sizeof(executor_library_cache));
    executor_library_cache_tick = 0;
    executor_library_cache_entry_count = 0;
    executor_library_cache_hit_count = 0;
}

/*
 * 현재 라이브러리 캐시에 저장된 실행 계획 수를 반환한다.
 */
int executor_get_library_cache_entry_count(void) {
    return executor_library_cache_entry_count;
}

/*
 * 마지막 초기화 이후 발생한 라이브러리 캐시 히트 수를 반환한다.
 */
int executor_get_library_cache_hit_count(void) {
    return executor_library_cache_hit_count;
}
