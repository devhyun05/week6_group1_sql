#ifndef INDEX_H
#define INDEX_H

#include "common.h"

typedef struct EqualityNode {
    char key[MAX_TOKEN_VALUE_LEN];
    long *offsets;
    int count;
    int capacity;
    struct EqualityNode *next;
} EqualityNode;

typedef struct {
    int column_index;
    int bucket_count;
    EqualityNode **buckets;
} EqualityIndex;

typedef struct {
    char value[MAX_TOKEN_VALUE_LEN];
    long offset;
    long long numeric_value;
    int is_numeric;
} RangeEntry;

typedef struct {
    int column_index;
    int entry_count;
    int use_numeric;
    RangeEntry *entries;
} RangeIndex;

/**
 * Build a hash index for equality predicates on one column.
 */
int index_build_equality(
    char ***rows,
    const long *offsets,
    int row_count,
    int column_index,
    EqualityIndex *out
);

/**
 * Query a hash index by exact value. The caller owns the returned offsets.
 */
int index_query_equals(const EqualityIndex *index, const char *value, long **offsets, int *match_count);

/**
 * Release an equality index.
 */
void index_free_equality(EqualityIndex *index);

/**
 * Build a sorted range index for one column.
 */
int index_build_range(
    char ***rows,
    const long *offsets,
    int row_count,
    int column_index,
    int prefer_numeric,
    RangeIndex *out
);

/**
 * Query a range index. The caller owns the returned offsets.
 */
int index_query_range(const RangeIndex *index, const char *op, const char *value, long **offsets, int *match_count);

/**
 * Release a range index.
 */
void index_free_range(RangeIndex *index);

#endif
