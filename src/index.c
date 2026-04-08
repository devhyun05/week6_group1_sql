#include "index.h"

#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned long hash_string(const char *value) {
    unsigned long hash = 5381;
    size_t index;

    for (index = 0; value[index] != '\0'; ++index) {
        hash = ((hash << 5) + hash) + (unsigned long) (unsigned char) value[index];
    }

    return hash;
}

static int append_offset_to_node(EqualityNode *node, long offset) {
    long *new_offsets;

    if (node->count >= node->capacity) {
        node->capacity *= 2;
        new_offsets = (long *) realloc(node->offsets, (size_t) node->capacity * sizeof(long));
        if (new_offsets == NULL) {
            fprintf(stderr, "Error: Memory allocation failed.\n");
            return FAILURE;
        }
        node->offsets = new_offsets;
    }

    node->offsets[node->count] = offset;
    node->count++;
    return SUCCESS;
}

int index_build_equality(
    char ***rows,
    const long *offsets,
    int row_count,
    int column_index,
    EqualityIndex *out
) {
    int bucket_count;
    int row_index;
    unsigned long hash_value;
    EqualityNode *node;

    if (out == NULL) {
        return FAILURE;
    }

    memset(out, 0, sizeof(*out));
    bucket_count = row_count > 0 ? row_count * 2 + 1 : 17;
    out->bucket_count = bucket_count;
    out->column_index = column_index;
    out->buckets = (EqualityNode **) calloc((size_t) bucket_count, sizeof(EqualityNode *));
    if (out->buckets == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }

    for (row_index = 0; row_index < row_count; ++row_index) {
        hash_value = hash_string(rows[row_index][column_index]) % (unsigned long) bucket_count;
        node = out->buckets[hash_value];

        while (node != NULL && strcmp(node->key, rows[row_index][column_index]) != 0) {
            node = node->next;
        }

        if (node == NULL) {
            node = (EqualityNode *) calloc(1, sizeof(EqualityNode));
            if (node == NULL) {
                fprintf(stderr, "Error: Memory allocation failed.\n");
                index_free_equality(out);
                return FAILURE;
            }

            utils_safe_strcpy(node->key, sizeof(node->key), rows[row_index][column_index]);
            node->capacity = INITIAL_DYNAMIC_CAPACITY;
            node->offsets = (long *) calloc((size_t) node->capacity, sizeof(long));
            if (node->offsets == NULL) {
                fprintf(stderr, "Error: Memory allocation failed.\n");
                free(node);
                index_free_equality(out);
                return FAILURE;
            }

            node->next = out->buckets[hash_value];
            out->buckets[hash_value] = node;
        }

        if (append_offset_to_node(node, offsets[row_index]) != SUCCESS) {
            index_free_equality(out);
            return FAILURE;
        }
    }

    return SUCCESS;
}

int index_query_equals(const EqualityIndex *index, const char *value, long **offsets, int *match_count) {
    unsigned long hash_value;
    EqualityNode *node;

    if (offsets == NULL || match_count == NULL) {
        return FAILURE;
    }

    *offsets = NULL;
    *match_count = 0;

    if (index == NULL || index->buckets == NULL) {
        return FAILURE;
    }

    hash_value = hash_string(value) % (unsigned long) index->bucket_count;
    node = index->buckets[hash_value];
    while (node != NULL && strcmp(node->key, value) != 0) {
        node = node->next;
    }

    if (node == NULL) {
        return SUCCESS;
    }

    *offsets = (long *) malloc((size_t) node->count * sizeof(long));
    if (*offsets == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }

    memcpy(*offsets, node->offsets, (size_t) node->count * sizeof(long));
    *match_count = node->count;
    return SUCCESS;
}

void index_free_equality(EqualityIndex *index) {
    int bucket_index;
    EqualityNode *node;
    EqualityNode *next;

    if (index == NULL || index->buckets == NULL) {
        return;
    }

    for (bucket_index = 0; bucket_index < index->bucket_count; ++bucket_index) {
        node = index->buckets[bucket_index];
        while (node != NULL) {
            next = node->next;
            free(node->offsets);
            node->offsets = NULL;
            free(node);
            node = next;
        }
    }

    free(index->buckets);
    index->buckets = NULL;
}

static int compare_range_entry_numeric(const void *left, const void *right) {
    const RangeEntry *left_entry = (const RangeEntry *) left;
    const RangeEntry *right_entry = (const RangeEntry *) right;

    if (left_entry->numeric_value < right_entry->numeric_value) {
        return -1;
    }
    if (left_entry->numeric_value > right_entry->numeric_value) {
        return 1;
    }
    if (left_entry->offset < right_entry->offset) {
        return -1;
    }
    if (left_entry->offset > right_entry->offset) {
        return 1;
    }
    return 0;
}

static int compare_range_entry_string(const void *left, const void *right) {
    const RangeEntry *left_entry = (const RangeEntry *) left;
    const RangeEntry *right_entry = (const RangeEntry *) right;
    int cmp = strcmp(left_entry->value, right_entry->value);

    if (cmp != 0) {
        return cmp;
    }

    if (left_entry->offset < right_entry->offset) {
        return -1;
    }
    if (left_entry->offset > right_entry->offset) {
        return 1;
    }
    return 0;
}

int index_build_range(
    char ***rows,
    const long *offsets,
    int row_count,
    int column_index,
    int prefer_numeric,
    RangeIndex *out
) {
    int row_index;
    int all_numeric = 1;

    if (out == NULL) {
        return FAILURE;
    }

    memset(out, 0, sizeof(*out));
    out->column_index = column_index;
    out->entry_count = row_count;
    out->entries = (RangeEntry *) calloc((size_t) row_count, sizeof(RangeEntry));
    if (out->entries == NULL && row_count > 0) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }

    for (row_index = 0; row_index < row_count; ++row_index) {
        utils_safe_strcpy(out->entries[row_index].value, sizeof(out->entries[row_index].value), rows[row_index][column_index]);
        out->entries[row_index].offset = offsets[row_index];
        out->entries[row_index].is_numeric = utils_is_integer(rows[row_index][column_index]);
        if (out->entries[row_index].is_numeric) {
            out->entries[row_index].numeric_value = strtoll(rows[row_index][column_index], NULL, 10);
        } else {
            all_numeric = 0;
        }
    }

    out->use_numeric = prefer_numeric && all_numeric;
    if (row_count > 1) {
        qsort(
            out->entries,
            (size_t) row_count,
            sizeof(RangeEntry),
            out->use_numeric ? compare_range_entry_numeric : compare_range_entry_string
        );
    }

    return SUCCESS;
}

static int compare_entry_with_value(const RangeIndex *index, const RangeEntry *entry, const char *value) {
    long long numeric_value;

    if (index->use_numeric) {
        numeric_value = strtoll(value, NULL, 10);
        if (entry->numeric_value < numeric_value) {
            return -1;
        }
        if (entry->numeric_value > numeric_value) {
            return 1;
        }
        return 0;
    }

    return strcmp(entry->value, value);
}

static int lower_bound_entry(const RangeIndex *index, const char *value) {
    int low = 0;
    int high = index->entry_count;
    int mid;

    while (low < high) {
        mid = low + (high - low) / 2;
        if (compare_entry_with_value(index, &index->entries[mid], value) < 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    return low;
}

static int upper_bound_entry(const RangeIndex *index, const char *value) {
    int low = 0;
    int high = index->entry_count;
    int mid;

    while (low < high) {
        mid = low + (high - low) / 2;
        if (compare_entry_with_value(index, &index->entries[mid], value) <= 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    return low;
}

int index_query_range(const RangeIndex *index, const char *op, const char *value, long **offsets, int *match_count) {
    int start = 0;
    int end = 0;
    int out_index = 0;
    int entry_index;

    if (offsets == NULL || match_count == NULL) {
        return FAILURE;
    }

    *offsets = NULL;
    *match_count = 0;

    if (index == NULL) {
        return FAILURE;
    }

    if (index->entry_count == 0) {
        return SUCCESS;
    }

    *offsets = (long *) malloc((size_t) index->entry_count * sizeof(long));
    if (*offsets == NULL) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return FAILURE;
    }

    if (strcmp(op, "=") == 0) {
        start = lower_bound_entry(index, value);
        end = upper_bound_entry(index, value);
        for (entry_index = start; entry_index < end; ++entry_index) {
            (*offsets)[out_index++] = index->entries[entry_index].offset;
        }
    } else if (strcmp(op, ">") == 0) {
        start = upper_bound_entry(index, value);
        for (entry_index = start; entry_index < index->entry_count; ++entry_index) {
            (*offsets)[out_index++] = index->entries[entry_index].offset;
        }
    } else if (strcmp(op, ">=") == 0) {
        start = lower_bound_entry(index, value);
        for (entry_index = start; entry_index < index->entry_count; ++entry_index) {
            (*offsets)[out_index++] = index->entries[entry_index].offset;
        }
    } else if (strcmp(op, "<") == 0) {
        end = lower_bound_entry(index, value);
        for (entry_index = 0; entry_index < end; ++entry_index) {
            (*offsets)[out_index++] = index->entries[entry_index].offset;
        }
    } else if (strcmp(op, "<=") == 0) {
        end = upper_bound_entry(index, value);
        for (entry_index = 0; entry_index < end; ++entry_index) {
            (*offsets)[out_index++] = index->entries[entry_index].offset;
        }
    } else if (strcmp(op, "!=") == 0) {
        for (entry_index = 0; entry_index < index->entry_count; ++entry_index) {
            if (compare_entry_with_value(index, &index->entries[entry_index], value) != 0) {
                (*offsets)[out_index++] = index->entries[entry_index].offset;
            }
        }
    } else {
        fprintf(stderr, "Error: Unsupported WHERE operator '%s'.\n", op);
        free(*offsets);
        *offsets = NULL;
        return FAILURE;
    }

    *match_count = out_index;
    return SUCCESS;
}

void index_free_range(RangeIndex *index) {
    if (index == NULL) {
        return;
    }

    free(index->entries);
    index->entries = NULL;
    index->entry_count = 0;
}
