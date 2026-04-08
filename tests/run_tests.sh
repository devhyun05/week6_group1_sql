#!/bin/bash
set -u

PASS=0
FAIL=0

run_unit_test() {
    local bin="$1"

    if "$bin" >/tmp/sql_processor_test.out 2>&1; then
        echo "[PASS] $(basename "$bin")"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] $(basename "$bin")"
        cat /tmp/sql_processor_test.out
        FAIL=$((FAIL + 1))
    fi
}

run_sql_test() {
    local name="$1"
    local sql_file="$2"
    local expected="$3"

    rm -f data/*.csv
    if ./sql_processor "$sql_file" >/tmp/sql_processor_sql.out 2>&1 && grep -q "$expected" /tmp/sql_processor_sql.out; then
        echo "[PASS] $name"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] $name"
        cat /tmp/sql_processor_sql.out
        FAIL=$((FAIL + 1))
    fi
}

mkdir -p data

echo "=== Unit Tests ==="
for bin in build/tests/test_*; do
    if [ -f "$bin" ] && [ -x "$bin" ]; then
        run_unit_test "$bin"
    fi
done

echo ""
echo "=== SQL Scenarios ==="
run_sql_test "Basic INSERT" "tests/test_cases/basic_insert.sql" "1 row inserted into users."
run_sql_test "Basic SELECT" "tests/test_cases/basic_select.sql" "Alice"
run_sql_test "WHERE equals" "tests/test_cases/select_where.sql" "Bob"
run_sql_test "CSV comma handling" "tests/test_cases/edge_cases.sql" "Lee, Jr."
run_sql_test "Primary key duplicate" "tests/test_cases/duplicate_primary_key.sql" "Error: Duplicate primary key '1' in table 'users'."

echo ""
echo "Results: $PASS passed, $FAIL failed"

if [ "$FAIL" -ne 0 ]; then
    exit 1
fi
