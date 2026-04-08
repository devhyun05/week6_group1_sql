# SQL Processor

파일 기반 CSV 저장소 위에서 동작하는 경량 SQL 처리기입니다. `INSERT`와 `SELECT`를 지원하며, SQL 입력을 토크나이징(soft parser)과 구문 분석(hard parser)으로 나누어 처리합니다.

## Build

```bash
make
```

## Run

```bash
./sql_processor
./sql_processor tests/test_cases/basic_select.sql
```

예시 REPL 입력:

```sql
INSERT INTO users (name, age) VALUES ('Alice', 30);
INSERT INTO users (name, age) VALUES ('Bob', 25);
SELECT * FROM users;
```

## Test

```bash
make tests
```

## Docker

```bash
docker build -t sql-processor .
docker run -it --rm sql-processor bash -lc "make && ./sql_processor"
```
