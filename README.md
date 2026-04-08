# SQL Processor

파일 기반 CSV 저장소 위에서 동작하는 경량 SQL 처리기입니다. `INSERT`와 `SELECT`를 지원하며, SQL 입력은 파일 모드와 REPL 모드 둘 다 사용할 수 있습니다.

## 빌드

```bash
make
```

## 실행

```bash
./sql_processor
./sql_processor tests/test_cases/basic_select.sql
```

## 지원 구문

```sql
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 27;
```

## 테스트

```bash
make test
```

## Docker

이미지 빌드:

```bash
docker build -t sql-processor .
```

REPL 실행:

```bash
docker run --rm -it sql-processor
```

SQL 파일 실행:

```bash
docker run --rm -it sql-processor ./sql_processor tests/test_cases/basic_select.sql
```

Docker 안에서 테스트 실행:

```bash
docker run --rm -it sql-processor make test
```

실전 전에 데이터 초기화가 필요하면 이미지 재빌드 없이 새 컨테이너를 띄우면 됩니다. `.dockerignore`에서 `data/*.csv`를 제외했기 때문에 컨테이너는 깨끗한 데이터 상태로 시작합니다.

## 저장 형식

- 테이블 파일: `data/<table>.csv`
- 첫 줄: 헤더
- 이후 줄: 데이터 레코드

## 프로젝트 구조

- `src/`: 파서, 실행기, 스토리지, 인덱스, 유틸
- `tests/`: 단위 테스트와 기능 테스트
- `data/`: CSV 테이블 저장소
