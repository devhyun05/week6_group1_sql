# SQL 처리기 (SQL Processor) — 요구사항 명세서

> **언어**: C (표준 C99 이상)
> **빌드**: `gcc` + `Makefile`
> **실행 환경**: Docker (`ubuntu:latest`)

---

## 1. 시스템 개요

SQL문을 입력받아 **파싱 → 실행 → 파일 I/O**를 수행하는 경량 SQL 처리기.

이 시스템은 **DB 서버도, 인메모리 엔진도 없다.** 데이터의 저장과 조회는 전부 **파일 읽기/쓰기**만으로 동작한다. 하나의 테이블은 곧 하나의 파일이며, 그 이상도 이하도 아니다.

```
┌─────────────────┐     ┌─────────────┐     ┌───────────┐     ┌────────────┐
│ SQL 입력         │     │   Parser    │     │ Executor  │     │ File Store │
│ (파일 or REPL)   │ ──▶ │ Soft → Hard │ ──▶ │           │ ──▶ │ (CSV 파일)  │
└─────────────────┘     └─────────────┘     └───────────┘     └────────────┘
                                                  │
                                                  ▼
                                            ┌───────────┐
                                            │  stdout    │
                                            │ (결과 출력) │
                                            └───────────┘
```

---

## 2. 전제 조건

이 프로젝트에서 **구현하지 않는 것**을 명확히 한다:

- `CREATE TABLE`은 구현하지 않는다.
- 별도의 스키마 정의 파일(`.schema`, `.meta` 등)은 만들지 않는다.
- 테이블의 구조(컬럼명, 컬럼 수)는 **첫 INSERT 시 컬럼 목록**으로부터 결정되거나, 이미 존재하는 **CSV 파일의 헤더 행**으로부터 읽어온다.
- 별도의 메타데이터 파일, 인덱스 파일은 기본 요구사항이 아니다 (WHERE 최적화를 위한 인메모리 인덱스는 별도, 아래 참조).
- 데이터 타입 검증(정수인지 문자열인지)은 최소한만 수행한다.

**실행 환경**:

- 빌드, 실행, 테스트는 동일한 **Docker 컨테이너** 환경을 기준으로 수행한다.
- 베이스 이미지: `ubuntu:latest`
- 로컬 개발은 자유롭게 하되, 최종 검증 기준은 Docker 환경으로 통일한다.

---

## 3. CLI 인터페이스

`main()`은 `argc`, `argv`를 사용하여 실행 모드를 결정한다.

### 3.1 파일 모드

```bash
./sql_processor <파일경로.sql>
```

- `argv[1]`로 SQL 파일 경로를 받는다.
- 파일 내 SQL문을 세미콜론(`;`) 기준으로 분리하여 순차 실행한다.
- 모든 SQL문 실행 후 프로그램 종료.

### 3.2 대화형 쉘 (Interactive REPL) 모드 — 필수

```bash
./sql_processor
SQL> INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
1 row inserted into users.
SQL> SELECT * FROM users;
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
|  1 | Alice |  30 |
+----+-------+-----+
1 row selected.
SQL> exit
Bye.
```

- CLI 인자(`argv[1]`)가 없을 경우, 프로그램이 종료되지 않고 `SQL> ` 프롬프트를 띄운다.
- 사용자가 `exit`를 입력할 때까지 연속해서 명령을 처리한다.
- 각 SQL문은 세미콜론(`;`)으로 끝나야 실행된다.
- 여러 줄에 걸친 SQL 입력을 지원한다 (세미콜론이 나올 때까지 입력을 이어 받음).

### 3.3 입력 파일 형식

- 확장자: `.sql`
- 인코딩: UTF-8
- 하나의 파일에 여러 SQL문 포함 가능
- 각 SQL문은 세미콜론(`;`)으로 구분
- 줄바꿈, 여러 줄에 걸친 SQL문 허용

**예시 (`test.sql`)**:
```sql
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 27;
```

---

## 4. SQL 파싱 (Parser) — 소프트 파서 / 하드 파서

파서는 **두 단계**로 분리하여 구현한다. 코드상에서도 별도의 함수(또는 파일)로 나뉘어야 한다.

### 4.1 소프트 파서 (Soft Parser — Tokenizer/Lexer)

SQL 문자열을 의미 단위의 **토큰 배열**로 분해하는 1차 처리.

**역할**:
- 앞뒤 공백/개행 제거 (trim)
- SQL 문자열을 토큰 단위로 분리
- 토큰 유형 분류: 키워드(`INSERT`, `SELECT`, `INTO`, `FROM`, `WHERE`, `VALUES`), 식별자(테이블명, 컬럼명), 리터럴(정수, 문자열), 연산자(`=`, `>`, `<`, `!=`, `>=`, `<=`), 구두점(`(`, `)`, `,`, `;`)
- 대소문자 무관 처리 (키워드는 내부적으로 대문자로 정규화)
- 작은따옴표 내부의 쉼표/공백은 문자열 리터럴의 일부로 보존

**토큰 구조체**:
```c
typedef enum {
    TOKEN_KEYWORD,      // INSERT, SELECT, INTO, FROM, WHERE, VALUES
    TOKEN_IDENTIFIER,   // 테이블명, 컬럼명
    TOKEN_INT_LITERAL,  // 정수 값
    TOKEN_STR_LITERAL,  // 문자열 값 (작은따옴표 제거된 상태)
    TOKEN_OPERATOR,     // =, !=, >, <, >=, <=
    TOKEN_LPAREN,       // (
    TOKEN_RPAREN,       // )
    TOKEN_COMMA,        // ,
    TOKEN_SEMICOLON,    // ;
    TOKEN_UNKNOWN
} TokenType;

typedef struct {
    TokenType type;
    char value[256];
} Token;

// 소프트 파서 인터페이스
// 반환: 토큰 배열 (동적 할당), token_count에 토큰 수 저장
// 호출자가 free 책임
Token *soft_parse(const char *sql, int *token_count);
```

### 4.2 하드 파서 (Hard Parser — Syntax Analyzer)

토큰 배열을 분석하여 **구조체(`SqlStatement`)**로 변환하는 2차 처리.

**역할**:
- 토큰 배열의 첫 키워드로 SQL 유형 판별 (`INSERT` / `SELECT`)
- 유형별 전용 파싱 함수 호출
- 토큰 순서가 문법에 맞는지 검증
- 문법 오류 시 에러 메시지를 `stderr`로 출력 후 해당 문장 스킵 (프로그램 종료하지 않음)

**결과 구조체**:
```c
typedef enum {
    SQL_INSERT,
    SQL_SELECT
} SqlType;

typedef struct {
    char table_name[64];
    int  column_count;
    char columns[32][64];   // 컬럼명 배열
    char values[32][256];   // 값 배열 (문자열로 저장)
} InsertStatement;

typedef struct {
    char column[64];
    char op[4];             // "=", "!=", ">", "<", ">=", "<="
    char value[256];
} WhereClause;

typedef struct {
    char table_name[64];
    int  column_count;      // 0이면 SELECT *
    char columns[32][64];
    int  has_where;         // 0 or 1
    WhereClause where;
} SelectStatement;

typedef struct {
    SqlType type;
    union {
        InsertStatement insert;
        SelectStatement select;
    };
} SqlStatement;

// 하드 파서 인터페이스
// 성공 시 0, 실패 시 -1 반환
int hard_parse(const Token *tokens, int token_count, SqlStatement *out);
```

### 4.3 필수 지원 구문

#### INSERT

```sql
INSERT INTO <테이블명> (<컬럼1>, <컬럼2>, ...) VALUES (<값1>, <값2>, ...);
```

- 값 타입: 정수(`int`), 문자열(`'...'` 작은따옴표로 감싼 문자열)
- 컬럼 수와 값 수 불일치 시 에러 처리

#### SELECT

```sql
-- 전체 조회
SELECT * FROM <테이블명>;

-- 특정 컬럼 조회
SELECT <컬럼1>, <컬럼2> FROM <테이블명>;

-- WHERE 조건 조회
SELECT <컬럼들> FROM <테이블명> WHERE <컬럼> <연산자> <값>;
```

- 연산자: `=`, `!=`, `>`, `<`, `>=`, `<=`
- WHERE 조건은 단일 조건 지원 (AND/OR는 추가 구현)

---

## 5. 실행 엔진 (Executor)

Executor는 **절대로 파일에 직접 접근하지 않는다.** 모든 데이터 접근은 반드시 Storage 인터페이스 함수를 통해서만 수행한다 (6장 참조).

### 5.1 INSERT 실행

1. `InsertStatement`를 받아 `storage_insert()` 호출
2. 성공 시: `"1 row inserted into <테이블명>."` 출력 (`stdout`)

### 5.2 SELECT 실행

1. `SelectStatement`를 받아 `storage_select()` 호출
2. `SELECT *` → 모든 컬럼 출력
3. 특정 컬럼 지정 시 해당 컬럼만 출력
4. WHERE 조건 있으면 필터링 적용 (인덱스/해시테이블 활용, 5.3 참조)
5. 결과를 MySQL 스타일 테이블 형태로 출력:

```
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
|  1 | Alice |  30 |
|  2 | Bob   |  25 |
+----+-------+-----+
2 rows selected.
```

### 5.3 WHERE 필터링 — 인덱싱/오프셋/해시테이블

WHERE 절 처리 시 단순 순차 스캔(full scan)만으로 끝내지 않는다. 다음 메커니즘을 설계·구현한다:

- **해시테이블 기반 검색**: 등호 조건(`=`)에 대해, SELECT 실행 시 해당 컬럼의 값을 키로 하는 해시테이블을 인메모리에 구축하여 O(1) 조회를 수행한다.
- **오프셋 기반 접근**: 파일 내 각 행의 바이트 오프셋을 기록하여, 조건에 맞는 행만 `fseek`로 직접 접근할 수 있게 한다.
- **인덱싱**: 자주 조회되는 컬럼에 대해 인메모리 인덱스(정렬 배열 또는 해시맵)를 구축하여 범위 조건(`>`, `<`, `>=`, `<=`)도 효율적으로 처리한다.

> 인덱스는 **인메모리**로 구축하며, 별도의 인덱스 파일을 디스크에 저장하는 것은 기본 요구사항이 아니다.

---

## 6. 파일 기반 저장소 (File Store)

### 6.1 핵심 원칙: 테이블 = 파일

이 프로젝트에서 **테이블은 곧 하나의 파일**이다.

- DB 서버 없음. 인메모리 엔진 없음.
- 모든 데이터는 `data/<테이블명>.csv` 파일에 저장된다.
- 파일이 곧 데이터베이스의 전부이다.

```
data/
├── users.csv
├── products.csv
└── orders.csv
```

### 6.2 파일 구조

```csv
id,name,age
1,Alice,30
2,Bob,25
```

- 첫 행: 컬럼 헤더 (테이블의 스키마 역할)
- 이후 행: 데이터 (한 행 = 한 레코드)
- 문자열 값에 쉼표 포함 시 `"..."` 큰따옴표로 감싸기
- 파일이 존재하지 않으면 첫 INSERT 시 헤더 행과 함께 새로 생성

### 6.3 스토리지 인터페이스 — 반드시 이 함수를 통해서만 파일에 접근

Executor, Parser, main 등 **다른 모듈은 절대로 `fopen`/`fread`/`fwrite` 등으로 데이터 파일에 직접 접근하지 않는다.** 모든 파일 접근은 아래 인터페이스 함수를 통해서만 이루어진다.

```c
// 테이블 파일에 한 행 추가
// 성공 시 0, 실패 시 -1
int storage_insert(const char *table_name, const InsertStatement *stmt);

// 테이블 파일에서 전체 데이터 읽기
// 반환: 행 배열 (동적 할당), row_count에 행 수, col_count에 컬럼 수 저장
// 호출자가 storage_free_rows()로 해제할 책임
char ***storage_select(const char *table_name, int *row_count, int *col_count);

// 테이블의 컬럼 헤더 읽기
int storage_get_columns(const char *table_name, char columns[][64], int *col_count);

// 메모리 해제
void storage_free_rows(char ***rows, int row_count, int col_count);
```

### 6.4 파일 락 (File Locking)

동시에 여러 프로세스(또는 여러 REPL 인스턴스)가 같은 테이블 파일에 INSERT하면 파일이 깨질 수 있다. 이를 방지하기 위해 파일 락을 구현한다.

- **쓰기(INSERT)**: `flock()` 또는 `fcntl()`로 배타적 락(exclusive lock)을 획득한 후 쓰기, 완료 후 해제.
- **읽기(SELECT)**: 공유 락(shared lock) 획득 또는 락 없이 허용 (설계 선택).
- 락 획득 실패 시 에러 메시지 출력 후 재시도하거나 해당 SQL문 스킵.

```c
#include <sys/file.h>

// 쓰기 시 배타적 락 예시
int fd = fileno(fp);
flock(fd, LOCK_EX);   // 배타적 락 획득
// ... 쓰기 수행 ...
flock(fd, LOCK_UN);   // 락 해제
```

---

## 7. 프로젝트 구조

```
sql-processor/
├── Dockerfile
├── Makefile
├── README.md
├── src/
│   ├── main.c              # CLI 진입점: argc/argv 처리, 파일 모드 / REPL 모드 분기
│   ├── soft_parser.c       # 소프트 파서: SQL → 토큰 배열
│   ├── soft_parser.h
│   ├── hard_parser.c       # 하드 파서: 토큰 배열 → SqlStatement 구조체
│   ├── hard_parser.h
│   ├── executor.c          # SQL 실행 로직 (storage 인터페이스만 호출)
│   ├── executor.h
│   ├── storage.c           # 파일 I/O (CSV 읽기/쓰기, 파일 락)
│   ├── storage.h
│   ├── index.c             # 인메모리 인덱스/해시테이블 (WHERE 최적화)
│   ├── index.h
│   ├── utils.c             # 문자열 유틸 (trim, to_upper 등)
│   └── utils.h
├── data/                   # 테이블 데이터 파일 (CSV)
├── tests/
│   ├── test_soft_parser.c  # 소프트 파서 단위 테스트
│   ├── test_hard_parser.c  # 하드 파서 단위 테스트
│   ├── test_executor.c     # 실행기 단위 테스트
│   ├── test_storage.c      # 스토리지 단위 테스트
│   ├── test_cases/         # 기능 테스트용 SQL 파일들
│   │   ├── basic_insert.sql
│   │   ├── basic_select.sql
│   │   ├── select_where.sql
│   │   └── edge_cases.sql
│   └── run_tests.sh        # 테스트 자동 실행 스크립트
└── docs/
    └── SPEC.md             # 이 문서
```

---

## 8. Makefile

```makefile
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g
SRC_DIR = src
BUILD_DIR = build
TARGET = sql_processor

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

.PHONY: all clean
```

---

## 9. 환경 설정 (Docker)

빌드, 실행, 테스트는 아래 Docker 환경에서 수행한다.

### 9.1 Dockerfile

```dockerfile
FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    gcc \
    make \
    valgrind \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

CMD ["bash"]
```

### 9.2 사용법

```bash
# 이미지 빌드
docker build -t sql-processor .

# 컨테이너 실행 (대화형)
docker run -it sql-processor

# 컨테이너 내부에서 빌드 & 실행
make
./sql_processor                    # REPL 모드
./sql_processor tests/test_cases/basic_insert.sql  # 파일 모드

# 메모리 누수 검사
valgrind --leak-check=full ./sql_processor tests/test_cases/basic_insert.sql
```

---

## 10. 코드 품질 요구사항

이 프로젝트의 모든 C 코드는 아래 규칙을 반드시 준수한다.

### 10.1 가독성

- 초보자가 읽어도 이해하기 쉽게 작성한다.
- 함수 단위로 역할을 분리한다. 하나의 함수가 200줄을 넘지 않도록 한다.
- 핵심 로직에는 반드시 주석을 추가한다.
- 하드코딩을 최소화한다. 배열 크기, 최대 컬럼 수 등은 `#define` 상수로 정의한다.
- 일관된 네이밍: `snake_case` 통일, 모듈별 접두어 사용 (`soft_parse_`, `hard_parse_`, `executor_`, `storage_`).
- 함수 상단에 역할, 파라미터 설명, 반환값 의미, 메모리 소유권을 짧게 기술한다.

### 10.2 안전한 C 코드 스타일

**금지 함수 → 대체 함수**:

| 금지 | 대체 | 이유 |
|------|------|------|
| `strcpy` | `strncpy` | 버퍼 오버플로우 방지 |
| `sprintf` | `snprintf` | 버퍼 오버플로우 방지 |
| `scanf("%s", ...)` | `fgets` | 입력 길이 제한 불가 |
| `gets` | `fgets` | 절대 사용 금지 (C11에서 삭제됨) |

### 10.3 메모리 관리

- 모든 `malloc`/`calloc`/`realloc` 호출 후 `NULL` 체크 필수. 할당 실패 시 에러 메시지 출력 후 안전하게 반환.
- `free` 후 포인터를 `NULL`로 설정하여 dangling pointer 방지.
- 메모리 소유권 규칙을 명확히 한다: 동적 할당 메모리를 반환하는 함수는 "호출자가 free할 책임"임을 주석으로 명시.
- `strtok`은 원본 문자열을 훼손한다. 원본 보존이 필요하면 `strdup`으로 복사 후 토크나이징, 복사본은 이후 `free`.
- `valgrind --leak-check=full`로 메모리 누수 0을 목표로 한다.

### 10.4 파일 I/O

- `fopen` 반환값 `NULL` 체크 필수.
- 파일 사용 후 반드시 `fclose`. 에러 경로(early return)에서도 닫히도록 `goto cleanup` 패턴을 사용한다.
- 쓰기 직후 `fflush` 또는 `fclose`로 버퍼를 디스크에 반영.

### 10.5 에러 처리

- 에러 메시지는 `stderr`로 출력한다 (`fprintf(stderr, ...)`). 정상 결과는 `stdout`.
- 에러 발생 시 해당 SQL문만 스킵하고 다음 SQL문을 계속 처리한다 (프로그램을 종료하지 않음).
- 함수 반환값으로 성공/실패를 구분한다: `0` = 성공, `-1` = 실패. 매직넘버 대신 `#define SUCCESS 0`, `#define FAILURE -1`.
- 실패 시 반환값 검사를 철저히 수행한다.

### 10.6 문자열 파싱 주의사항

- 작은따옴표 내부의 쉼표(`'Lee, Jr.'`)는 구분자로 취급하지 않는 파싱 로직을 구현한다. 따옴표 상태를 추적하는 별도 로직이 필요하다.
- `strtok` 또는 직접 파서를 구현하여 토크나이징한다.
- 입력 길이 검증: SQL 한 문장의 최대 길이, 컬럼 수 최대값 등을 상수로 정의하고 초과 시 에러 처리.

### 10.7 계층 분리

- **Parser** → Storage에 접근하지 않는다.
- **Executor** → Storage 인터페이스 함수만 호출한다. 직접 `fopen` 금지.
- **Storage** → 파일 I/O를 캡슐화한다. 다른 모듈은 파일의 존재조차 알 필요 없다.
- **main** → 모드 분기(파일/REPL)와 SQL문 분리만 담당한다.

---

## 11. 핵심 구현 포인트 (코드 리뷰 / 발표 대비)

반드시 이해하고 설명할 수 있어야 하는 부분:

### 11.1 소프트 파서 — 토크나이징

- SQL 문자열을 토큰 배열로 분해하는 과정
- 작은따옴표 문자열 리터럴 처리 (내부 쉼표/공백 보존)
- **왜 중요**: 컴파일러의 lexer와 동일한 원리. "문자열 → 토큰"이 왜 필요한지 설명할 수 있어야 함.

### 11.2 하드 파서 — 구문 분석

- 토큰 배열을 순회하며 문법 규칙에 따라 구조체로 변환
- INSERT/SELECT 각각의 문법 패턴 매칭
- **왜 중요**: 컴파일러의 parser와 동일한 원리. 토큰 → 구조화된 데이터(AST의 축소판)로의 변환을 설명할 수 있어야 함.

### 11.3 WHERE 최적화 — 해시테이블/인덱스

- 등호 조건에 해시테이블을 사용한 O(1) 검색
- 범위 조건에 정렬 배열 인덱스를 사용한 효율적 검색
- **왜 중요**: 실제 RDBMS의 쿼리 옵티마이저가 인덱스를 활용하는 원리의 축소판.

### 11.4 저장소 — 파일 I/O와 락

- `fopen`, `fprintf`, `fgets`를 사용한 행 단위 읽기/쓰기
- CSV 파싱 시 쉼표/따옴표 처리 (엣지 케이스 다수)
- 동적 메모리 할당 (`malloc`/`realloc`/`free`)으로 가변 행 수 처리
- `flock()`을 이용한 동시 접근 보호
- **왜 중요**: C에서 파일 I/O + 동적 메모리 + 동시성 제어는 가장 버그가 많이 발생하는 영역.

### 11.5 실행기 — 분기와 계층 분리

- `SqlType`에 따라 INSERT/SELECT 함수를 분기하는 dispatcher 패턴
- Storage 인터페이스만 호출하여 계층 간 의존성을 깔끔하게 유지
- **왜 중요**: 실제 소프트웨어에서 모듈 간 의존성 관리의 기본. "왜 Executor가 직접 fopen하면 안 되는가?"를 설명할 수 있어야 함.

---

## 12. 테스트 케이스

### 12.1 기본 기능 테스트

| # | SQL | 기대 결과 |
|---|-----|-----------|
| 1 | `INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);` | `1 row inserted into users.` + 파일에 행 추가 |
| 2 | `INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);` | `1 row inserted into users.` |
| 3 | `SELECT * FROM users;` | 2행 테이블 출력 |
| 4 | `SELECT name FROM users;` | name 컬럼만 출력 |
| 5 | `SELECT * FROM users WHERE age > 27;` | Alice 행만 출력 |
| 6 | `SELECT * FROM users WHERE name = 'Bob';` | Bob 행만 출력 (해시테이블 경로) |

### 12.2 엣지 케이스 테스트

| # | 케이스 | 기대 동작 |
|---|--------|-----------|
| 1 | 존재하지 않는 테이블 SELECT | `"Error: Table 'xxx' not found."` |
| 2 | 컬럼 수 != 값 수 INSERT | `"Error: Column count doesn't match value count."` |
| 3 | 빈 SQL 파일 | 아무 동작 없이 정상 종료 |
| 4 | 세미콜론 없는 SQL | 에러 메시지 or 마지막 문장으로 처리 |
| 5 | 문자열에 쉼표 포함 `'Lee, Jr.'` | 올바르게 하나의 값으로 처리 |
| 6 | 대소문자 혼용 `insert INTO`, `Select` | 정상 동작 |
| 7 | 여러 줄에 걸친 SQL문 | 세미콜론 기준으로 올바르게 분리 |
| 8 | 연속 공백, 탭 문자 | 정상 파싱 |
| 9 | 빈 테이블에 SELECT | 헤더만 출력, `0 rows selected.` |
| 10 | 매우 긴 문자열 값 (255자) | 버퍼 오버플로우 없이 처리 |
| 11 | REPL에서 여러 줄 SQL 입력 | 세미콜론 나올 때까지 이어 받아서 실행 |
| 12 | 동시 INSERT (두 프로세스) | 파일 락으로 데이터 깨짐 없이 처리 |

### 12.3 테스트 실행 스크립트 구조

```bash
#!/bin/bash
# tests/run_tests.sh

echo "=== SQL Processor Test Suite ==="
PASS=0
FAIL=0

run_test() {
    local test_name=$1
    local sql_file=$2
    local expected=$3

    actual=$(./sql_processor "$sql_file" 2>&1)
    if echo "$actual" | grep -q "$expected"; then
        echo "[PASS] $test_name"
        ((PASS++))
    else
        echo "[FAIL] $test_name"
        echo "  Expected: $expected"
        echo "  Actual:   $actual"
        ((FAIL++))
    fi
}

# 테스트 실행 전 data 디렉토리 초기화
rm -rf data && mkdir -p data

run_test "Basic INSERT" "tests/test_cases/basic_insert.sql" "1 row inserted"
run_test "Basic SELECT" "tests/test_cases/basic_select.sql" "Alice"
run_test "WHERE equals" "tests/test_cases/select_where.sql" "Bob"
# ... 추가 테스트

echo ""
echo "Results: $PASS passed, $FAIL failed"
```

---

## 13. I/O 명세 요약

| 구분 | 포맷 | 위치 / 방식 | 비고 |
|------|------|-------------|------|
| **SQL 입력 (파일 모드)** | `.sql` 텍스트 파일 | `argv[1]`로 경로 전달 | 세미콜론(`;`)으로 문장 구분, UTF-8 |
| **SQL 입력 (REPL 모드)** | 표준 입력 (`stdin`) | `SQL> ` 프롬프트에서 직접 입력 | `exit`로 종료, 여러 줄 입력 지원 |
| **데이터 저장** | `.csv` 파일 | `data/<테이블명>.csv` | 첫 행 = 컬럼 헤더, 이후 행 = 레코드 |
| **조회 결과 출력** | 터미널 텍스트 | `stdout` | MySQL 스타일 테이블 포맷 (`+---+`) |
| **에러 메시지** | 터미널 텍스트 | `stderr` | 에러 시 해당 SQL문만 스킵 |
| **파일 락** | OS 레벨 | `flock()` — 쓰기 시 배타적 락 | 동시 INSERT 시 파일 깨짐 방지 |
