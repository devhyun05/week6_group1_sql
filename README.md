## 프로젝트 개요
이 프로젝트는 C 언어로 구현한 간단한 SQL 처리기(SQL Processor)입니다.
사용자로부터 SQL 쿼리를 입력받아 파싱하고, 이를 실행하여 CSV 파일 기반 저장소에 반영합니다.


## 전체 토크나이저 흐름 (tokenizer_tokenize 함수)
```mermaid
flowchart TD
    A["① SQL 입력\n예: SELECT * FROM users;"]
    B["② 복사 & 정규화\nstrdup → trim → 빈 문자열 체크"]
    C{"③ 캐시 조회\ntokenizer_lookup_cache()"}
    D["캐시 히트\n복제본 즉시 반환"]
    E["④ 실제 파싱\ntokenizer_tokenize_sql()"]
    F["⑤ 캐시 저장\ntokenizer_store_cache()"]
    G{"64개 초과?"}
    H["오래된 항목 evict\nevict_oldest_cache_entry()"]
    I["⑥ 토큰 배열 반환\n호출자가 free() 책임"]

    A --> B
    B --> C
    C -- "히트" --> D
    C -- "미스" --> E
    D --> I
    E --> F
    F --> G
    G -- "예" --> H
    G -- "아니오" --> I
    H --> I
```

## 토큰 타입 분류 (tokenizer_tokenize_sql 내부 분기)
![tokenizer](https://github.com/user-attachments/assets/d6e03310-df0f-4e4f-8659-73f852cb8d36)

## 실행 엔진

아래 다이어그램은 `executor.c`가 파싱된 SQL 문을 받아
`INSERT`, `SELECT`, `DELETE`를 어떻게 실행하는지 보여준다.

```mermaid
flowchart TD
    A["SqlStatement 입력"] --> B["executor_execute()"]
    B --> C{"statement.type 분기"}

    C --> D["INSERT"]
    C --> E["SELECT"]
    C --> F["DELETE"]

    D --> G["storage_insert() 호출"]
    G --> H["1 row inserted 출력"]

    E --> I["storage_load_table()로 CSV 로드"]
    I --> J["조회할 컬럼 준비"]
    J --> K{"WHERE 존재?"}

    K -->|No| L["모든 행 수집"]
    K -->|Yes| M["index_build()로 인메모리 인덱스 생성"]
    M --> N["조건에 맞는 row offset 조회"]
    N --> O["storage_read_row_at_offset()로 필요한 행만 읽기"]

    L --> P["표 형태로 출력"]
    O --> P
    P --> Q["N rows selected 출력"]

    F --> R["storage_delete() 호출"]
    R --> S["N rows deleted 출력"]
```
## 테스트는 어떻게 했는지?

테스트는 네 단계로 나눴다.

| 분류 | 목적 |
| --- | --- |
| Unit Test | 토크나이저, 파서, 스토리지, 실행기 같은 모듈 단위 검증 |
| Integration Test | tokenizer -> parser -> executor -> storage가 연결돼서 잘 동작하는지 확인 |
| Functional Test | INSERT, SELECT, DELETE, WHERE 같은 실제 SQL 기능 검증 |
| Edge Case Test | 중복 PK, 문자열 내 수미표, 존재하지 않는 테이블, 빈 결과 같은 예외 상황 검증  |

<img width="881" height="370" alt="image" src="https://github.com/user-attachments/assets/fcafb6cd-0f97-450f-932a-7ed9fa6e6912" />

## 수요 코딩회 작업 비중
<img width="666" height="448" alt="image" src="https://github.com/user-attachments/assets/802587bc-2688-45fe-a258-80c2f3112552" />


## 최적화 요약

| 항목 |  설명 |
|------|----------|
| Tokenizer 캐시 | 동일 SQL 입력 시, 토큰화 결과를 캐시에서 재사용하여 문자열 → 토큰 변환 비용 감소 |
| 인덱스 기반 조회 | WHERE 조건 시, 인메모리 인덱스를 생성하여 조건에 맞는 row offset 탐색 |
| Offset 기반 파일 접근 | 전체 파일을 순회하지 않고, 필요한 행만 직접 읽어서 조회 |
