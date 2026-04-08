# SQL Processor

파일 기반 CSV 저장소 위에서 동작하는 경량 SQL 처리기입니다. `INSERT`, `SELECT`, `DELETE`를 지원하며, SQL 입력을 토크나이징(soft parser)과 구문 분석(hard parser)으로 나누어 처리합니다.

## Build

```bash
make
```

## Run

```bash
./sql_processor
./sql_processor tests/test_cases/basic_select.sql
./sql_processor tests/test_cases/jungle_menu_demo.sql
```

예시 REPL 입력:

```sql
INSERT INTO jungle_menu (slot_key, menu_date, meal_type, dish_order, dish_name) VALUES ('20260408_lunch', 20260408, 'lunch', 1, '경양식돈가스');
INSERT INTO jungle_menu (slot_key, menu_date, meal_type, dish_order, dish_name) VALUES ('20260408_lunch', 20260408, 'lunch', 2, '쌀밥');
INSERT INTO jungle_menu (slot_key, menu_date, meal_type, dish_order, dish_name) VALUES ('20260408_dinner', 20260408, 'dinner', 1, '뚝배기소불고기');
SELECT * FROM jungle_menu;
SELECT dish_order, dish_name FROM jungle_menu WHERE slot_key = '20260408_lunch';
DELETE FROM jungle_menu WHERE dish_name = '깍두기';
```

`jungle_menu`는 데모용 단일 테이블 예시입니다.

- `id`: auto-increment primary key
- `slot_key`: 날짜와 식사 시간대를 묶은 조회 키. 예: `20260408_lunch`
- `menu_date`: 날짜 정수값
- `meal_type`: `lunch` 또는 `dinner`
- `dish_order`: 식단 내 순서
- `dish_name`: 음식 이름

현재 `WHERE`는 단일 조건만 지원하므로, `slot_key`를 두면 "특정 날짜의 점심 메뉴"를 한 번에 조회하기 좋습니다.

## Test

```bash
make tests
```

## Docker

```bash
docker build -t sql-processor .
docker run -it --rm sql-processor bash -lc "make && ./sql_processor"
docker run --rm sql-processor bash -lc "make && ./sql_processor tests/test_cases/jungle_menu_demo.sql"
```
