#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "hard_parser.h"

/*
 * 파싱이 끝난 SQL 문 하나를 실행한다.
 * 성공 시 SUCCESS, 실패 시 FAILURE를 반환한다.
 */
int executor_execute(const SqlStatement *statement);

/*
 * 실행기 런타임 캐시 상태를 초기화한다.
 */
void executor_reset_runtime_state(void);

/*
 * 현재 라이브러리 캐시에 저장된 실행 계획 수를 반환한다.
 */
int executor_get_library_cache_entry_count(void);

/*
 * 마지막 초기화 이후 발생한 라이브러리 캐시 히트 수를 반환한다.
 */
int executor_get_library_cache_hit_count(void);

#endif
