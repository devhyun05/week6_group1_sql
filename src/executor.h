#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "hard_parser.h"

/**
 * Execute a parsed SQL statement and print any user-visible result.
 */
int executor_execute(const SqlStatement *stmt);

#endif
