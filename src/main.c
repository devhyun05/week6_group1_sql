#include "executor.h"
#include "hard_parser.h"
#include "soft_parser.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

static int process_statement(const char *statement) {
    Token *tokens = NULL;
    SqlStatement parsed_statement;
    int token_count = 0;
    int result = FAILURE;

    tokens = soft_parse(statement, &token_count);
    if (tokens == NULL || token_count == 0) {
        soft_free_tokens(tokens);
        return FAILURE;
    }

    if (hard_parse(tokens, token_count, &parsed_statement) != SUCCESS) {
        goto cleanup;
    }

    if (executor_execute(&parsed_statement) != SUCCESS) {
        goto cleanup;
    }

    result = SUCCESS;

cleanup:
    soft_free_tokens(tokens);
    return result;
}

static int run_sql_buffer(const char *buffer, int allow_trailing_statement) {
    char **statements = NULL;
    char *remainder = NULL;
    char *trimmed_remainder = NULL;
    int statement_count = 0;
    int index;

    if (utils_split_complete_sql_statements(buffer, &statements, &statement_count, &remainder) != SUCCESS) {
        return FAILURE;
    }

    for (index = 0; index < statement_count; ++index) {
        process_statement(statements[index]);
    }

    if (allow_trailing_statement) {
        trimmed_remainder = utils_trim_copy(remainder == NULL ? "" : remainder);
        if (trimmed_remainder != NULL && trimmed_remainder[0] != '\0') {
            process_statement(trimmed_remainder);
        }
        free(trimmed_remainder);
        trimmed_remainder = NULL;
    }

    free(remainder);
    utils_free_string_array(statements, statement_count);
    return SUCCESS;
}

static int run_file_mode(const char *path) {
    char *contents = utils_read_file(path);
    int result = FAILURE;

    if (contents == NULL) {
        return FAILURE;
    }

    result = run_sql_buffer(contents, 1);
    free(contents);
    return result;
}

static int is_exit_command(const char *line) {
    char *trimmed = utils_trim_copy(line);
    int should_exit = 0;

    if (trimmed == NULL) {
        return 0;
    }

    should_exit = utils_equals_ignore_case(trimmed, "exit") || utils_equals_ignore_case(trimmed, "quit");
    free(trimmed);
    return should_exit;
}

static int repl_input_ready_with_timeout(long timeout_usec) {
    fd_set read_fds;
    struct timeval timeout;

    FD_ZERO(&read_fds);
    FD_SET(STDIN_FILENO, &read_fds);
    timeout.tv_sec = timeout_usec / 1000000L;
    timeout.tv_usec = timeout_usec % 1000000L;

    return select(STDIN_FILENO + 1, &read_fds, NULL, NULL, &timeout) > 0;
}

static int repl_input_ready(void) {
    return repl_input_ready_with_timeout(0);
}

static int drain_ready_repl_input(char **buffer, size_t *capacity) {
    char line[MAX_LINE_LENGTH];

    while (repl_input_ready_with_timeout(20000)) {
        if (fgets(line, sizeof(line), stdin) == NULL) {
            break;
        }

        if (utils_append_text(buffer, capacity, line) != SUCCESS) {
            return FAILURE;
        }
    }

    return SUCCESS;
}

static void run_repl_mode(void) {
    char line[MAX_LINE_LENGTH];
    char *buffer = NULL;
    char *remainder = NULL;
    char *trimmed_remainder = NULL;
    char **statements = NULL;
    size_t capacity = 0;
    int statement_count = 0;
    int index;

    while (1) {
        if (buffer != NULL && buffer[0] != '\0' && is_exit_command(buffer)) {
            break;
        }

        if (isatty(STDIN_FILENO) && !repl_input_ready()) {
            printf("%s", (buffer == NULL || buffer[0] == '\0') ? "SQL> " : "...> ");
            fflush(stdout);
        }

        if (fgets(line, sizeof(line), stdin) == NULL) {
            if (buffer != NULL && buffer[0] != '\0') {
                run_sql_buffer(buffer, 1);
            }
            break;
        }

        if ((buffer == NULL || buffer[0] == '\0') && is_exit_command(line)) {
            break;
        }

        if (utils_append_text(&buffer, &capacity, line) != SUCCESS) {
            break;
        }

        if (drain_ready_repl_input(&buffer, &capacity) != SUCCESS) {
            break;
        }

        if (!utils_statement_complete(buffer)) {
            continue;
        }

        if (utils_split_complete_sql_statements(buffer, &statements, &statement_count, &remainder) != SUCCESS) {
            break;
        }

        if (statement_count > 0) {
            printf("\n");
            fflush(stdout);
        }

        for (index = 0; index < statement_count; ++index) {
            process_statement(statements[index]);
        }

        utils_free_string_array(statements, statement_count);
        statements = NULL;
        statement_count = 0;

        free(buffer);
        trimmed_remainder = utils_trim_copy(remainder == NULL ? "" : remainder);
        free(remainder);
        remainder = NULL;
        buffer = trimmed_remainder;
        trimmed_remainder = NULL;
        capacity = buffer == NULL ? 0 : strlen(buffer) + 1;
    }

    free(trimmed_remainder);
    free(remainder);
    utils_free_string_array(statements, statement_count);
    free(buffer);
    printf("Bye.\n");
}

int main(int argc, char *argv[]) {
    if (argc > 2) {
        fprintf(stderr, "Usage: %s [file.sql]\n", argv[0]);
        return EXIT_FAILURE;
    }

    if (argc == 2) {
        return run_file_mode(argv[1]) == SUCCESS ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    run_repl_mode();
    return EXIT_SUCCESS;
}
