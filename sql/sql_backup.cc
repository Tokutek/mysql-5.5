#include <mysqld_error.h>
#include "sql_class.h"
#include "sql_backup.h"
#include "mysqld.h"
#include "backup.h"

ulong backup_throttle = ULONG_MAX;

static int mysql_backup_poll_fun(float progress, const char *progress_string, void *poll_extra);
static int mysql_backup_poll_fun(float progress, const char *progress_string, void *poll_extra) {
    THD *thd = (THD*)poll_extra;
    if (thd->killed) {
        return ER_ABORTING_CONNECTION;
    }
    float percentage = progress * 100;
    int len = 100+strlen(progress_string);
    char str[len];
    int r = snprintf(str, len, "Backup about %.0f%% done: %s", percentage, progress_string);
    assert(r<len);
    thd_proc_info(thd, str);
    return 0;
}

static void mysql_error_fun(int error_number, const char *error_string, void *error_extra) {
    // We might be able to do better on certain known errors (such as ER_ABORTING_CONNECTION)
    my_printf_error(ER_UNKNOWN_ERROR, "Backup failed (errno=%d): %s\n", MYF(0), error_number, error_string);
}

int sql_backups(const char *source_dir, const char *dest_dir, THD *thd) {
    // We should honor permissions, but for now just do it.
    fprintf(stderr, "Now I call backup from %s:%d.  The dest directory is %s, source dir is %s\n", __FILE__, __LINE__, dest_dir, source_dir);
    const char *source_dirs[] = {source_dir};
    const char *dest_dirs  [] = {dest_dir};
    int r = tokubackup_create_backup(source_dirs, dest_dirs, 1,
                                     mysql_backup_poll_fun, thd,
                                     mysql_error_fun,       thd);
    return r;
}

void sql_backup_throttle(unsigned long rate) {
    tokubackup_throttle_backup(rate);
}

const char *tokubackup_version = tokubackup_version_string;
