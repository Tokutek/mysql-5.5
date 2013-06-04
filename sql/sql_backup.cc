#include <mysqld_error.h>
#include "sql_class.h"
#include "sql_backup.h"
#include "mysqld.h"
#include "backup.h"

ulong backup_throttle = ULONG_MAX;

struct backup_poll {
    THD *thd;
    char *the_string;
    size_t len;
    backup_poll(THD *t): thd(t), the_string(NULL), len(0) {};
    ~backup_poll(void) {
        if (the_string) {
            free(the_string);
        }
    }
};

static int mysql_backup_poll_fun(float progress, const char *progress_string, void *poll_extra);
static int mysql_backup_poll_fun(float progress, const char *progress_string, void *poll_extra) {
    backup_poll *bp = (backup_poll*)poll_extra;
    if (bp->thd->killed) {
        return ER_ABORTING_CONNECTION;
    }
    float percentage = progress * 100;
    char *old_string = NULL;
    {
        int len = 100+strlen(progress_string);
        if ((size_t)len > bp->len) {
            old_string = bp->the_string; // don't free this until after we set the thd_proc_info (since it may still be refered to)
            bp->the_string = (char*)malloc(len);
            bp->len    = len;
        }
    }
    int r = snprintf(bp->the_string, bp->len, "Backup about %.0f%% done: %s", percentage, progress_string);
    assert((size_t)r<bp->len);
    thd_proc_info(bp->thd, bp->the_string);
    if (old_string) free(old_string);
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
    backup_poll bp(thd);
    int r = tokubackup_create_backup(source_dirs, dest_dirs, 1,
                                     mysql_backup_poll_fun, &bp,
                                     mysql_error_fun,       thd);
    thd_proc_info(thd, "backup done"); // set this before freeing the pointer to the bp string, since it may be still refered to inside.
    free(bp.the_string);
    return r;
}

void sql_backup_throttle(unsigned long rate) {
    tokubackup_throttle_backup(rate);
}

const char *tokubackup_version = tokubackup_version_string;
