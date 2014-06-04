#include <mysqld_error.h>

extern int sql_backups(const char *source_dir, const char *dest_dir, THD *thd);
extern void sql_backup_throttle(unsigned long rate);

const extern char *tokubackup_version;
