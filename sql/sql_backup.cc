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
            my_free(the_string);
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
            bp->the_string = (char*)my_malloc(len, MY_FAE);
            bp->len    = len;
        }
    }
    int r = snprintf(bp->the_string, bp->len, "Backup about %.0f%% done: %s", percentage, progress_string);
    assert((size_t)r<bp->len);
    thd_proc_info(bp->thd, bp->the_string);
    if (old_string) my_free(old_string);
    return 0;
}

static void mysql_error_fun(int error_number, const char *error_string, void *error_extra) {
    // We might be able to do better on certain known errors (such as ER_ABORTING_CONNECTION)
    my_printf_error(ER_UNKNOWN_ERROR, "Backup failed (errno=%d): %s\n", MYF(0), error_number, error_string);
}

const int MYSQL_MAX_DIR_COUNT = 3;

struct source_dirs {
    const char *m_dirs[MYSQL_MAX_DIR_COUNT];
    Item * m_items[MYSQL_MAX_DIR_COUNT];
    int m_count;
    bool m_custom_dirs_set;

    source_dirs() : m_count(1), m_custom_dirs_set(false) {
        m_dirs[0] = mysql_real_data_home; // Default: Just mysql's data dir.                                                                                  
        m_items[0] = NULL;
        for (int i = 1; i < MYSQL_MAX_DIR_COUNT; ++i) {
            m_dirs[i] = NULL;
            m_items[i] = NULL;
        }
    };

    ~source_dirs() {
        for (int i = 0; i < MYSQL_MAX_DIR_COUNT; ++i) {
            if (m_items[i]) {
                delete m_items[i];
            }
        }
    };

    bool sys_var_found_and_set(const char *name, THD *thd) {
        bool result = false;
        String null_string;
        String name_to_find(name, &my_charset_bin);
        m_items[m_count] = get_system_var(thd,
                                          OPT_GLOBAL,
                                          name_to_find.lex_string(),
                                          null_string.lex_string());
        if (m_items[m_count]) {
            String scratch;
            String * str = m_items[m_count]->val_str(&scratch);
            if (str) {
                m_dirs[m_count++] = str->ptr();
                result = true;
                m_custom_dirs_set = true;
            }
        }

        return result;
    };
};

struct destination_dirs {
    const char * m_backup_dir;
    int m_backup_dir_len;
    const char * m_dirs[MYSQL_MAX_DIR_COUNT];

    destination_dirs(const char *backup_dir) : m_backup_dir(backup_dir) {
        m_backup_dir_len = strlen(m_backup_dir);
        m_dirs[0] = m_backup_dir;
        for (int i = 1; i < MYSQL_MAX_DIR_COUNT; ++i) {
            m_dirs[i] = NULL;
        }
    };

    ~destination_dirs() {
        for (int i = 0; i < MYSQL_MAX_DIR_COUNT; ++i) {
            if (m_dirs[i]) {
                my_free((void*)m_dirs[i]);
            }
        }
    };

    bool set_backup_subdir(const char *postfix, const int index) {
        bool result = false;
        if (index < 0 || index >= MYSQL_MAX_DIR_COUNT) {
            return false;
        }
        const int len = strlen(postfix);
        const int total_len = len + m_backup_dir_len + 1;
        char *str = (char *)my_malloc(sizeof(char) * total_len, MYF(0));
        if (str) {
            strcpy(str, m_backup_dir);
            strcat(str, postfix);
            m_dirs[index] = str;
            result = true;
        }

        return result;
    };

    int create_dirs(void) {
        int result = 0;
        for (int i = 0; i < MYSQL_MAX_DIR_COUNT; ++i) {
            if (m_dirs[i]) {
                result = my_mkdir(m_dirs[i], 0777, MYF(0));
                if (result != 0) {
                    break;
                }
            }
        }

        return result;
    };

private:
    destination_dirs() {};
};

int sql_backups(const char *source_dir, const char *dest_dir, THD *thd) {
    struct source_dirs sources;
    bool toku_data_dir = false;
    toku_data_dir = sources.sys_var_found_and_set("tokudb_data_dir", thd);
    bool toku_log_dir = false;
    toku_log_dir = sources.sys_var_found_and_set("tokudb_log_dir", thd);

    struct destination_dirs destinations(dest_dir);
    if (sources.m_custom_dirs_set) {
        int index = 0;
        destinations.set_backup_subdir("/mysql_data_dir", index);

        // NOTE: The order of non-mysql data directories we create                                                                                            
        // backup dirs for matters here.  It must match the order of                                                                                          
        // the corresponding source directories.                                                                                                              
        if (toku_data_dir) {
            destinations.set_backup_subdir("/tokudb_data_dir", ++index);
        }

        if (toku_log_dir) {
            destinations.set_backup_subdir("/tokudb_log_dir", ++index);
        }

        int r = destinations.create_dirs();
        if (r != 0) {
            sql_print_error("Hot Backup couldn't create destination directories.\n");
            return r;
        }
    }

    // We should honor permissions, but for now just do it.
    fprintf(stderr, "Now I call backup from %s:%d.  The dest directory is %s, source dir is %s\n", __FILE__, __LINE__, dest_dir, source_dir);
    const char *source_dirs[] = {source_dir};
    const char *dest_dirs  [] = {dest_dir};
    backup_poll bp(thd);
    int r = tokubackup_create_backup(sources.m_dirs,
                                     destinations.m_dirs,
                                     sources.m_count,
                                     mysql_backup_poll_fun, &bp,
                                     mysql_error_fun,       thd);
    thd_proc_info(thd, "backup done"); // set this before freeing the pointer to the bp string, since it may be still refered to inside.
    return r;
}

void sql_backup_throttle(unsigned long rate) {
    tokubackup_throttle_backup(rate);
}

const char *tokubackup_version = tokubackup_version_string;
