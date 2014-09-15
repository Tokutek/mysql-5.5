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
    assert((size_t)r < bp->len);
    thd_proc_info(bp->thd, bp->the_string);
    if (old_string) my_free(old_string);
    return 0;
}

static void mysql_error_fun(int error_number, const char *error_string, void *error_extra) {
    // We might be able to do better on certain known errors (such as ER_ABORTING_CONNECTION)
    my_printf_error(ER_UNKNOWN_ERROR, "Backup failed (errno=%d): %s\n", MYF(0), error_number, error_string);
}

const int MYSQL_MAX_DIR_COUNT = 4;

class source_dirs {
    int m_count;
    const char *m_dirs[MYSQL_MAX_DIR_COUNT];
    char *m_mysql_data_dir;
    const char *m_tokudb_data_dir;
    const char *m_tokudb_log_dir;
    const char *m_log_bin_dir;

public:
    bool log_bin_set;
    bool tokudb_data_set;
    bool tokudb_log_set;

public:
    source_dirs() : m_count(0),
                    m_mysql_data_dir(NULL),
                    m_tokudb_data_dir(NULL),
                    m_tokudb_log_dir(NULL),
                    m_log_bin_dir(NULL),
                    log_bin_set(false),
                    tokudb_data_set(false),
                    tokudb_log_set(false) {
        for (int i = 0; i < MYSQL_MAX_DIR_COUNT; ++i) {
            m_dirs[i] = NULL;
        }
    }

    ~source_dirs() {
        for (int i = 0; i < MYSQL_MAX_DIR_COUNT; ++i) {
            if (m_mysql_data_dir) {
                my_free((void*)m_mysql_data_dir);
                m_mysql_data_dir = NULL;
            }

            if (m_tokudb_data_dir) {
                my_free((void*)m_tokudb_data_dir);
                m_tokudb_data_dir = NULL;
            }

            if (m_tokudb_log_dir) {
                my_free((void*)m_tokudb_log_dir);
                m_tokudb_log_dir = NULL;
            }

            if (m_log_bin_dir) {
                my_free((void*)m_log_bin_dir);
                m_log_bin_dir = NULL;
            }
        }
    }

    bool find_and_allocate_dirs(THD *thd) {
        // Sanitize the trailing slash of the MySQL Data Dir.
        m_mysql_data_dir = strdup(mysql_real_data_home);
        if (m_mysql_data_dir == NULL) {
            // HUH? Memory error?
            return false;
        }

        const size_t length = strlen(m_mysql_data_dir);
        m_mysql_data_dir[length - 1] = 0;
        
        // Note: These all allocate new strings or return NULL.
        m_tokudb_data_dir = this->find_plug_in_sys_var("tokudb_data_dir", thd);
        m_tokudb_log_dir = this->find_plug_in_sys_var("tokudb_log_dir", thd);
        m_log_bin_dir = this->find_log_bin_dir(thd);
        return true;
    }

    bool check_dirs_layout(void) {
        // Ignore directories that are children of the MySQL data dir.
        if (m_tokudb_data_dir != NULL && 
            this->dir_is_child_of_dir(m_tokudb_data_dir, m_mysql_data_dir) == false) {
            tokudb_data_set = true;
        }

        if (m_tokudb_log_dir != NULL && 
            this->dir_is_child_of_dir(m_tokudb_log_dir, m_mysql_data_dir) == false) {
            tokudb_log_set = true;
        }

        if (m_log_bin_dir != NULL && 
            this->dir_is_child_of_dir(m_log_bin_dir, m_mysql_data_dir) == false) {
            log_bin_set = true;
        }

        // Check if TokuDB log dir is a child of TokuDB data dir.  If it is, we want to ignore it.
        if (tokudb_log_set && tokudb_data_set) {
            if (this->dir_is_child_of_dir(m_tokudb_log_dir, m_tokudb_data_dir)) {
                tokudb_log_set = false;
            }
        }

        // Check if log bin dir is a child of either TokuDB data dir.
        if (log_bin_set && tokudb_data_set) {
            if (this->dir_is_child_of_dir(m_log_bin_dir, m_tokudb_data_dir)) {
                log_bin_set = false;
            }
        }

        // Check if log bin dir is a child of either TokuDB log dir.
        if (log_bin_set && tokudb_log_set) {
            if (this->dir_is_child_of_dir(m_log_bin_dir, m_tokudb_log_dir)) {
                log_bin_set = false;
            }
        }

        // Check if any of the three non-mysql dirs is a strict parent
        // of the mysql data dir.  This is an error.  NOTE: They can
        // be the same.
        const char *error = "%s directory %s can't be a parent of mysql data dir %s when backing up";
        if (tokudb_data_set &&
            this->dir_is_child_of_dir(m_mysql_data_dir, m_tokudb_data_dir) == true && 
            this->dirs_are_the_same(m_tokudb_data_dir, m_mysql_data_dir) == false) {
            sql_print_error(error, "tokudb-data-dir", m_tokudb_data_dir, m_mysql_data_dir);
            return false;
        }

        if (tokudb_log_set &&
            this->dir_is_child_of_dir(m_mysql_data_dir, m_tokudb_log_dir) == true && 
            this->dirs_are_the_same(m_tokudb_log_dir, m_mysql_data_dir) == false) {
            sql_print_error(error, "tokudb-log-dir", m_tokudb_log_dir, m_mysql_data_dir);
            return false;
        }

        if (log_bin_set &&
            this->dir_is_child_of_dir(m_mysql_data_dir, m_log_bin_dir) == true && 
            this->dirs_are_the_same(m_log_bin_dir, m_mysql_data_dir) == false) {
            sql_print_error(error, "mysql log-bin", m_log_bin_dir, m_mysql_data_dir);
            return false;
        }

        return true;
    }

    void set_dirs(void) {
        // Set the directories in the output array.
        m_count = 0;
        m_dirs[m_count++] = m_mysql_data_dir;

        if (tokudb_data_set) {
            m_dirs[m_count++] = m_tokudb_data_dir;
        }

        if (tokudb_log_set) {
            m_dirs[m_count++] = m_tokudb_log_dir;
        }

        if (log_bin_set) {
            m_dirs[m_count++] = m_log_bin_dir;
        }
    }

    int set_valid_dirs_and_get_count(const char *array[], const int size) {
        int count = 0;
        if (size > MYSQL_MAX_DIR_COUNT) {
            return count;
        }

        for (int i = 0; i < MYSQL_MAX_DIR_COUNT; ++i) {
            if (m_dirs[i] != NULL) {
                count++;
            }

            array[i] = m_dirs[i];
        }

        return count;
    }

private:

    const char * find_log_bin_dir(THD *thd) {
        if (opt_bin_logname == NULL) {
            return NULL;
        }

        // If this has been set to just a filename, and not a path to
        // a regular file, we don't want to back this up to its own
        // directory, just skip it.
        if (opt_bin_logname[0] != '/') {
            return NULL;
        }

        int length = strlen(opt_bin_logname);
        char *buf = (char *)my_malloc(length + 1, 0);
        if (buf == NULL) {
            return NULL;
        }

        bool r = normalize_binlog_name(buf, opt_bin_logname, false);
        if (r) {
            my_free((void*)buf);
            return NULL;
        }

        // Add end of string char.
        buf[length] = 0;

        // NOTE: We have to extract the directory of this field.
        this->truncate_and_set_file_name(buf, length);
        return buf;
    }

    const char * find_plug_in_sys_var(const char *name, THD *thd) {
        const char * result = NULL;
        String null_string;
        String name_to_find(name, &my_charset_bin);
        Item *item = get_system_var(thd,
                                    OPT_GLOBAL,
                                    name_to_find.lex_string(),
                                    null_string.lex_string());
        if (item) {
            String scratch;
            String * str = item->val_str(&scratch);
            if (str) {
                result = strdup(str->ptr());
            }
        }

        delete item;
        return result;
    }

    bool dir_is_child_of_dir(const char *candidate, const char *potential_parent) {
        size_t length = strlen(potential_parent);
        int r = strncmp(candidate, potential_parent, length);
        if (r == 0) {
            return true;
        }

        return false;
    }

    bool dirs_are_the_same(const char *left, const char *right) {
        int r = strcmp(left, right);
        if (r == 0) {
            return true;
        }

        return false;
    }

    // Removes the trailing bin log file from the system variable.
    void truncate_and_set_file_name(char *str, int length) {
        const char slash = '/';
        int position_of_last_slash = 0;

        // NOTE: We don't care about the leading slash, so it's ok to
        // only scan backwards to the 2nd character.
        for (int i = length; i > 0; --i) {
            if (str[i] == slash) {
                position_of_last_slash = i;
                break;
            }
        }

        // NOTE: MySQL should not allow this to happen.  The user
        // needs to specify a file, not the root dir (/).  This
        // shouldn't happen, but it might, so let's pretend it's ok.
        if (position_of_last_slash != 0) {
            // NOTE: We are sanitizing the path by removing the last slash.
            str[position_of_last_slash] = 0;
        }
    }
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
    }

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
    if (sources.find_and_allocate_dirs(thd) == false) {
        return -1;
    }

    if (sources.check_dirs_layout() == false) {
        return -1;
    }

    sources.set_dirs();
    struct destination_dirs destinations(dest_dir);
    int index = 0;
    destinations.set_backup_subdir("/mysql_data_dir", index);
    if (sources.tokudb_data_set) {
       destinations.set_backup_subdir("/tokudb_data_dir", ++index);
    }

    if (sources.tokudb_log_set) {
       destinations.set_backup_subdir("/tokudb_log_dir", ++index);
    }

    if (sources.log_bin_set) {
        destinations.set_backup_subdir("/mysql_log_bin", ++index);
    }

    int r = destinations.create_dirs();
    if (r != 0) {
        sql_print_error("Hot Backup couldn't create needed directories.\n");
        return r;
    }

    // We should honor permissions, but for now just do it.
    const char *source_dirs[MYSQL_MAX_DIR_COUNT] = {NULL};
    const char *dest_dirs[MYSQL_MAX_DIR_COUNT] = {NULL};
    int count = sources.set_valid_dirs_and_get_count(source_dirs, 
                                                      MYSQL_MAX_DIR_COUNT);
    for (int i = 0; i < count; ++i) {
        dest_dirs[i] = destinations.m_dirs[i];
    }

    sql_print_information("Hot Backup initiating backup with the following source and destination directories:");
    for (int i = 0; i < count; ++i) {
        sql_print_information("%d: %s -> %s", i + 1, source_dirs[i], dest_dirs[i]);
    }

    backup_poll bp(thd);
    r = tokubackup_create_backup(source_dirs,
                                 dest_dirs,
                                 count,
                                 mysql_backup_poll_fun, &bp,
                                 mysql_error_fun,       thd);
    thd_proc_info(thd, "backup done"); // set this before freeing the pointer to the bp string, since it may be still refered to inside.
    return r;
}

void sql_backup_throttle(unsigned long rate) {
    tokubackup_throttle_backup(rate);
}

const char *tokubackup_version = tokubackup_version_string;
