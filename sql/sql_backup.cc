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
const int BAD_BACKUP_SOURCE_DIR_INDEX = -1;

class source_dirs {
    int m_count;
    int m_mysql_data_dir_index;
    int m_tokudb_data_dir_index;
    int m_tokudb_log_dir_index;
    int m_log_bin_dir_index;
    const char *m_dirs[MYSQL_MAX_DIR_COUNT];
    bool m_valid_dirs[MYSQL_MAX_DIR_COUNT];

public:
    source_dirs() : m_count(1),
                    m_mysql_data_dir_index(0), 
                    m_tokudb_data_dir_index(BAD_BACKUP_SOURCE_DIR_INDEX), 
                    m_tokudb_log_dir_index(BAD_BACKUP_SOURCE_DIR_INDEX),
                    m_log_bin_dir_index(BAD_BACKUP_SOURCE_DIR_INDEX) {
        m_dirs[0] = mysql_real_data_home; // Default: Just mysql's data dir.
        m_valid_dirs[0] = true;
        for (int i = 1; i < MYSQL_MAX_DIR_COUNT; ++i) {
            m_dirs[i] = NULL;
            m_valid_dirs[i] = false;
        }
    }

    ~source_dirs() {
        // NOTE: Start at the second dir entry, we don't want to free
        // mysql's data dir string!
        for (int i = 1; i < MYSQL_MAX_DIR_COUNT; ++i) {
            if (m_dirs[i]) {
                my_free((void*)m_dirs[i]);
            }
        }
    }

    bool tokudb_data_dir_set(void) const {
        if (m_tokudb_data_dir_index != BAD_BACKUP_SOURCE_DIR_INDEX &&
            m_valid_dirs[m_tokudb_data_dir_index]) {
            return true;
        } 

        return false;
    }

    bool tokudb_log_dir_set(void) const {
        if (m_tokudb_log_dir_index != BAD_BACKUP_SOURCE_DIR_INDEX &&
            m_valid_dirs[m_tokudb_log_dir_index]) {
            return true;
        } 

        return false;
    }

    bool log_bin_dir_set(void) const {
        if (m_log_bin_dir_index != BAD_BACKUP_SOURCE_DIR_INDEX &&
            m_valid_dirs[m_log_bin_dir_index]) {
            return true;
        }

        return false;
    }

    void find_tokudb_data_dir(THD *thd) {
        bool variable_found = this->find_plug_in_sys_var("tokudb_data_dir", thd);
        if (variable_found) {
            m_tokudb_data_dir_index = m_count - 1;
        }
    }

    void find_tokudb_log_dir(THD *thd) {
        bool variable_found = this->find_plug_in_sys_var("tokudb_log_dir", thd);
        if (variable_found) {
            m_tokudb_log_dir_index = m_count - 1;
        }
    }

    bool find_log_bin(THD *thd) {
        if (opt_bin_logname == NULL) {
            return false;
        }

        // If this has been set to just a filename, and not a path to
        // a regular file, we don't want to back this up to its own
        // directory, just skip it.
        if (opt_bin_logname[0] != '/') {
            return false;
        }

        int length = strlen(opt_bin_logname);
        char *buf = (char *)my_malloc(length + 1, 0);
        if (buf == NULL) {
            return false;
        }

        bool r = normalize_binlog_name(buf, opt_bin_logname, false);
        if (r) { 
            return false;
        }
        buf[length] = 0;

        // NOTE: We have to extract the directory of this field.
        this->truncate_and_set_file_name(buf, length);
        return true;
    }

    bool verify_source_dir_arrangement(void) {
        bool valid_dir_set = true;
        for (int i = 0; i < m_count; ++i) {
            for (int j = 0; j < m_count; ++j) {
                // We don't want to compare one dir to itself.
                if (i == j) {
                    continue;
                }

                // The dir may have been erased on a previous
                // iteration.
                if (m_dirs[i] == NULL || m_dirs[j] == NULL) {
                    continue;
                }

                // If the left dir is not a child of the right dir,
                // move on to the next one.
                if (this->dir_is_child_of_dir(m_dirs[i], m_dirs[j]) == false) {
                    continue;
                }

                // We only allow a select number of scenarios where
                // child dirs can be backed up.  These special cases
                // require reducing the number of directories sent to
                // hot backup.  All other cases are errors.
                if (j == m_mysql_data_dir_index) {
                    // We have to eliminate the child dir from the
                    // list of dirs backed up.  Remove dirs[i] from
                    // list of dirs to back up, but not from the list
                    // of dirs to check, there could still be an error
                    // where dirs[i] is a parent of another dir.
                    m_valid_dirs[i] = false;
                    continue;
                }

                if (j == m_tokudb_data_dir_index && 
                    i == m_tokudb_log_dir_index) {
                    // This is ok, but we have to eliminate the log
                    // dir from the lsit of source files.
                    m_valid_dirs[i] = false;
                    continue;
                }

                if (this->dirs_are_the_same(m_dirs[i], m_dirs[j])) {
                    // If the dir on the left is mysqld's data dir this is ok.
                    if (i == m_mysql_data_dir_index) {
                        m_valid_dirs[j] = false;
                        continue;
                    }

                    // If the dir on the left is the tokudb data dir,
                    // and the other is not mysql's data dir, then
                    // it's also ok.
                    if (i == m_tokudb_data_dir_index &&
                        j != m_mysql_data_dir_index) {
                        m_valid_dirs[j] = false;
                        continue;
                    }
                }

                // If we got this far then we have discovered an
                // invalid scenario, we need to return the failure to
                // abort the backup.
                valid_dir_set = false;
                sql_print_error("Hot Backup can't backup %s as a subdirectory of %s.  Backup not started.\n", 
                                m_dirs[i], m_dirs[j]);
                return valid_dir_set;
            }
        }

        return valid_dir_set;
    }


    int set_valid_dirs_and_get_count(const char *array[], const int size) {
        int count = 0;
        if (size > MYSQL_MAX_DIR_COUNT) {
            return count;
        }

        for (int i = 0; i < MYSQL_MAX_DIR_COUNT; ++i) {
            if (m_valid_dirs[i]) {
                count++;
                array[i] = m_dirs[i];
            } else {
                array[i] = NULL;
            }
        }

        return count;
    }

private:
    bool find_plug_in_sys_var(const char *name, THD *thd) {
        bool result = false;
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
                m_dirs[m_count] = strdup(str->ptr());
                if (m_dirs[m_count]) {
                    m_valid_dirs[m_count] = true;
                    m_count++;
                    result = true;
                }
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
        int pos = 0;

        // NOTE: We don't care about the leading slash, so it's ok to
        // only scan backwards to the 2nd character.
        for (int i = length; i > 0; --i) {
            if (str[i] == slash) {
                pos = i;
                break;
            }
        }

        // NOTE: MySQL should not allow this to happen.  The user
        // needs to specify a file, not the root dir (/).  This
        // shouldn't happen, but it might, so let's pretend it's ok.
        if (pos != 0) {
            const int size = pos + 1;
            str[size] = 0;
        }

        m_valid_dirs[m_count] = true;
        m_log_bin_dir_index = m_count;
        m_dirs[m_count++] = str;
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
    sources.find_tokudb_data_dir(thd);
    sources.find_tokudb_log_dir(thd);
    sources.find_log_bin(thd);
    bool valid_arrangement = sources.verify_source_dir_arrangement();
    if (valid_arrangement == false) {
        // Error reported in above function.
        return -1;
    }

    struct destination_dirs destinations(dest_dir);
    int index = 0;
    destinations.set_backup_subdir("/mysql_data_dir", index);
    if (sources.tokudb_data_dir_set()) {
       destinations.set_backup_subdir("/tokudb_data_dir", ++index);
    }

    if (sources.tokudb_log_dir_set()) {
       destinations.set_backup_subdir("/tokudb_log_dir", ++index);
    }

    if (sources.log_bin_dir_set()) {
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

    sql_print_information("Hot Backup initiating backup with the following source and destination directories:\n");
    for (int i = 0; i < count; ++i) {
        printf("%d: %s -> %s\n", i + 1, source_dirs[i], dest_dirs[i]);
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
