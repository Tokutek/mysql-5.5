/* Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql_parse.h"                       // check_access
#include "sql_table.h"                       // mysql_alter_table,
                                             // mysql_exchange_partition
#include "sql_alter.h"

bool Alter_table_statement::execute(THD *thd)
{
  LEX *lex= thd->lex;
  /* first SELECT_LEX (have special meaning for many of non-SELECTcommands) */
  SELECT_LEX *select_lex= &lex->select_lex;
  /* first table of first SELECT_LEX */
  TABLE_LIST *first_table= (TABLE_LIST*) select_lex->table_list.first;
  /*
    Code in mysql_alter_table() may modify its HA_CREATE_INFO argument,
    so we have to use a copy of this structure to make execution
    prepared statement- safe. A shallow copy is enough as no memory
    referenced from this structure will be modified.
    @todo move these into constructor...
  */
  HA_CREATE_INFO create_info(lex->create_info);
  Alter_info alter_info(lex->alter_info, thd->mem_root);
  ulong priv=0;
  ulong priv_needed= ALTER_ACL;
  bool result;

  DBUG_ENTER("Alter_table_statement::execute");

  if (thd->is_fatal_error) /* out of memory creating a copy of alter_info */
    DBUG_RETURN(TRUE);
  /*
    We also require DROP priv for ALTER TABLE ... DROP PARTITION, as well
    as for RENAME TO, as being done by SQLCOM_RENAME_TABLE
  */
  if (alter_info.flags & (ALTER_DROP_PARTITION | ALTER_RENAME))
    priv_needed|= DROP_ACL;

  /* Must be set in the parser */
  DBUG_ASSERT(select_lex->db);
  DBUG_ASSERT(!(alter_info.flags & ALTER_ADMIN_PARTITION));
  if (check_access(thd, priv_needed, first_table->db,
                   &first_table->grant.privilege,
                   &first_table->grant.m_internal,
                   0, 0) ||
      check_access(thd, INSERT_ACL | CREATE_ACL, select_lex->db,
                   &priv,
                   NULL, /* Don't use first_tab->grant with sel_lex->db */
                   0, 0))
    DBUG_RETURN(TRUE);                  /* purecov: inspected */

  /* If it is a merge table, check privileges for merge children. */
  if (create_info.merge_list.first &&
      check_table_access(thd, SELECT_ACL | UPDATE_ACL | DELETE_ACL,
                         create_info.merge_list.first, FALSE, UINT_MAX, FALSE))
    DBUG_RETURN(TRUE);

  if (check_grant(thd, priv_needed, first_table, FALSE, UINT_MAX, FALSE))
    DBUG_RETURN(TRUE);                  /* purecov: inspected */

  if (lex->name.str && !test_all_bits(priv, INSERT_ACL | CREATE_ACL))
  {
    // Rename of table
    TABLE_LIST tmp_table;
    bzero((char*) &tmp_table,sizeof(tmp_table));
    tmp_table.table_name= lex->name.str;
    tmp_table.db= select_lex->db;
    tmp_table.grant.privilege= priv;
    if (check_grant(thd, INSERT_ACL | CREATE_ACL, &tmp_table, FALSE,
                    UINT_MAX, FALSE))
      DBUG_RETURN(TRUE);                  /* purecov: inspected */
  }

  /* Don't yet allow changing of symlinks with ALTER TABLE */
  if (create_info.data_file_name)
    push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                        WARN_OPTION_IGNORED, ER(WARN_OPTION_IGNORED),
                        "DATA DIRECTORY");
  if (create_info.index_file_name)
    push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                        WARN_OPTION_IGNORED, ER(WARN_OPTION_IGNORED),
                        "INDEX DIRECTORY");
  create_info.data_file_name= create_info.index_file_name= NULL;

  thd->enable_slow_log= opt_log_slow_admin_statements;

  result= mysql_alter_table(thd, select_lex->db, lex->name.str,
                            &create_info,
                            first_table,
                            &alter_info,
                            select_lex->order_list.elements,
                            select_lex->order_list.first,
                            lex->ignore);

  DBUG_RETURN(result);
}

Alter_table_ctx::Alter_table_ctx()
  : datetime_field(NULL), error_if_not_empty(false),
    tables_opened(0),
    db(NULL), table_name(NULL), alias(NULL),
    new_db(NULL), new_name(NULL), new_alias(NULL)
#ifndef DBUG_OFF
    , tmp_table(false)
#endif
{
}


Alter_table_ctx::Alter_table_ctx(THD *thd, TABLE_LIST *table_list,
                                 uint tables_opened_arg,
                                 char *new_db_arg, char *new_name_arg)
  : datetime_field(NULL), error_if_not_empty(false),
    tables_opened(tables_opened_arg),
    new_db(new_db_arg), new_name(new_name_arg)
#ifndef DBUG_OFF
    , tmp_table(false)
#endif
{
  /*
    Assign members db, table_name, new_db and new_name
    to simplify further comparisions: we want to see if it's a RENAME
    later just by comparing the pointers, avoiding the need for strcmp.
  */
  db= table_list->db;
  table_name= table_list->table_name;
  alias= (lower_case_table_names == 2) ? table_list->alias : table_name;

  if (!new_db || !my_strcasecmp(table_alias_charset, new_db, db))
    new_db= db;

  if (new_name)
  {
    DBUG_PRINT("info", ("new_db.new_name: '%s'.'%s'", new_db, new_name));

    if (lower_case_table_names == 1) // Convert new_name/new_alias to lower case
    {
      my_casedn_str(files_charset_info, new_name);
      new_alias= new_name;
    }
    else if (lower_case_table_names == 2) // Convert new_name to lower case
    {
      strmov(new_alias= new_alias_buff, new_name);
      my_casedn_str(files_charset_info, new_name);
    }
    else
      new_alias= new_name; // LCTN=0 => case sensitive + case preserving

    if (!is_database_changed() &&
        !my_strcasecmp(table_alias_charset, new_name, table_name))
    {
      /*
        Source and destination table names are equal:
        make is_table_renamed() more efficient.
      */
      new_alias= table_name;
      new_name= table_name;
    }
  }
  else
  {
    new_alias= alias;
    new_name= table_name;
  }

  my_snprintf(tmp_name, sizeof(tmp_name), "%s-%lx_%lx", tmp_file_prefix,
              current_pid, thd->thread_id);
  /* Safety fix for InnoDB */
  if (lower_case_table_names)
    my_casedn_str(files_charset_info, tmp_name);

  if (table_list->table->s->tmp_table == NO_TMP_TABLE)
  {
    build_table_filename(path, sizeof(path) - 1, db, table_name, "", 0);

    build_table_filename(new_path, sizeof(new_path) - 1, new_db, new_name, "", 0);

    build_table_filename(new_filename, sizeof(new_filename) - 1,
                         new_db, new_name, reg_ext, 0);

    build_table_filename(tmp_path, sizeof(tmp_path) - 1, new_db, tmp_name, "",
                         FN_IS_TMP);
  }
  else
  {
    /*
      We are not filling path, new_path and new_filename members if
      we are altering temporary table as these members are not used in
      this case. This fact is enforced with assert.
    */
    build_tmptable_filename(thd, tmp_path, sizeof(tmp_path));
#ifndef DBUG_OFF
    tmp_table= true;
#endif
  }
}
