disable_query_log;
--require r/true.require
select (variable_value not like '5.5%') as `TRUE` from information_schema.global_variables where variable_name='version';
enable_query_log;
