create or replace package tlog_support is

  type t_tlog_file is record(
    file_id        number,
    directory_name varchar2(4000),
    file_base      varchar2(4000),
    retention_days number);

  type t_tlog_table is record(
    table_id       number,
    table_name     varchar2(4000),
    retention_days number);

  type t_tlog_module is record(
    module_id            number,
    module_name          varchar2(4000),
    program_name         varchar2(4000),
    action               varchar2(4000),
    owner                varchar2(4000),
    file_id              number,
    table_id             number,
    file_logging_level   number,
    table_logging_level  number,
    output_logging_level number);

  --Создать или отредактировать файлы логирования
  procedure create_file(i_file in out nocopy t_tlog_file);

  --Создать или отредактировать таблицы логирования
  procedure create_table(i_table in out nocopy t_tlog_table);

  --Создать или отредактировать настройки логирования модуля
  procedure create_module(i_module in out nocopy t_tlog_module);

--Пример добавления таблицы логов для пакета TEST2 из любой схемы
--declare
--  l_table  tlog_support.t_tlog_table;
--  l_module tlog_support.t_tlog_module;
--begin
--
--  l_table.table_name := 'log_test2';
--
--  tlog_support.create_table(l_table);
--
--  l_module.module_name := 'Логирование пакета TEST2';
--  l_module.program_name := 'TEST2';
--  l_module.action := '';
--  l_module.owner := '';
--  l_module.file_id := 0;
--  l_module.table_id := l_table.table_id;
--  l_module.file_logging_level := 0;
--  l_module.table_logging_level := 500;
--  l_module.output_logging_level := 0;
--
--  tlog_support.create_module(l_module);
--
--  commit;
--end;
--/

--Пример добавления таблицы логов для любого логирования из схемы FSS_CR_COMMON
--declare
--  l_table  tlog_support.t_tlog_table;
--  l_module tlog_support.t_tlog_module;
--begin
--
--  l_table.table_name := 'log_fss_cr_common';
--
--  tlog_support.create_table(l_table);
--
--  l_module.module_name := 'Логирование схемы fss_cr_common';
--  l_module.program_name := tlog.gc_default_program;
--  l_module.action := '';
--  l_module.owner := 'FSS_CR_COMMON';
--  l_module.table_id := l_table.table_id;
--  l_module.table_logging_level := 500;
--  l_module.output_logging_level := 0;
--
--  tlog_support.create_module(l_module);
--
--  commit;
--end;
--/

end tlog_support;
/
create or replace package body tlog_support is

  c_default_retention constant tlog_files.n_retention_days%type := 45;

  procedure create_file(i_file in out nocopy t_tlog_file) is
    e_null_agr exception;

    c_default_directory constant tlog_files.s_directory_name%type := 'EXTLOGS_DIR';

  begin
    if (i_file.file_base is null and i_file.file_id is null) then
      raise e_null_agr;
    end if;
    if i_file.file_id is null then
      insert into tlog_files
        (file_id,
         s_directory_name,
         s_file_base,
         n_retention_days)
      values
        (seq_tlog_files.nextval,
         nvl(i_file.directory_name, c_default_directory),
         i_file.file_base,
         nvl(i_file.retention_days, c_default_retention))
      returning file_id into i_file.file_id;
    else
      update tlog_files f
         set f.s_directory_name = nvl(i_file.directory_name, f.s_directory_name),
             f.s_file_base      = nvl(i_file.file_base, f.s_file_base),
             f.n_retention_days = nvl(i_file.retention_days, f.n_retention_days)
       where f.file_id = i_file.file_id;
      if sql%rowcount = 0 then
        insert into tlog_files
          (file_id,
           s_directory_name,
           s_file_base,
           n_retention_days)
        values
          (i_file.file_id,
           nvl(i_file.directory_name, c_default_directory),
           i_file.file_base,
           nvl(i_file.retention_days, c_default_retention));
      end if;
    end if;
  exception
    when e_null_agr then
      raise_application_error(-20000, 'tlog_support.create_file file_base and file_id is null');
  end create_file;

  procedure create_table(i_table in out nocopy t_tlog_table) is
    e_null_agr exception;

    c_log_schema constant varchar2(30) := 'FSS_CR_COMMON';
    c_seq_prefix constant varchar2(30) := 'SEQ_';
    c_pk_prefix  constant varchar2(30) := 'PK_';

    procedure create_log_table(i_table_name     in varchar2,
                               i_retention_days in number) is
      c_tab_sql constant varchar2(32767) := 'create table %TABLE_NAME% (
           log_id        number                                                            not null,
           d_timestamp   timestamp      default systimestamp                               not null,
           s_log_level   varchar2(10)                                                      not null,
           s_progam_name varchar2(30)                                                      not null,
           s_log_line    number                                                            not null,
           s_log_action  varchar2(30)                                                      not null,
           s_log_text    varchar2(4000)                                                    not null,
           s_owner       varchar2(30)                                                      not null,
           s_user        varchar2(30)   default sys_context(''USERENV'', ''SESSION_USER'') not null,
           n_sid         number         default sys_context(''USERENV'', ''SID'')          not null,
           n_instance    number         default sys_context(''USERENV'', ''INSTANCE'')     not null,
           s_call_stack  varchar2(4000)                                                    not null,
           s_error_stack varchar2(4000),
           cl_add_info   clob)
         partition by range (d_timestamp)
          interval(numtodsinterval(%RETENTION_DAYS%, ''DAY''))
          (
             partition p_base values less than (to_timestamp(''%CURRENT_DATE%'',''dd.mm.yyyy''))
          )';
      l_tab_sql varchar2(32767);
    begin
      l_tab_sql := replace(c_tab_sql, '%TABLE_NAME%', i_table_name);
      l_tab_sql := replace(l_tab_sql, '%RETENTION_DAYS%', i_retention_days);
      l_tab_sql := replace(l_tab_sql, '%CURRENT_DATE%', to_char(current_date, 'dd.mm.yyyy'));

      begin
        execute immediate l_tab_sql;
      exception
        when others then
          raise_application_error(-20001,
                                  'Ошибка при создании таблицы логирования ' || i_table_name || 'Текст ошибки: ' ||
                                  dbms_utility.format_error_stack || chr(10) || dbms_utility.format_error_backtrace);
      end;
    end create_log_table;

    procedure create_log_indexes(i_table_name in varchar2) is
      c_pki_sql constant varchar2(32767) := 'create unique index %PK_PREFIX%%TABLE_NAME% on %TABLE_NAME% (log_id)';
      c_pk_sql  constant varchar2(32767) := 'alter table %TABLE_NAME% add constraint %PK_PREFIX%%TABLE_NAME% primary key (log_id) using index %PK_PREFIX%%TABLE_NAME%';
      l_index_sql varchar2(32767);
    begin
      l_index_sql := replace(c_pki_sql, '%TABLE_NAME%', i_table_name);
      l_index_sql := replace(l_index_sql, '%PK_PREFIX%', c_pk_prefix);

      begin
        execute immediate l_index_sql;
      exception
        when others then
          raise_application_error(-20001,
                                  'Ошибка при создании  индекса для первичного ключа таблицы логирования ' ||
                                  c_pk_prefix || i_table_name || 'Текст ошибки: ' || dbms_utility.format_error_stack ||
                                  chr(10) || dbms_utility.format_error_backtrace);
      end;

      l_index_sql := replace(c_pk_sql, '%TABLE_NAME%', i_table_name);
      l_index_sql := replace(l_index_sql, '%PK_PREFIX%', c_pk_prefix);

      begin
        execute immediate l_index_sql;
      exception
        when others then
          raise_application_error(-20001,
                                  'Ошибка при создании первичного ключа таблицы логирования ' || c_pk_prefix ||
                                  i_table_name || 'Текст ошибки: ' || dbms_utility.format_error_stack || chr(10) ||
                                  dbms_utility.format_error_backtrace);
      end;
    end create_log_indexes;

    procedure create_log_sequence(i_table_name in varchar2) is
      c_seq_sql constant varchar2(32767) := 'create sequence %SEQ_PREFIX%%TABLE_NAME%';
      l_seq_sql varchar2(32767);
    begin
      l_seq_sql := replace(c_seq_sql, '%TABLE_NAME%', i_table_name);
      l_seq_sql := replace(l_seq_sql, '%SEQ_PREFIX%', c_seq_prefix);

      begin
        execute immediate l_seq_sql;
      exception
        when others then
          raise_application_error(-20001,
                                  'Ошибка при создании последовательности ' || c_seq_prefix || i_table_name ||
                                  'Текст ошибки: ' || dbms_utility.format_error_stack || chr(10) ||
                                  dbms_utility.format_error_backtrace);
      end;
    end create_log_sequence;

    procedure create_structures(i_table_name     in varchar2,
                                i_retention_days in number) is
      pragma autonomous_transaction;
      l_chk integer;
    begin
      -- Проверим что существует таблица для записи логов
      begin
        select 1
          into l_chk
          from dual
         where exists (select 1
                  from all_tables at
                 where at.table_name = i_table_name
                   and at.owner = c_log_schema);
      exception
        when no_data_found then
          create_log_table(i_table_name, i_retention_days);
      end;

      -- Проверим что существуют индексы для записи логов
      begin
        select 1
          into l_chk
          from dual
         where exists (select 1
                  from all_indexes ai
                 where ai.index_name = c_pk_prefix || i_table_name
                   and ai.owner = c_log_schema);
      exception
        when no_data_found then
          create_log_indexes(i_table_name);
      end;

      -- Проверим что существуют последовательности для записи логов
      begin
        select 1
          into l_chk
          from dual
         where exists (select 1
                  from all_sequences aseq
                 where aseq.sequence_name = c_seq_prefix || i_table_name
                   and aseq.sequence_owner = c_log_schema);
      exception
        when no_data_found then
          create_log_sequence(i_table_name);
      end;

      commit;
    end create_structures;

  begin
    if (i_table.table_name is null and i_table.table_id is null) then
      raise e_null_agr;
    end if;
    if i_table.table_id is null then
      insert into tlog_tables
        (table_id,
         s_table_name,
         n_retention_days)
      values
        (seq_tlog_tables.nextval,
         i_table.table_name,
         nvl(i_table.retention_days, c_default_retention))
      returning table_id, n_retention_days into i_table.table_id, i_table.retention_days;
    else
      update tlog_tables f
         set f.s_table_name     = nvl(i_table.table_name, f.s_table_name),
             f.n_retention_days = nvl(i_table.retention_days, f.n_retention_days)
       where f.table_id = i_table.table_id
      returning n_retention_days into i_table.retention_days;
      if sql%rowcount = 0 then
        insert into tlog_tables
          (table_id,
           s_table_name,
           n_retention_days)
        values
          (i_table.table_id,
           i_table.table_name,
           nvl(i_table.retention_days, c_default_retention))
        returning table_id, n_retention_days into i_table.table_id, i_table.retention_days;
      end if;
    end if;

    create_structures(upper(i_table.table_name), i_table.retention_days);
  exception
    when e_null_agr then
      raise_application_error(-20000, 'tlog_support.create_table table_name and file_id is null');
  end create_table;

  procedure create_module(i_module in out nocopy t_tlog_module) is
    e_null_agr exception;
    e_no_rows  exception;
  begin
    if i_module.module_id is null and (i_module.module_name is null or i_module.program_name is null or
       (i_module.file_id is null and i_module.table_id is null) or
       (i_module.file_logging_level is null and i_module.table_logging_level is null) or
       i_module.output_logging_level is null) then
      raise e_null_agr;
    end if;
    if (i_module.module_id is null) then

      insert into tlog_modules
        (module_id,
         s_module_name,
         s_program_name,
         s_action,
         s_owner,
         file_id,
         table_id,
         n_file_logging_level,
         n_table_logging_level,
         n_output_logging_level)
      values
        (seq_tlog_modules.nextval,
         i_module.module_name,
         i_module.program_name,
         i_module.action,
         i_module.owner,
         i_module.file_id,
         i_module.table_id,
         nvl(i_module.file_logging_level, tlog.gc_off),
         nvl(i_module.table_logging_level, tlog.gc_info),
         nvl(i_module.output_logging_level, tlog.gc_off))
      returning module_id into i_module.module_id;

    else

      update tlog_modules m
         set s_module_name          = nvl(i_module.module_name, m.s_module_name),
             s_program_name         = nvl(i_module.program_name, m.s_program_name),
             s_action               = nvl(i_module.action, m.s_action),
             s_owner                = nvl(i_module.owner, m.s_owner),
             file_id                = nvl(i_module.file_id, m.file_id),
             table_id               = nvl(i_module.table_id, m.table_id),
             n_file_logging_level   = nvl(i_module.file_logging_level, m.n_file_logging_level),
             n_table_logging_level  = nvl(i_module.table_logging_level, m.n_table_logging_level),
             n_output_logging_level = nvl(i_module.output_logging_level, m.n_output_logging_level)
       where m.module_id = i_module.module_id;
      if sql%rowcount = 0 then
        insert into tlog_modules
          (module_id,
           s_module_name,
           s_program_name,
           s_action,
           s_owner,
           file_id,
           table_id,
           n_file_logging_level,
           n_table_logging_level,
           n_output_logging_level)
        values
          (i_module.module_id,
           i_module.module_name,
           i_module.program_name,
           i_module.action,
           i_module.owner,
           i_module.file_id,
           i_module.table_id,
           nvl(i_module.file_logging_level, tlog.gc_off),
           nvl(i_module.table_logging_level, tlog.gc_info),
           nvl(i_module.output_logging_level, tlog.gc_off));
      end if;

    end if;

  exception
    when e_null_agr then
      raise_application_error(-20000,
                              'tlog_support.create_module параметры заполнены не верно');
  end create_module;

end tlog_support;
/
