create or replace package tlog is
  /******************************************************************************

     NAME:       TLOG
     подсистема логирования.
     Поддерживает вывод логов в таблицы, файлы файловой системы сервера БД (через utl_file) и в dbms_output

     Использование:
     Для настройки логирования своих модулей необходимо заполнить:

     1. TLOG_FILES - определяет директорию, и формат имени файла (при необходимости записывать в файлы).
     S_FILE_BASE - основная часть имени файла файла
     N_RETENTION_DAYS - Количество дней, в течении которых будут хранится логи в архиве
     Итоговый файл имеет вид: [retention_days]_[YYYYMMDD]_[sсhema_name]_[file_base].log
     Для вызовов из анонимных PLSQL блоков sсhema_name = anonymous_block

     2. TLOG_TABLES - определяет директорию, и формат имени файла (при необходимости записывать в файлы).
     S_TABLE_NAME - название таблицы
     RETENTION_DAYS - Количество дней, в течении которых будут хранится логи в таблице

     3. TLOG_MODULES - настройки детальности логирования в таблицы, файлы и в dbms_output.
     MODULE_NAME - Имя или описание для удобства ведения настроек. задается название модуля или подсистемы для которой настраивается логирование. На функционал не влияет
     program_name заполняется в UPPER_CASE. указывает на пакет  или standalone процедуру/функцию
     Если ACTION пустой - то настройки логирования применяются ко всем вызовам функций логирования из данного пакета,
     Если ACTION указан, то настройки логирования применяются ко всем вызовам функций из указаного пакета с указаным действием (action)
     ACTION_NAME - передается параметром в каждую функцию логирования
     Также можно указать OWNER - имя схемы владельца объекта, из которого осуществляется вызов логера.

     Настройки логирования по-умолчанию задаются в таблице TLOG_MODULES в строке module_id=0

     Уровни логирования:
     FATAL = 100;
     ERROR = 200;
     WARN  = 300;
     INFO  = 400;
     DEBUG = 500;
     TRACE = 600;

     В функии логирования помимо строки-сообщения лога, можно передать коллекцию доп. строк (t_add_info),
     значения которой будут добавлены после основной строки лога через разделители "|"

     Формируемая в лог строка содержит:
     Дата
     Критичность(уровень) сообщения
     Пакет, вызвавший логирование
     Строка в пакете вызвавшая логирование
     Действие - параметр переданный в функцию логирования
     Сообщение
     Пользователь, залогиневшийся в БД
     SID - для возможности разбирать парралельные события
     стек вызова
     стек ошибки для записей вызванных с уровнем ERROR и FATAL(dbms_utility.format_error_backtrace)
     дополнительные поля, переданные в i_add_info$rec при логировании, разделенные "|"

     для удобства отладки в сессии можно переопределить уровень логирования и файл, в который будут попадать логи
     Настройки можно определить как для конкретного действия, так и для всех действий пакета или всех вызовов TLOG
     set_session_logginig_level
     set_session_logginig_file

  ******************************************************************************/

  /********Уровни логирования*****/
  gc_off   constant integer := 0;
  gc_fatal constant integer := 100;
  gc_error constant integer := 200;
  gc_warn  constant integer := 300;
  gc_info  constant integer := 400;
  gc_debug constant integer := 500;
  gc_trace constant integer := 600;
  gc_all   constant integer := 1000;
  /*******************/

  /**Названия уровней логирования*/
  gc_name_off   constant varchar2(5) := 'OFF';
  gc_name_fatal constant varchar2(5) := 'FATAL';
  gc_name_error constant varchar2(5) := 'ERROR';
  gc_name_warn  constant varchar2(5) := 'WARN';
  gc_name_info  constant varchar2(5) := 'INFO';
  gc_name_debug constant varchar2(5) := 'DEBUG';
  gc_name_trace constant varchar2(5) := 'TRACE';
  gc_name_all   constant varchar2(5) := 'ALL';
  /*******************/

  gc_all_units       constant varchar2(5 char) := '!' || gc_name_all;
  gc_default_program constant varchar2(10) := 'DEFAULT';

  g_output_mask varchar2(4000) := '%LOGID%%DELIMITER%%LOGTEXT%';

  --дополнительная информация добавляемая к основной строке лога
  type t_add_info is table of varchar2(32750) index by pls_integer;

  gc_empty_add_info t_add_info;

  --сбросить на диск буффер всех открытых файлов
  procedure flush_buffer;

  --закрыть все открытые файлы
  procedure close_all;

  procedure trace(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info);

  procedure debug(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info);

  procedure info(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info);

  procedure warn(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info);

  procedure error(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info);

  procedure fatal(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info);

  procedure trace(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2);

  procedure debug(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2);

  procedure info(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2);

  procedure warn(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2);

  procedure error(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2);

  procedure fatal(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2);

  --установить уровень логирования для действия на уровне сессии
  --i_program_name = Для установки уровня логирования для всех программ необходимо использовать константу g_all_units
  --Для установки уровня логирования для всех действий необходимо использовать константу g_all_units
  --Если  заданы настройки для i_program_name = gc_all_units - они переопределяют любые другие заданные настройки
  --Настройки для i_action = gc_all_units переопределяют настройки для конкретного i_action
  --с помощью данной процедуры можно только увеличить уровень логирования, заданный в настройках tlog_modules
  procedure set_session_logginig_level(i_file_logging_level   tlog_modules.n_file_logging_level%type,
                                       i_table_logging_level  tlog_modules.n_table_logging_level%type,
                                       i_output_logging_level tlog_modules.n_output_logging_level%type,
                                       i_program_name         tlog_modules.s_program_name%type default gc_all_units,
                                       i_action               tlog_modules.s_action%type default gc_all_units);

  --Выводим в output настройки указанного модуля и действия
  procedure print_module_settings(i_owner        tlog_modules.s_owner%type,
                                  i_program_name tlog_modules.s_program_name%type,
                                  i_action       tlog_modules.s_action%type);

  --Логируется ли DEBUG для данного пакета и действия
  function is_debug(i_action in varchar2) return boolean;
  --Логируется ли INFO для данного пакета и действия
  function is_info(i_action in varchar2) return boolean;
  --Логируется ли WARN для данного пакета и действия
  function is_warn(i_action in varchar2) return boolean;
  --Логируется ли ERROR для данного пакета и действия
  function is_error(i_action in varchar2) return boolean;
  --Логируется ли TRACE для данного пакета и действия
  function is_trace(i_action in varchar2) return boolean;
  --Логируется ли FATAL для данного пакета и действия
  function is_fatal(i_action in varchar2) return boolean;

end tlog;
/
create or replace package body tlog is

  gc_delimeter constant varchar2(1) := '|';

  type t_file is record(
    file_name      varchar2(100 char),
    file_handle    utl_file.file_type,
    directory_name tlog_files.s_directory_name%type,
    file_base      tlog_files.s_file_base%type,
    retention_days tlog_files.n_retention_days%type);

  type t_file_handles is table of t_file index by varchar2(100 char);

  --Кэш открытых файлов
  --Операция открытия достаточно дорогая относительно записи в него поэтому держим файлы открытыми
  -- ключ кэша имеет вид schema.file_id, при этом file_id <0
  g_file_handles t_file_handles;

  type t_table is record(
    table_name varchar2(100 char));

  type t_table_handles is table of t_table index by varchar2(100 char);

  --Кэш найденны таблиц
  -- ключ кэша имеет вид schema.table_id, при этом table_id <0
  g_table_handles t_table_handles;

  type t_settings is record(
    file_id              integer,
    table_id             integer,
    file_logging_level   integer,
    table_logging_level  integer,
    output_logging_level integer);

  --кэш метаданных модулей, чтобы не ходить каждый раз в таблицу
  type t_settings_table is table of t_settings index by varchar2(201 char);
  --ключ кэша имеет вид schema.programm.action
  g_actions_settings t_settings_table;

  type t_log_record is record(
    llevel       integer,
    lprogram     varchar2(100 char),
    lline        integer,
    laction      varchar2(100 char),
    ltext        varchar2(4000),
    lstack       varchar2(4000),
    lerror_stack varchar2(4000));

  type t_output_record is record (
    delimiter    varchar2(1),
    log_id       integer,
    log_text     varchar2(4000));

  --сбросить на диск буффер всех открытых файлов
  procedure flush_buffer is
    l_index varchar2(50 char);
  begin
    l_index := g_file_handles.first;
    while l_index is not null loop
      if utl_file.is_open(g_file_handles(l_index).file_handle) then
        utl_file.fflush(g_file_handles(l_index).file_handle);
      end if;
      l_index := g_file_handles.next(l_index);
    end loop;
  end flush_buffer;

  --закрыть все открытые файлы
  procedure close_all is
    l_index varchar2(100 char);
  begin
    l_index := g_file_handles.first;
    while l_index is not null loop
      if utl_file.is_open(g_file_handles(l_index).file_handle) then
        utl_file.fclose(g_file_handles(l_index).file_handle);
      end if;
      l_index := g_file_handles.next(l_index);
    end loop;
    g_file_handles.delete;
  end close_all;

  --получаем пакет и строку вызвавшего нас, а так же укороченный стек вызова, если i_get_stack = 1
  procedure get_stack_info(o_owner     out nocopy varchar2,
                           o_caller    out nocopy varchar2,
                           o_line      out nocopy integer,
                           o_stack_str out nocopy varchar2,
                           i_get_stack in integer default 0) is

    l_owner     varchar2(100);
    l_name      varchar2(100);
    l_prev_name varchar2(100) := ' ';

    l_lineno varchar2(100);

    l_call_stack  varchar2(4096) default dbms_utility.format_call_stack;
    n             number;
    l_found_stack boolean default false;
    l_line        varchar2(255);

    c_nl_char constant varchar2(1) := chr(10);
    c_my_name constant varchar2(2000) := $$plsql_unit;
  begin
    loop
      n := instr(l_call_stack, c_nl_char);
      exit when(n is null or n = 0);
      --
      l_line       := ltrim(substr(l_call_stack, 1, n - 1));
      l_call_stack := substr(l_call_stack, n + 1);
      --
      if (not l_found_stack) then
        if (l_line like '%handle%number%name%') then
          l_found_stack := true;
        end if;
      elsif l_line not like '%.' || c_my_name then

        n := instr(l_line, ' ');
        if (n > 0) then
          l_line := ltrim(substr(l_line, n));
          n      := instr(l_line, ' ');
        end if;
        if (n > 0) then
          l_lineno := to_number(substr(l_line, 1, n - 1));
        else
          l_lineno := 0;
        end if;

        n       := instr(l_line, ' ', -1);
        l_name  := substr(l_line, n + 1);
        n       := instr(l_name, '.');
        l_owner := substr(l_name, 1, n - 1);
        l_name  := substr(l_name, n + 1);
        if l_name <> l_prev_name then
          if o_stack_str is not null then
            o_stack_str := o_stack_str || ')<--';
          end if;
          o_stack_str := o_stack_str || l_name || '(' || l_lineno;
          l_prev_name := l_name;
        else
          o_stack_str := o_stack_str || ',' || l_lineno;
        end if;

        if o_caller is null then
          o_owner  := nvl(l_owner, 'ANONYMOUS_BLOCK');
          o_caller := l_name;
          o_line   := l_lineno;
          if i_get_stack = 0 then
            o_stack_str := '';
            return;
          end if;
        end if;

      end if;
    end loop;
    o_stack_str := o_stack_str || ')';

  end get_stack_info;

  --Функция формирует строку для выдачи в формате заданном переменной g_output_mask
  --Вместо %LOGID% подставляется ид лога
  function apply_output_mask(i_output_record in t_output_record) return varchar2 is
    l_output_string varchar2(4000);
  begin
    l_output_string := g_output_mask;

    if i_output_record.delimiter is null then
      l_output_string := replace(l_output_string, '%DELIMITER%', gc_delimeter);
    else
      l_output_string := replace(l_output_string, '%DELIMITER%', i_output_record.delimiter);
    end if;

    l_output_string := replace(l_output_string, '%LOGID%', i_output_record.log_id);
    l_output_string := replace(l_output_string, '%LOGTEXT%', i_output_record.log_text);

    l_output_string := substr(l_output_string, 1, 4000);

    return l_output_string;
  end apply_output_mask;

  --Определяем модуль по переданому пакету и выполняемому действию
  -- 0 - модуль по умолчанию, если не заданы другие настройки
  --program_name может быть в виде имени пакета (standalone процедуры или функции)
  --либо в виде PACKAGE_NAME.ACTION. Где ACTION - строковая константа передаваемая в качестве параметра логирования
  function get_module_settings(i_owner          in tlog_modules.s_owner%type,
                               i_program_name   in tlog_modules.s_program_name%type,
                               i_action         in tlog_modules.s_action%type,
                               i_print_settings in boolean default false) return t_settings is
    l_settings        t_settings;
    l_action_settings t_settings;

    l_owner        tlog_modules.s_owner%type := upper(i_owner);
    l_program_name tlog_modules.s_program_name%type := upper(i_program_name);
    l_action       tlog_modules.s_action%type := upper(i_action);

    l_full_name varchar2(201 char) := l_owner || '.' || l_program_name || '.' || l_action;

    procedure print_settings(i_settings in t_settings) is
    begin
      dbms_output.put_line('file_id: ' || i_settings.file_id);
      dbms_output.put_line('table_id: ' || i_settings.table_id);
      dbms_output.put_line('file_logging_level: ' || i_settings.file_logging_level);
      dbms_output.put_line('table_logging_level: ' || i_settings.table_logging_level);
      dbms_output.put_line('output_logging_level: ' || i_settings.output_logging_level);
    end print_settings;

  begin
    --Проверим не установлено ли в сессии настройки для всех программ
    if g_actions_settings.exists(gc_all_units) then
      l_settings := g_actions_settings(gc_all_units);
      if i_print_settings then
        dbms_output.put_line('Обнаружены переопределенные настройки для всех программ');
        print_settings(l_settings);
      end if;
      --Проверим не установлено ли в сессии настройки для всех действий указанной программы
    elsif g_actions_settings.exists(l_program_name || '.' || gc_all_units) then
      l_settings := g_actions_settings(l_program_name || '.' || gc_all_units);
      if i_print_settings then
        dbms_output.put_line('Обнаружены переопределенные настройки для всех действий программы ' || l_program_name);
        print_settings(l_settings);
      end if;
      --Проверим не установлено ли настроек для конкретного действия
    elsif g_actions_settings.exists(gc_all_units || '.' || l_program_name || '.' || l_action) then
      l_settings := g_actions_settings(gc_all_units || '.' || l_program_name || '.' || l_action);
      if i_print_settings then
        dbms_output.put_line('Обнаружены переопределенные настройки действия ' || l_program_name || '.' || l_action);
        print_settings(l_settings);
      end if;
    end if;
    --Если сессионных глобальных установок нет, или они не полные,
    --И если не запрошены настройки для самих глобальных настроек, то смотрим настройки в табличке
    if (l_program_name <> gc_all_units) and (l_action <> gc_all_units) and (l_owner <> gc_all_units) then
      --Ищем в кэше настройки для указанного действия
      if g_actions_settings.exists(l_full_name) then
        l_action_settings := g_actions_settings(l_full_name);
        if i_print_settings then
          dbms_output.put_line('Настройки действия взяты из кэша');
          print_settings(l_action_settings);
        end if;

        --не нашли - считаем из базы и положим в кэш
      else
        --module_id=0 - минимальный приоритет
        --у action+owner - максимальный
        --для двух строк-  owner заполнен, а action -нет и наоборот owner пустой, а action - заполнен, победит строчка с заполненым owner
        select /*+ result_cache*/
         min(m.file_id) keep(dense_rank first order by decode(m.module_id, 0, 8, 0) + decode(m.s_program_name, gc_default_program, 4, 0) + nvl2(m.s_owner, 0, 1) + nvl2(m.s_action, 0, 2)),
         min(m.table_id) keep(dense_rank first order by decode(m.module_id, 0, 8, 0) + decode(m.s_program_name, gc_default_program, 4, 0) + nvl2(m.s_owner, 0, 1) + nvl2(m.s_action, 0, 2)),
         max(m.n_file_logging_level) keep(dense_rank first order by decode(m.module_id, 0, 8, 0) + decode(m.s_program_name, gc_default_program, 4, 0) + nvl2(m.s_owner, 0, 1) + nvl2(m.s_action, 0, 2)),
         max(m.n_table_logging_level) keep(dense_rank first order by decode(m.module_id, 0, 8, 0) + decode(m.s_program_name, gc_default_program, 4, 0) + nvl2(m.s_owner, 0, 1) + nvl2(m.s_action, 0, 2)),
         max(m.n_output_logging_level) keep(dense_rank first order by decode(m.module_id, 0, 8, 0) + decode(m.s_program_name, gc_default_program, 4, 0) + nvl2(m.s_owner, 0, 1) + nvl2(m.s_action, 0, 2))
          into l_action_settings
          from tlog_modules m
         where m.module_id = 0 -- Модуль логирования по умолчанию
            or (l_owner = m.s_owner and m.s_program_name = gc_default_program and l_action = nvl(m.s_action, l_action)) -- Модуль логирования для схемы в целом
            or (m.s_program_name = l_program_name and l_action = nvl(m.s_action, l_action) and
               l_owner = nvl(m.s_owner, l_owner)); -- Модуль логирования для программы, при передаче - с учетом действия и владельца
        g_actions_settings(l_full_name) := l_action_settings;

        if i_print_settings then
          dbms_output.put_line('Настройки действия определены по метаданным');
          print_settings(l_action_settings);
        end if;
      end if;

      l_settings.file_id  := nvl(l_settings.file_id, l_action_settings.file_id);
      l_settings.table_id := nvl(l_settings.table_id, l_action_settings.table_id);
      --Переопределять можно только в сторону увеличения подробности логирования
      l_settings.file_logging_level := greatest(nvl(l_settings.file_logging_level, l_action_settings.file_logging_level),
                                                l_action_settings.file_logging_level);
      --Переопределять можно только в сторону увеличения подробности логирования
      l_settings.table_logging_level := greatest(nvl(l_settings.table_logging_level,
                                                     l_action_settings.table_logging_level),
                                                 l_action_settings.table_logging_level);
      --Переопределять можно только в сторону увеличения подробности логирования
      l_settings.output_logging_level := greatest(nvl(l_settings.output_logging_level,
                                                      l_action_settings.output_logging_level),
                                                  l_action_settings.output_logging_level);

    end if;

    if i_print_settings then
      dbms_output.put_line('Итоговые настройки');
      print_settings(l_settings);
    end if;

    return l_settings;

  end get_module_settings;

  --Формируем строчку лога и записываем в файл в соответсвии с настрийками модуля.
  procedure put_record(i_owner       in tlog_modules.s_owner%type,
                       i_settings    in t_settings,
                       i_log_record  in t_log_record,
                       i_append_info in clob,
                       i_tab_info    in t_add_info,
                       o_log_id      out nocopy integer) is

    l_file_handle utl_file.file_type;
    l_table_name  tlog_tables.s_table_name%type;

    function delimeter(p_count integer := 1) return varchar2 is
    begin
      return lpad(gc_delimeter, p_count, gc_delimeter);
    end delimeter;

    --Получить указатель на файл в соответсвии с настройками и схемой, из которой производится логирование
    --Открытые файлы кешируются в g_file_handles
    function get_file_handle(i_owner   in tlog_modules.s_owner%type,
                             i_file_id in integer) return utl_file.file_type is
      l_file_name   varchar2(100 char);
      l_file_handle utl_file.file_type;
      l_file        t_file;
      l_owner       tlog_modules.s_owner%type := upper(i_owner);

      l_file_index varchar2(50 char) := gc_all_units || '.' || i_file_id;
      --генерация имени файла
      function get_file_name(i_prefix         in tlog_modules.s_owner%type,
                             i_file_base      in tlog_files.s_file_base%type,
                             i_retention_days in tlog_files.n_retention_days%type) return varchar2 is
        l_file_name varchar2(100 char);
      begin
        if i_prefix is null or i_file_base is null or i_retention_days is null then
          raise_application_error(-20000, 'p_prefix, p_file_base и p_retention_days не должны быть NULL');
        end if;
        l_file_name := lower(to_char(i_retention_days) || '_' || to_char(current_date, 'yyyymmdd') || '_' || i_prefix || '_' ||
                             i_file_base || '.log');

        return l_file_name;
      end get_file_name;

      --Открыть файл на запись. При необходимости файл создается.
      function fopen(i_directory_name in varchar2,
                     i_file_name      in varchar2,
                     i_file_id        in integer) return utl_file.file_type is
        pragma autonomous_transaction;
        l_file_exist      boolean;
        l_size            number;
        l_block_size      number;
        l_dummy           number;
        l_file_handle_loc utl_file.file_type;
      begin
        --Проверяем существует ли файл, и если нет, то создаем его и ПЕРЕОТКРЫВАЕМ,
        -- т.к. при создании файла Oracle открывает его в режиме write, а не в нужном нам append
        -- А при парралелльной записи в режиме write часть записей теряется.
        utl_file.fgetattr(i_directory_name, i_file_name, l_file_exist, l_size, l_block_size);
        --i_file_id может быть отрицательным и не существовать в таблице для случая, если в сессии было настроено логирование
        --В свой собственный файл
        --Для таких случаев блокровку не ставим и файл не пересоздаем
        if (not l_file_exist) and (i_file_id >= 0) then
          --Ставим блокировку чтобы никто параллельно не создал файл
          select m.file_id
            into l_dummy
            from tlog_files m
           where m.file_id = i_file_id
             for update;

          --ещё раз проверяем не создал ли кто-то файл после блокировки
          utl_file.fgetattr(i_directory_name, i_file_name, l_file_exist, l_size, l_block_size);

          if (not l_file_exist) then
            l_file_handle_loc := utl_file.fopen(i_directory_name, i_file_name, 'W', 1);
            utl_file.fclose(l_file_handle_loc);
          end if;
          commit;

        end if;
        l_file_handle_loc := utl_file.fopen(i_directory_name, i_file_name, 'AB', 32767);

        return l_file_handle_loc;

      end fopen;
    begin
      --Проверим кэш
      if g_file_handles.exists(l_file_index) then
        l_file := g_file_handles(l_file_index);
      end if;

      --Если настройки не были найдены, то прочитаем в метаданных
      if l_file.directory_name is null then
        begin
          select /*+ result_cache*/
           f.s_directory_name,
           f.s_file_base,
           f.n_retention_days
            into l_file.directory_name,
                 l_file.file_base,
                 l_file.retention_days
            from tlog_files f
           where f.file_id = i_file_id;
        exception
          when no_data_found then
            raise_application_error(-20001,
                                    'Из-за настроек модуля, невозможно совершить запись в файл');
        end;
        l_file_name := get_file_name(l_owner, l_file.file_base, l_file.retention_days);
        l_file.file_name := l_file_name;
        l_file_handle := fopen(l_file.directory_name, l_file.file_name, i_file_id);
        l_file.file_handle := l_file_handle;
        g_file_handles(l_file_index) := l_file;
      else
        l_file_handle := l_file.file_handle;
        --Сгенерируем имя файла. Оно могло измениться, если предыдущий вызов был в предыдущем дне (перешагнули через полночь)
        l_file_name := get_file_name(l_owner, l_file.file_base, l_file.retention_days);

        if (l_file.file_name = l_file_name) then
          if not utl_file.is_open(l_file_handle) then
            l_file_handle := fopen(l_file.directory_name, l_file.file_name, i_file_id);
            g_file_handles(l_file_index).file_handle := l_file_handle;
          end if;
        else
          --Если имя все-таки изменилось, то закроем старый файл и откроем новый.
          if utl_file.is_open(l_file_handle) then
            utl_file.fclose(l_file_handle);
          end if;
          l_file.file_name := l_file_name;

          l_file_handle := fopen(l_file.directory_name, l_file.file_name, i_file_id);
          l_file.file_handle := l_file_handle;
          g_file_handles(l_file_index) := l_file;

        end if;
      end if;

      return l_file_handle;

    end get_file_handle;

    -- Получение названия таблицы для логирования
    function get_table_name(i_table_id in number) return varchar2 is
      l_table_index varchar2(50 char) := gc_all_units || '.' || i_table_id;
      l_table       t_table;
    begin
      --Проверим кэш
      if g_table_handles.exists(l_table_index) then
        l_table := g_table_handles(l_table_index);
      end if;

      --Если не нашли в кэше, выполняем запрос и сохраняем ответ в кэше
      if l_table.table_name is null then
        begin
          select /*+ result_cache*/
           t.s_table_name
            into l_table.table_name
            from tlog_tables t
           where t.table_id = i_table_id;
        exception
          when no_data_found then
            raise_application_error(-20001,
                                    'Из-за настроек модуля, невозможно совершить запись в таблицу');
        end;

        g_table_handles(l_table_index) := l_table;
      end if;

      return l_table.table_name;
    end get_table_name;

    function prepare_string(i_str       in varchar2,
                            i_delimeter in varchar2 default ' ') return varchar2 is
    begin
      if (i_delimeter = ' ' and i_str is null) then
        return '""';
      else
        -- Экранируем строку если в ней есть разделитель или она начинается или кончается с двойной кавычки
        return case when(instr(i_str, i_delimeter) > 0) or(substr(i_str, 1, 1) = '"') or(substr(i_str, -1, 1) = '"') then '"' || replace(i_str,
                                                                                                                                         '"',
                                                                                                                                         '""') || '"' else i_str end;
      end if;
    end prepare_string;

    -- Получение наименования уровня логирования по его ИД
    function get_log_level_name(i_level_id in number) return varchar2 is
    begin
      return case i_level_id when gc_debug then gc_name_debug when gc_error then gc_name_error when gc_fatal then gc_name_fatal when gc_info then gc_name_info when gc_trace then gc_name_trace when gc_warn then gc_name_warn end;
    end get_log_level_name;

    --формируем основную строку лога
    --Разделители - пробелы
    --значения, содержащие пробелы экранируем кавычками
    function get_log_string(i_log_record in t_log_record) return varchar2 is
      l_log_string varchar2(32767);
    begin
      l_log_string :=  --Дата
       to_char(current_timestamp, 'yyyy-mm-dd.hh24:mi:ss.FF4FM') || ' ' ||
                     --Уровень логирования
                      get_log_level_name(i_log_record.llevel) || ' ' ||
                     --модуль
                      prepare_string(i_log_record.lprogram) || ' ' ||
                     -- строка
                      i_log_record.lline || ' ' ||
                     -- действие
                      prepare_string(i_log_record.laction) || ' ' ||
                     --текст
                      prepare_string(i_log_record.ltext) || ' ' ||
                     --пользователь
                      sys_context('USERENV', 'SESSION_USER') || ' ' ||
                     --сессия
                      sys_context('USERENV', 'SID') || ' ' ||
                     --стек
                      i_log_record.lstack || ' ' ||
                     --стек ошибок
                      prepare_string(i_log_record.lerror_stack);
      return l_log_string;
    end get_log_string;

    --пишем переданную строку и добавляем доп.инфо
    --пишем в файл и в dbms_output порциями по 32к символов
    procedure put_file_record_int(i_buffer      in varchar2,
                                  i_append_info in clob,
                                  i_tab_info    in t_add_info,
                                  i_file_handle in utl_file.file_type,
                                  i_settings    in t_settings,
                                  i_force_flush in boolean default false) is
      l_idx      pls_integer := 0;
      l_cnt      pls_integer;
      l_prev_inx pls_integer := 0;

      c_buf_size pls_integer := 16000;

      l_buffer_str varchar2(32767) := i_buffer;
      l_tmp_buffer varchar2(32767);

      procedure put(i_file_handle in utl_file.file_type,
                    i_settings    in t_settings,
                    i_string      in varchar2,
                    i_last_part   in boolean default false,
                    i_force_flush in boolean default false) is
        l_buffer raw(32767);
        c_newline_delimeter constant varchar2(1) := chr(10);
      begin
        if (i_log_record.llevel <= i_settings.file_logging_level) then

          if i_last_part then
            l_buffer := utl_raw.cast_to_raw(i_string || c_newline_delimeter);
          else
            l_buffer := utl_raw.cast_to_raw(i_string);
          end if;

          utl_file.put_raw(i_file_handle, l_buffer, i_force_flush);
        end if;

        if (i_log_record.llevel <= i_settings.output_logging_level) then
          dbms_output.put_line(i_string);
        end if;
      end put;

    begin

      -- Запишем информацию из CLOBа
      if i_append_info is not null then
        l_cnt := trunc(length(i_append_info) / c_buf_size);

        while l_idx <= l_cnt loop
          l_buffer_str := prepare_string(substr(i_append_info, l_idx * c_buf_size + 1, c_buf_size));
          put(i_file_handle, i_settings, l_buffer_str);
          l_idx := l_idx + 1;
        end loop;

        l_buffer_str := l_buffer_str || gc_delimeter;
      end if;

      -- Запишем информацию из коллекции
      l_idx := i_tab_info.first;
      while l_idx is not null loop
        if i_tab_info(l_idx) is not null then
          l_tmp_buffer := delimeter(l_idx - l_prev_inx) || prepare_string(i_tab_info(l_idx), gc_delimeter);
          -- Эксперементально установлено что dbms_output падает при длинне >32512
          if length(l_tmp_buffer) + length(l_buffer_str) > c_buf_size then
            put(i_file_handle, i_settings, l_buffer_str);

            l_buffer_str := '';
          end if;

          l_buffer_str := l_buffer_str || l_tmp_buffer;
          l_prev_inx   := l_idx;
        end if;
        l_idx := i_tab_info.next(l_idx);

      end loop;

      -- Терминатор сроки
      l_buffer_str := l_buffer_str || gc_delimeter;
      put(i_file_handle => i_file_handle,
          i_settings    => i_settings,
          i_string      => l_buffer_str,
          i_last_part   => true,
          i_force_flush => i_force_flush);

    end put_file_record_int;

    -- Запись в таблицу. Происходит в автономной транзакции
    procedure put_table_record_int(i_log_record  in  t_log_record,
                                   i_owner       in  tlog_modules.s_owner%type,
                                   i_append_info in  clob,
                                   i_tab_info    in  t_add_info,
                                   i_table_name  in  tlog_tables.s_table_name%type,
                                   o_log_id      out nocopy integer) is
      pragma autonomous_transaction;

      c_log_sql constant varchar2(4000) := 'begin
        insert into %TABLE_NAME% (log_id, s_log_level, s_progam_name, s_log_line, s_log_action, s_log_text, s_owner, s_call_stack, s_error_stack, cl_add_info)
        values (seq_%TABLE_NAME%.nextval, :log_level, :progam_name, :log_line, :log_action, :log_text, :owner, :call_stack, :error_stack, :add_info)
        returning log_id into :log_id;
        commit;
      end;';

      l_add      clob;
      l_sql      varchar2(4000);
      l_idx      number;
      l_prev_inx number;
    begin

      -- Запишем информацию из CLOBа
      if i_append_info is not null then
        l_add := i_append_info || gc_delimeter;
      end if;

      l_sql := replace(c_log_sql, '%TABLE_NAME%', i_table_name);

      -- Запишем информацию из коллекции
      l_idx := i_tab_info.first;
      while l_idx is not null loop
        if i_tab_info(l_idx) is not null then
          l_add      := l_add || delimeter(l_idx - l_prev_inx) || i_tab_info(l_idx);
          l_prev_inx := l_idx;
        end if;
        l_idx := i_tab_info.next(l_idx);
      end loop;

      execute immediate l_sql
        using get_log_level_name(i_log_record.llevel), i_log_record.lprogram, i_log_record.lline, i_log_record.laction, i_log_record.ltext, i_owner, i_log_record.lstack, i_log_record.lerror_stack, l_add, out o_log_id;
    end put_table_record_int;

  begin
    -- Ветка для логирования в файлы и dbms_output
    if (i_log_record.llevel <= greatest(i_settings.file_logging_level, i_settings.output_logging_level)) then
      if (i_log_record.llevel <= i_settings.file_logging_level) then
        l_file_handle := get_file_handle(i_owner, i_settings.file_id);
      end if;
      put_file_record_int(get_log_string(i_log_record), i_append_info, i_tab_info, l_file_handle, i_settings, true);
      -- Ветка для логирования в таблицы
    elsif (i_log_record.llevel <= i_settings.table_logging_level) then
      l_table_name := get_table_name(i_settings.table_id);
      put_table_record_int(i_log_record, i_owner, i_append_info, i_tab_info, l_table_name, o_log_id);
    end if;

  end put_record;

  -- Функция убирает из строки символы возврата каретки (CHR(13)) и заменяет символы новой строки (CHR(10)) на пробел
  function clear_newlines(p_text in varchar2) return varchar2 is
  begin
    return replace(replace(p_text, chr(13)), chr(10), ' ');
  end clear_newlines;

  -- Сформировать информацию по ошибке для логирования
  function get_error_backtrace return varchar2 is
  begin
    -- Вырезаем из стека ORA-06512 и последние пробелы и переносы строк
    return regexp_replace(clear_newlines(dbms_utility.format_error_stack || dbms_utility.format_error_backtrace),
                          '(ORA-06512: (at|на) |[[:space:]]*$)');
  end get_error_backtrace;

  procedure log_int(i_level         in integer,
                    i_action        in varchar2,
                    i_text          in varchar2,
                    i_append_info   in clob,
                    i_tab_info      in t_add_info default gc_empty_add_info,
                    o_output_string out nocopy varchar2) is
    l_log_record t_log_record;
    l_settings   t_settings;
    l_owner      tlog_modules.s_owner%type;

    l_output_record t_output_record;

  begin
    l_log_record.ltext   := substr(clear_newlines(i_text), 1, 4000);
    l_log_record.llevel  := i_level;
    l_log_record.laction := substr(clear_newlines(i_action), 1, 100);
    get_stack_info(o_owner     => l_owner,
                   o_caller    => l_log_record.lprogram,
                   o_line      => l_log_record.lline,
                   o_stack_str => l_log_record.lstack,
                   i_get_stack => 1);
    l_settings := get_module_settings(l_owner, l_log_record.lprogram, i_action);
    --Проверяем нужно ли логировать данную запись хотя бы в один приемник
    if (greatest(l_settings.file_logging_level, l_settings.table_logging_level, l_settings.output_logging_level) <
       i_level) then
      return;
    end if;

    if i_level <= gc_error then
      l_log_record.lerror_stack := substr(get_error_backtrace, 1, 4000);
    end if;

    l_output_record.log_text := l_log_record.ltext;
    put_record(i_owner       => l_owner,
               i_settings    => l_settings,
               i_log_record  => l_log_record,
               i_append_info => i_append_info,
               i_tab_info    => i_tab_info,
               o_log_id      => l_output_record.log_id);

    o_output_string := apply_output_mask(l_output_record);
  end log_int;

  procedure trace(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info) is
    l_log_text varchar2(4000);
  begin
    log_int(gc_trace, i_action, i_text, i_append_info, i_tab_info, l_log_text);
  end trace;

  procedure debug(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info) is
    l_log_text varchar2(4000);
  begin
    log_int(gc_debug, i_action, i_text, i_append_info, i_tab_info, l_log_text);
  end debug;

  procedure info(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info) is
    l_log_text varchar2(4000);
  begin
    log_int(gc_info, i_action, i_text, i_append_info, i_tab_info, l_log_text);
  end info;

  procedure warn(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info) is
    l_log_text varchar2(4000);
  begin
    log_int(gc_warn, i_action, i_text, i_append_info, i_tab_info, l_log_text);
  end warn;

  procedure error(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info) is
    l_log_text varchar2(4000);
  begin
    log_int(gc_error, i_action, i_text, i_append_info, i_tab_info, l_log_text);
  end error;

  procedure fatal(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info) is
    l_log_text varchar2(4000);
  begin
    log_int(gc_fatal, i_action, i_text, i_append_info, i_tab_info, l_log_text);
  end fatal;

  procedure trace(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2) is
  begin
    log_int(gc_trace, i_action, i_text, i_append_info, i_tab_info, o_log_text);
  end trace;

  procedure debug(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2) is
  begin
    log_int(gc_debug, i_action, i_text, i_append_info, i_tab_info, o_log_text);
  end debug;

  procedure info(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2) is
  begin
    log_int(gc_info, i_action, i_text, i_append_info, i_tab_info, o_log_text);
  end info;

  procedure warn(i_action      in varchar2,
                 i_text        in varchar2,
                 i_append_info in clob default null,
                 i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2) is
  begin
    log_int(gc_warn, i_action, i_text, i_append_info, i_tab_info, o_log_text);
  end warn;

  procedure error(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2) is
  begin
    log_int(gc_error, i_action, i_text, i_append_info, i_tab_info, o_log_text);
  end error;

  procedure fatal(i_action      in varchar2,
                  i_text        in varchar2,
                  i_append_info in clob default null,
                  i_tab_info    in t_add_info default gc_empty_add_info,
                  o_log_text    out nocopy varchar2) is
  begin
    log_int(gc_fatal, i_action, i_text, i_append_info, i_tab_info, o_log_text);
  end fatal;

  --установить уровень логирования для действия на уровне сессии
  --p_program_name = Для установки уровня логирования для всех программ необходимо использовать константу g_all_units
  --Для установки уровня логирования для всех действий необходимо использовать константу g_all_units
  --Если  заданы настройки для p_program_name = gc_all_units - они переопределяют любые другие заданные настройки
  --Настройки для p_action = gc_all_units переопределяют настройки для конкретного p_action
  --с помощью данной процедуры можно только увеличить уровень логирования, заданный в настройках tlog_modules
  procedure set_session_logginig_level(i_file_logging_level   tlog_modules.n_file_logging_level%type,
                                       i_table_logging_level  tlog_modules.n_table_logging_level%type,
                                       i_output_logging_level tlog_modules.n_output_logging_level%type,
                                       i_program_name         tlog_modules.s_program_name%type default gc_all_units,
                                       i_action               tlog_modules.s_action%type default gc_all_units) is
    l_settings  t_settings;
    l_full_name varchar2(201 char);
  begin
    if (i_program_name is null) then
      raise_application_error(-20000,
                              'Не указана программа для которой устанавливается уровень i_program_name is null. Для установки уровня логирования для всех программ необходимо использовать константу g_all_units');
    end if;
    if (i_action is null) then
      raise_application_error(-20000,
                              'Не указано действие для которой устанавливается уровень i_action is null. Для установки уровня логирования для всех действий необходимо использовать константу g_all_units');
    end if;

    if nvl(i_file_logging_level, -1) not in (gc_off, gc_fatal, gc_error, gc_warn, gc_info, gc_debug, gc_trace, gc_all) then
      raise_application_error(-20000,
                              'Не верно указан уровень логирования в файл i_file_logging_level');
    end if;
    if nvl(i_table_logging_level, -1) not in (gc_off, gc_fatal, gc_error, gc_warn, gc_info, gc_debug, gc_trace, gc_all) then
      raise_application_error(-20000,
                              'Не верно указан уровень логирования в файл i_table_logging_level');
    end if;
    if nvl(i_output_logging_level, -1) not in
       (gc_off, gc_fatal, gc_error, gc_warn, gc_info, gc_debug, gc_trace, gc_all) then
      raise_application_error(-20000,
                              'Не верно указан уровень логирования в аутпут i_output_logging_level');
    end if;

    if (i_program_name = gc_all_units) then
      l_full_name := gc_all_units;
    elsif (i_action = gc_all_units) then
      l_full_name := upper(i_program_name || '.' || gc_all_units);
    else
      l_full_name := upper(gc_all_units || '.' || i_program_name || '.' || i_action);
    end if;

    l_settings := get_module_settings(gc_all_units, i_program_name, i_action);

    l_settings.file_logging_level   := i_file_logging_level;
    l_settings.table_logging_level  := i_table_logging_level;
    l_settings.output_logging_level := i_output_logging_level;

    g_actions_settings(l_full_name) := l_settings;

  end set_session_logginig_level;

  --Выводим в output настройки указанного модуля и действия
  procedure print_module_settings(i_owner        tlog_modules.s_owner%type,
                                  i_program_name tlog_modules.s_program_name%type,
                                  i_action       tlog_modules.s_action%type) is
    l_settings t_settings;
  begin
    if (i_owner is null) or (i_program_name is null) or (i_action is null) then
      raise_application_error(-20000, 'i_owner, i_program_name и i_action должны быть NOT NULL');
    end if;
    l_settings := get_module_settings(i_owner, i_program_name, i_action, true);
  end print_module_settings;

  --логируются ли сообщения переданного уровня
  function is_level(i_level  in integer,
                    i_action in varchar2) return boolean is
    l_program  tlog_modules.s_module_name%type;
    l_line     integer;
    l_stack    varchar2(4000);
    l_settings t_settings;
    l_owner    tlog_modules.s_owner%type;
  begin

    if (i_level is null) or (i_action is null) then
      raise_application_error(-20000, 'i_level и i_action должны быть NOT NULL');
    end if;

    get_stack_info(l_owner, l_program, l_line, l_stack, i_get_stack => 0);
    l_settings := get_module_settings(l_owner, l_program, i_action);

    return i_level <= greatest(l_settings.output_logging_level, l_settings.file_logging_level);

  end is_level;

  --Логируется ли DEBUG для данного пакета и действия
  function is_debug(i_action in varchar2) return boolean is
  begin
    return is_level(gc_debug, i_action);
  end is_debug;
  --Логируется ли INFO для данного пакета и действия
  function is_info(i_action in varchar2) return boolean is
  begin
    return is_level(gc_info, i_action);
  end is_info;
  --Логируется ли WARN для данного пакета и действия
  function is_warn(i_action in varchar2) return boolean is
  begin
    return is_level(gc_warn, i_action);
  end is_warn;
  --Логируется ли ERROR для данного пакета и действия
  function is_error(i_action in varchar2) return boolean is
  begin
    return is_level(gc_error, i_action);
  end is_error;
  --Логируется ли TRACE для данного пакета и действия
  function is_trace(i_action in varchar2) return boolean is
  begin
    return is_level(gc_trace, i_action);
  end is_trace;
  --Логируется ли FATAL для данного пакета и действия
  function is_fatal(i_action in varchar2) return boolean is
  begin
    return is_level(gc_fatal, i_action);
  end is_fatal;

end tlog;
/
