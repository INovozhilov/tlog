create table TLOG_FILES
(
  file_id          number not null,
  s_directory_name varchar2(30 char) not null,
  s_file_base      varchar2(200 char) not null,
  n_retention_days number default 45 not null
);
comment on table TLOG_FILES
  is 'Описание файлов для логирования в ФС сервера. Имя файла формируется в виде retention_days_date_shema_name_fileBase.log';
comment on column TLOG_FILES.s_directory_name
  is 'Директория для файла из all_directories';
comment on column TLOG_FILES.s_file_base
  is 'Основная часть имени файла';
comment on column TLOG_FILES.n_retention_days
  is 'Количество дней, в течении которых будут хранится логи в архиве';
alter table TLOG_FILES
  add constraint PK_TLOG_FILES primary key (FILE_ID);
alter table TLOG_FILES
  add constraint CHK_TLOG_FILES_UNIQ_DIR_FILE unique (S_DIRECTORY_NAME, S_FILE_BASE)5;
alter table TLOG_FILES
  add constraint CHK_TLOG_FILES_FILE_BASE
  check (regexp_like(s_file_base,'^[a-z0-9_.]+$'));
alter table TLOG_FILES
  add constraint CHK_TLOG_FILES_FILE_ID
  check (file_id >= 0);
alter table TLOG_FILES
  add constraint CHK_TLOG_FILES_RETENTION_DAYS
  check (n_retention_days > 0);


create table TLOG_TABLES
(
  table_id         number not null,
  s_table_name     varchar2(30 char) not null,
  n_retention_days number default 45 not null
);
comment on table TLOG_TABLES
  is 'Описание таблиц для логирования';
comment on column TLOG_TABLES.s_table_name
  is 'Название таблицы';
comment on column TLOG_TABLES.n_retention_days
  is 'Количество дней, в течении которых будут хранится логи в таблице';
create unique index UIX_TLOG_TABLES_TNAME on TLOG_TABLES (S_TABLE_NAME);
alter table TLOG_TABLES
  add constraint PK_TLOG_TABLES primary key (TABLE_ID);
alter table TLOG_TABLES
  add constraint CHK_TLOG_TABLES_RETENTION_DAYS
  check (n_retention_days > 0);
alter table TLOG_TABLES
  add constraint CHK_TLOG_TABLES_TABLE_ID
  check (table_id >= 0);
alter table TLOG_TABLES
  add constraint CHK_TLOG_TABLES_TAB_NAME
  check (regexp_like(s_table_name,'^log_[a-z0-9_.]+$'));
  
  
  
create table TLOG_MODULES
(
  module_id              number not null,
  s_module_name          varchar2(100 char) not null,
  s_program_name         varchar2(30 char) not null,
  s_action               varchar2(30 char),
  s_owner                varchar2(30 char),
  file_id                number,
  table_id               number,
  n_file_logging_level   number default 400 not null,
  n_table_logging_level  number default 400 not null,
  n_output_logging_level number default 400 not null
);
comment on table TLOG_MODULES
  is '"Модули" объединяющие программы, уровень логирования и шаблон файла. Модуль по-умолчанию (module_id = 0) задает настройки логирования для всех программных единиц, для которых не заданы собственные настройки логирования';
comment on column TLOG_MODULES.s_module_name
  is 'Наименование модуля';
comment on column TLOG_MODULES.s_program_name
  is 'Имя пакета для которого настраивается логирование UPPERCASE';
comment on column TLOG_MODULES.s_action
  is 'Действие для которого настраивается логирование. Если пусто, то применяется ко всем действиям пакета UPPERCASE';
comment on column TLOG_MODULES.s_owner
  is 'Схема-Владелец пакета. Если пусто, то применяется ко всем схемам с одноименным пакетом UPPERCASE';
comment on column TLOG_MODULES.file_id
  is 'Ссылка на настройки файла';
comment on column TLOG_MODULES.table_id
  is 'Ссылка на настройки таблицы';
comment on column TLOG_MODULES.n_file_logging_level
  is 'Уровень логирования в файл чем больше значение - тем подробнее логи (ERROR=200, DEBUG=500)';
comment on column TLOG_MODULES.n_table_logging_level
  is 'Уровень логирования в таблицу чем больше значение - тем подробнее логи (ERROR=200, DEBUG=500)';
comment on column TLOG_MODULES.n_output_logging_level
  is 'Уровень логирования в dbms_output чем больше значение - тем подробнее логи (ERROR=200, DEBUG=500)';
create unique index I1_TLOG_MODULES_NAME_ACT_OWN on TLOG_MODULES (S_PROGRAM_NAME, S_ACTION, S_OWNER);
alter table TLOG_MODULES
  add constraint PK_TLOG_MODULES primary key (MODULE_ID));
alter table TLOG_MODULES
  add constraint FK1_TLOG_MODULES_FILE_ID foreign key (FILE_ID)
  references TLOG_FILES (FILE_ID);
alter table TLOG_MODULES
  add constraint FK1_TLOG_MODULES_TABLE_ID foreign key (TABLE_ID)
  references TLOG_TABLES (TABLE_ID);
alter table TLOG_MODULES
  add constraint CHK_TLOG_TAB_FILE_FILL
  check (file_id||table_id is not null);
alter table TLOG_MODULES
  add constraint CHK_TLOG_UPPERCASE_REQ
  check (regexp_like(s_program_name||s_action||s_owner,'^[A-Z0-9_.]+$'));
