Импортер из SQL Server в PostgreSQL
Этот проект представляет собой набор инструментов для эффективной передачи данных из SQL Server в PostgreSQL, используя возможности PXF и system-versioned temporal tables для оптимизации обработки данных. 

Возможности
Инкрементная загрузка: Поддержка инкрементной загрузки данных, передающей только новые или измененные данные с момента последнего импорта.
Полная загрузка данных: Обеспечивает полную передачу данных, идеально подходит для начальной миграции данных или полного обновления.
Автоматизированное логирование: Включает подробное логирование каждой операции импорта данных, помогая в мониторинге и устранении неполадок.
Обработка ошибок: Надежные механизмы обработки ошибок для обеспечения надежности и консистентности передачи данных.
Настройка импорта: Позволяет настроить конфигурацию для различных требований к импорту данных.
Начало работы
Требования
Установленная и настроенная база данных PostgreSQL или Greenplum.
Установленный и настроенный PXF для PostgreSQL/Greenplum.
SQL Server с системно-версионированными временными таблицами.
Установка
Клонируйте репозиторий на локальную машину или сервер, где планируется выполнить процесс импорта:

bash
Copy code
git clone https://github.com/arkatias/SQL-Server-to-Postgres-importer.git
Перейдите в склонированный каталог:

bash
Copy code
cd SQL-Server-to-Postgres-importer
Настройка
Создание конфигурационной таблицы: В схеме import создайте таблицу конфигурации с названием вида server_database_schema_table, отражающую структуру данных источника в SQL Server, включая технические поля rowstartdate и rowenddate с типом timestamp (тип timestamp важен, так как с другим типом не сработает pushdown предиката в SQL Server).

Создание целевой таблицы: В схеме public создайте целевую таблицу (public.server_database_schema_table), структура которой должна соответствовать конфигурационной таблице, адаптируя типы данных к требующимся. Этот шаг нужен для нашей специфической проблемы, в целом можно переписать и оставить создание только одной таблицы.

Настройка параметров импорта: Определите параметры импорта, включая имена сервера, базы данных, схемы, таблиц и учетные данные.

Использование
Выполните функцию load_delta_from_pxf с указанными параметрами для начала процесса импорта:

sql
Copy code
PERFORM load_delta_from_pxf('ВашСервер', 'ВашаБазаДанных', 'ВашаСхема', 'ВашаТаблица', 'ВашПользователь', 'ВашПароль', true/false);
Замените ВашСервер, ВашаБазаДанных, ВашаСхема, ВашаТаблица, ВашПользователь и ВашПароль на фактические данныеваших сервера SQL Server.
Последний параметр true/false определяет тип загрузки: true для полной загрузки, false для загрузки дельты.
Мониторинг и Логи
Проверьте таблицу import.pxf_load_log для просмотра логов и подробностей каждой операции импорта.
