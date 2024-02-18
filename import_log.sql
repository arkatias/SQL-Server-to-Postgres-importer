drop  table if exists import.pxf_load_log;
create table import.pxf_load_log
(
    id         serial
        primary key,
    table_name text not null,
    load_date  timestamp,
    last_ts    timestamp
)
    distributed by (id);

create or replace function import_log(address text, import_type text
)
    returns void as
$$
begin

    if import_type not in ('delta', 'full') then
        raise exception 'Unsupported import type.';
    end if;

    /* Добавляем таблицу в лог, если она ещё не добавлена */
    execute ('
	insert into import.pxf_load_log (table_name)
    select ''' || address || '''
    where not exists (select * from import.pxf_load_log where table_name = ''' || address || ''');');

    /* Записываем последнее загруженное значение */
    if import_type = 'delta' then
        execute ('
	update import.pxf_load_log
    set load_date = current_timestamp,
	    last_ts = (select max(rowstartdate)::timestamp from delta)
    where table_name = ''' || address || ''';
	');
    elsif import_type = 'full' then
        execute ('
	update import.pxf_load_log
    set load_date = current_timestamp,
	    last_ts = (select max(rowstartdate)::timestamp from ' || address || ')
    where table_name = ''' || address || ''';
	');
    end if;
end
$$ language plpgsql;