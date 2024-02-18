create or replace function load_delta_from_pxf(server_name text, db_name text, schema_name text, table_name text,
                                               username text,
                                               password text, full_load boolean default false)
    returns void as
$$
declare
    pxf_address_main text;
    pxf_address_hist text;
    gp_address       text;
    max_ts           timestamp;
begin

    /* Создаем внешние таблицы для вычитки */
    select*
    from create_temp_table_from_pxf(server_name, db_name, schema_name, table_name,
                                    username,
                                    password)
    into gp_address, pxf_address_main, pxf_address_hist;

    if full_load then
        /* Полная загрузка данных */
        execute ('truncate table public.' || gp_address || ';');
        execute ('insert into public.' || gp_address || ' select * from ' || pxf_address_main || ';');
        perform import_log(gp_address, 'full');
    else
        /* Получаем последний максимальный TS */
        execute ('select coalesce(last_ts, to_timestamp(0)) from import.pxf_load_log where table_name = ''' ||
                 gp_address || ''';') into max_ts;

        execute ('
    /* Набираем дельту изменений */
    drop table if exists delta;
    create temp table delta as
    with inserted as (
    select external_main.*
    from ' || pxf_address_main || ' external_main
    where external_main.rowstartdate >=''' || max_ts || '''),

    mb_deleted as (
	select external_hist.*
    from ' || pxf_address_hist || ' as external_hist
    where external_hist.rowenddate >= ''' || max_ts || ''')

    select *, 0 as is_del
    from inserted
    union all
    select *, 1 as is_del
    from mb_deleted d
    where 1 = 1
    and not exists (select * from inserted as i where i.id = d.id and i.rowstartdate >= d.rowenddate);

	/* Удаляем все пересечения по "primary key" */
    delete
    from only public.' || gp_address || ' trg
    where exists (select * from delta src where src.id = trg.id);

	/* Удаляем обработанные данные */
	delete from delta
    where is_del = 1;

	/* Дропаем атрибуты, которые не нужны в целевой */
	alter table delta drop column is_del;

    /* Полностью заливаем дельту */
    insert into public.' || gp_address || '
    select src.*
    from delta src;

	/* Логируем */
	select import_log(''' || gp_address || ''', ''delta'');
	');

    end if;
exception
    when others then
        /* Можно дописать необходимую обработку */
        raise exception 'Ошибка при обновлении данных: %', SQLERRM;
end
$$ language plpgsql;
