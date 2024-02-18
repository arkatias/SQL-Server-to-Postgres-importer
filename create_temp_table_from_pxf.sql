create or replace function create_temp_table_from_pxf(server_name text, db_name text, schema_name text, table_name text,
                                                      username text,
                                                      password text, with_hist boolean default true,
                                                      out gp_address text,
                                                      out pxf_address_main text,
                                                      out pxf_address_hist text)
    returns record as
$$
declare
    trg_address text := concat_ws('_', $1, $2, $3, $4);
    src_address text := concat_ws('.', $2, $3, $4);
begin

    select trg_address, concat('pxf_', trg_address), concat('pxf_', trg_address, '_history')
    into gp_address, pxf_address_main, pxf_address_hist;

    execute ('
    /* Создаем основную таблицу */
    drop external table if exists ' || pxf_address_main || ';
    create external temp table ' || pxf_address_main || ' (like import.' || trg_address || ')
    location (''pxf://' || src_address || '?PROFILE=Jdbc&JDBC_DRIVER=com.microsoft.sqlserver.jdbc.SQLServerDriver&DB_URL=jdbc:sqlserver://;
	serverName=' || server_name || ';databaseName=' || db_name || ';encrypt=true;trustServerCertificate=true;
	&USER=' || username || '&PASS=' || password || ''') on all
    format ''CUSTOM'' ( formatter = ''pxfwritable_import'' )
    encoding ''UTF8'';');

    if with_hist then
        execute ('
    /* Создаем основную таблицу */
    drop external table if exists ' || pxf_address_hist || ';
    create external temp table ' || pxf_address_hist || ' (like import.' || trg_address || ')
    location (''pxf://' || src_address || '_history?PROFILE=Jdbc&JDBC_DRIVER=com.microsoft.sqlserver.jdbc.SQLServerDriver&DB_URL=jdbc:sqlserver://;
	serverName=' || server_name || ';databaseName=' || db_name || ';encrypt=true;trustServerCertificate=true;
	&USER=' || username || '&PASS=' || password || ''') on all
    format ''CUSTOM'' ( formatter = ''pxfwritable_import'' )
    encoding ''UTF8'';');
    end if;
end
$$ language plpgsql;