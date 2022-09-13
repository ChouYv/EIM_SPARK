create table load_file_list
(
    id                     serial
        primary key,
    load_file_name         varchar not null,
    drop_create_table_flag boolean not null,
    generate_tableddl_flag boolean not null,
    on_line                boolean not null
);


