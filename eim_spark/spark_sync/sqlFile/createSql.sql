create table load_file_list
(
    id                     serial
        primary key,
    load_file_name         varchar not null,
    drop_create_table_flag boolean not null,
    generate_tableddl_flag boolean not null,
    on_line                boolean not null
);



blackunionids
dictdatas
doctorsrecords
persons
physicianidrecords
physicians
retailpharmacists
sanofiusers
subscribeeventlogs
vaccinespecialists
wechataccounts
wechatuserbindings
wechatusers


INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('blackunionids',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('dictdatas',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('doctorsrecords',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('persons',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('physicianidrecords',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('physicians',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('retailpharmacists',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('sanofiusers',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('subscribeeventlogs',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('vaccinespecialists',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('wechataccounts',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('wechatuserbindings',false,false,true);
INSERT INTO app_upt.load_file_list (load_file_name,drop_create_table_flag,generate_tableddl_flag,on_line) VALUES ('wechatusers',false,false,true);
