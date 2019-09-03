
create table if not exists ab_tests
     ( test_name text primary key
     , active_fg text
     , description text
     , config_file text not null)
