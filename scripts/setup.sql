use role accountadmin;

create role autopilot_role;
grant create integration
  on account
  to role autopilot_role;

create user autopilot_user
  -- change the value for password in the line below
  password = '<password>'
  login_name = 'autopilot_user'
  display_name = 'autopilot_user' 
  -- update the values for first/last name in he lines below
  first_name = '<first name>'
  last_name = '<last name>'
  email = '<email address>'
  default_role = 'autopilot_role'
  default_warehouse = 'autopilot_wh'
  default_namespace = 'autopilot_db'
  must_change_password = false;
grant role autopilot_role
  to user autopilot_user;

create database autopilot_db;
grant usage on database autopilot_db
  to role autopilot_role;

create schema demo;
grant ownership
  on schema autopilot_db.demo
  to role autopilot_role;

create warehouse autopilot_wh
  with warehouse_size = 'medium';
grant modify,monitor,usage,operate
  on warehouse autopilot_wh
  to role autopilot_role;
