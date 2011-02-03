CREATE TABLE states (
  state_id   INTEGER,
  state_name VARCHAR(30)
);


CREATE TABLE cities (
  city_id    INTEGER,
  city_name  VARCHAR(30),
  population INTEGER,
  state_id   INTEGER
);


CREATE TABLE stores (
  store_id       INTEGER,
  city_id        INTEGER,
  property_costs INTEGER
);


CREATE TABLE employees (
  emp_id     INTEGER,
  last_name  VARCHAR(30),
  first_name VARCHAR(30),

  home_loc_id INTEGER,
  work_loc_id INTEGER,

  salary     INTEGER,
  manager_id INTEGER
);


QUIT;
