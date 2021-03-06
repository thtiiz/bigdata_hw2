DROP TABLE IF EXISTS coffee_chain;

CREATE TABLE coffee_chain (
  area_code int,
  date_ DATE,
  territory varchar(255),
  territory_size varchar(255),
  product varchar(255),
  product_line varchar(255),
  product_type varchar(255),
  state varchar(255),
  type varchar(255),
  budget_cogs int,
  budget_margin int,
  budget_profit int,
  budget_sales int,
  cogs int,
  inventory int,
  margin int,
  marketing int,
  profit int,
  sales int,
  total_expenses int
);