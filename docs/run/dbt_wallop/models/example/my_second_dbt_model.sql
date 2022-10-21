

  create or replace view `dbt-wallop-dev-1`.`example`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `dbt-wallop-dev-1`.`example`.`my_first_dbt_model`
where id = 1;

