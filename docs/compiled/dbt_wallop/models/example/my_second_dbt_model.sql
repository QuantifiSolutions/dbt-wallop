-- Use the `ref` function to select from other models

select *
from `dbt-wallop-dev-1`.`example`.`my_first_dbt_model`
where id = 1