
      update "analytics_dev"."snapshots"."products_snapshot" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "products_snapshot__dbt_tmp20250414201330062528" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and DBT_INTERNAL_TARGET.dbt_valid_to is null;

    insert into "analytics_dev"."snapshots"."products_snapshot" ("product_id", "product_name", "category", "price", "stock_quantity", "supplier", "stock_level", "inventory_value", "updated_at", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."product_id",DBT_INTERNAL_SOURCE."product_name",DBT_INTERNAL_SOURCE."category",DBT_INTERNAL_SOURCE."price",DBT_INTERNAL_SOURCE."stock_quantity",DBT_INTERNAL_SOURCE."supplier",DBT_INTERNAL_SOURCE."stock_level",DBT_INTERNAL_SOURCE."inventory_value",DBT_INTERNAL_SOURCE."updated_at",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "products_snapshot__dbt_tmp20250414201330062528" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  