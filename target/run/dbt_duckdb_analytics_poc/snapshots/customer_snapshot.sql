
      update "analytics_dev"."snapshots"."customers_snapshot" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "customers_snapshot__dbt_tmp20250414201330057664" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and DBT_INTERNAL_TARGET.dbt_valid_to is null;

    insert into "analytics_dev"."snapshots"."customers_snapshot" ("customer_id", "first_name", "last_name", "email", "registration_date", "country", "full_name", "days_since_registration", "updated_at", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."customer_id",DBT_INTERNAL_SOURCE."first_name",DBT_INTERNAL_SOURCE."last_name",DBT_INTERNAL_SOURCE."email",DBT_INTERNAL_SOURCE."registration_date",DBT_INTERNAL_SOURCE."country",DBT_INTERNAL_SOURCE."full_name",DBT_INTERNAL_SOURCE."days_since_registration",DBT_INTERNAL_SOURCE."updated_at",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "customers_snapshot__dbt_tmp20250414201330057664" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  