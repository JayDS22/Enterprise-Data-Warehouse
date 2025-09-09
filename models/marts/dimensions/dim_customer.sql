{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['customer_id', 'effective_from'],
    cluster_by=['customer_id', 'effective_from'],
    tags=['customer', 'master_data', 'scd_type2'],
    post_hook=[
        "{{ log_scd_stats() }}",
        "{{ validate_scd_integrity('dim_customer') }}"
    ]
) }}

-- Generated SCD Type 2 dimension: dim_customer
-- Description: Customer master data with demographic and geographic information

{{ scd_type2('staging_customers', 'customer_id', 'last_modified_date') }}
