{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'order_line_id', 'transaction_date'],
    cluster_by=['transaction_date', 'customer_key', 'product_key'],
    tags=['sales', 'revenue', 'daily', 'critical'],
    pre_hook="call system$log_info('Starting fact_sales_daily processing')",
    post_hook=[
        "{{ log_fact_stats() }}",
        "{{ validate_fact_table() }}",
        "call system$log_info('Completed fact_sales_daily processing')"
    ]
) }}

-- Generated fact table: fact_sales_daily
-- Description: Daily sales transactions and revenue metrics across all channels
-- Generated on: {{ run_started_at }}

with source_data as (
    select
        -- Business keys
        order_id,
        order_line_id,
        
        -- Dimension foreign keys
        customer_id as customer_key,
        product_id as product_key,
        transaction_date as date_key,
        sales_rep_id as sales_rep_key,
        store_id as store_key,
        
        -- Measures with data type casting
        cast(quantity_sold as integer) as quantity,
        cast(gross_amount_usd as decimal(15,2)) as gross_revenue,
        cast(discount_amount_usd as decimal(15,2)) as discount_amount,
        cast(tax_amount_usd as decimal(15,2)) as tax_amount,
        
        -- Audit and metadata columns
        last_modified_date as source_updated_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at,
        '{{ invocation_id }}' as batch_id,
        'daily' as grain_level
        
    from {{ ref('staging_sales_transactions') }}
    
    where transaction_date is not null
      and order_id is not null
      and order_line_id is not null
    
    {% if is_incremental() %}
        and last_modified_date > (
            select coalesce(max(source_updated_at), '1900-01-01'::timestamp) 
            from {{ this }}
        )
    {% endif %}
),

data_quality_layer as (
    select 
        *,
        -- Data quality scoring
        case 
            when order_id is not null and order_line_id is not null and quantity is not null and gross_revenue is not null
            then 'VALID'
            else 'INVALID'
        end as data_quality_status,
        
        -- Completeness indicators
        case when quantity is not null then 1 else 0 end as quantity_complete,
        case when gross_revenue is not null then 1 else 0 end as gross_revenue_complete,
        case when discount_amount is not null then 1 else 0 end as discount_amount_complete,
        case when tax_amount is not null then 1 else 0 end as tax_amount_complete,
        
        -- Row hash for change detection
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_line_id', 'quantity', 'gross_revenue', 'discount_amount', 'tax_amount']) }} as row_hash
        
    from source_data
),

business_logic_layer as (
    select 
        *,
        
        -- Derived measures
        gross_revenue - coalesce(discount_amount, 0) as net_revenue,
        case when quantity > 0 then gross_revenue / quantity else 0 end as average_unit_price,
        case when gross_revenue > 0 then discount_amount / gross_revenue else 0 end as discount_rate,
        
        -- Standard derived fields
        case 
            when quantity > 0 
            then gross_revenue / quantity
            else 0 
        end as average_unit_value,
        
        -- Data lineage
        '{{ this.schema }}.{{ this.table }}' as target_table,
        'staging_sales_transactions' as source_table_name
        
    from data_quality_layer
),

final as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_line_id']) }} as fact_sales_daily_key,
        
        -- All business data
        *,
        
        -- Performance metrics
        extract(epoch from current_timestamp() - source_updated_at) as data_age_seconds,
        
        -- Data freshness classification
        case 
            when extract(epoch from current_timestamp() - source_updated_at) <= 3600 then 'FRESH'
            when extract(epoch from current_timestamp() - source_updated_at) <= 86400 then 'ACCEPTABLE'
            else 'STALE'
        end as data_freshness_status
        
    from business_logic_layer
    where data_quality_status = 'VALID'
      and gross_revenue >= 0  -- Business rule: no negative revenue
      and quantity > 0        -- Business rule: must have positive quantity
)

select * from final
