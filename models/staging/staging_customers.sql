{{ config(
    materialized='view',
    tags=['staging', 'customer']
) }}

-- Staging model for customer data
-- Standardizes and cleanses raw customer data from source systems

with source_data as (
    select
        -- Natural key
        customer_id,
        
        -- Customer attributes with cleansing
        trim(upper(customer_name)) as customer_name,
        lower(trim(email_address)) as email_address,
        regexp_replace(phone_number, '[^0-9+()-]', '') as phone_number,
        birth_date,
        upper(trim(gender)) as gender,
        
        -- Address standardization
        trim(address_line1) as address_line1,
        trim(address_line2) as address_line2,
        trim(upper(city)) as city,
        trim(upper(state_province)) as state_province,
        upper(trim(postal_code)) as postal_code,
        upper(trim(country_code)) as country_code,
        
        -- Business attributes
        customer_since_date,
        upper(trim(customer_status)) as customer_status,
        
        -- Audit columns
        created_date,
        last_modified_date,
        
        -- Data quality indicators
        case 
            when customer_name is null or trim(customer_name) = '' then 'MISSING_NAME'
            when email_address is null or not regexp_like(email_address, '^[^@]+@[^@]+\\.[^@]+$') then 'INVALID_EMAIL'
            when country_code is null then 'MISSING_COUNTRY'
            else 'VALID'
        end as data_quality_flag
        
    from {{ source('raw_data', 'customers') }}
    
    -- Filter out test/dummy records
    where customer_id is not null
      and customer_id not like 'TEST_%'
      and customer_name not ilike '%test%'
),

enhanced_data as (
    select 
        *,
        
        -- Derived attributes
        case 
            when birth_date is not null then 
                datediff('year', birth_date, current_date())
            else null
        end as customer_age,
        
        case 
            when customer_since_date is not null then
                datediff('year', customer_since_date, current_date())
            else null
        end as customer_tenure_years,
        
        -- Age grouping
        case 
            when datediff('year', birth_date, current_date()) < 25 then '18-24'
            when datediff('year', birth_date, current_date()) < 35 then '25-34'
            when datediff('year', birth_date, current_date()) < 45 then '35-44'
            when datediff('year', birth_date, current_date()) < 55 then '45-54'
            when datediff('year', birth_date, current_date()) < 65 then '55-64'
            when datediff('year', birth_date, current_date()) >= 65 then '65+'
            else 'Unknown'
        end as age_group,
        
        -- Full address construction
        concat(
            coalesce(address_line1, ''),
            case when address_line2 is not null and trim(address_line2) != '' 
                then ', ' || address_line2 
                else '' 
            end,
            case when city is not null then ', ' || city else '' end,
            case when state_province is not null then ', ' || state_province else '' end,
            case when postal_code is not null then ' ' || postal_code else '' end
        ) as full_address,
        
        -- Customer lifecycle status
        case 
            when customer_status = 'ACTIVE' and customer_since_date >= current_date - 90 then 'NEW'
            when customer_status = 'ACTIVE' and customer_since_date < current_date - 365 then 'LOYAL'
            when customer_status = 'ACTIVE' then 'ESTABLISHED'
            when customer_status = 'INACTIVE' then 'CHURNED'
            else 'UNKNOWN'
        end as customer_lifecycle_stage
        
    from source_data
)

select * from enhanced_data
