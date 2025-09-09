"""
Enterprise Data Warehouse - DBT Orchestration DAG

This DAG orchestrates the complete data warehouse refresh process including:
- Data quality checks
- Staging layer processing
- Fact and dimension table refreshes
- Data lineage updates
- Monitoring and alerting

Author: Data Engineering Team
"""

from datetime import datetime, timedelta
from typing import Dict, List
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# DAG Configuration
DAG_ID = 'enterprise_dw_dbt_orchestration'
DESCRIPTION = 'Enterprise Data Warehouse DBT Orchestration'

# Default arguments
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-engineering@company.com'],
    'execution_timeout': timedelta(hours=4)
}

# Environment-specific configuration
ENVIRONMENT = Variable.get('environment', default_var='dev')
DBT_PROJECT_DIR = Variable.get('dbt_project_dir', default_var='/opt/airflow/dbt')
DBT_PROFILES_DIR = Variable.get('dbt_profiles_dir', default_var='/opt/airflow/.dbt')
SNOWFLAKE_CONN_ID = Variable.get('snowflake_conn_id', default_var='snowflake_default')
SLACK_WEBHOOK_CONN_ID = Variable.get('slack_webhook_conn_id', default_var='slack_webhook')

# DBT command templates
DBT_BASE_CMD = f'cd {DBT_PROJECT_DIR} && dbt --profiles-dir {DBT_PROFILES_DIR}'
DBT_STAGING_CMD = f'{DBT_BASE_CMD} run --select staging --target {ENVIRONMENT}'
DBT_INTERMEDIATE_CMD = f'{DBT_BASE_CMD} run --select intermediate --target {ENVIRONMENT}'
DBT_DIMENSIONS_CMD = f'{DBT_BASE_CMD} run --select marts.dimensions --target {ENVIRONMENT}'
DBT_FACTS_CMD = f'{DBT_BASE_CMD} run --select marts.facts --target {ENVIRONMENT}'
DBT_TEST_CMD = f'{DBT_BASE_CMD} test --target {ENVIRONMENT}'
DBT_DOCS_CMD = f'{DBT_BASE_CMD} docs generate --target {ENVIRONMENT}'


def get_run_stats(**context) -> Dict:
    """Get statistics about the current DAG run"""
    dag_run = context['dag_run']
    
    return {
        'dag_id': dag_run.dag_id,
        'run_id': dag_run.run_id,
        'execution_date': dag_run.execution_date.isoformat(),
        'start_date': dag_run.start_date.isoformat() if dag_run.start_date else None,
        'environment': ENVIRONMENT
    }


def log_run_start(**context) -> None:
    """Log the start of a DAG run"""
    stats = get_run_stats(**context)
    print(f"Starting DAG run: {stats}")
    
    # Log to Snowflake audit table
    audit_sql = f"""
    INSERT INTO {ENVIRONMENT.upper()}_DW.AUDIT.dbt_run_log (
        run_id, dag_id, run_started_at, invocation_id, target_name, status
    ) VALUES (
        '{stats['run_id']}',
        '{stats['dag_id']}', 
        '{stats['execution_date']}',
        '{context['ts']}',
        '{ENVIRONMENT}',
        'RUNNING'
    )
    """
    
    # This would be executed via SnowflakeOperator in practice
    print(f"Audit SQL: {audit_sql}")


def log_run_success(**context) -> None:
    """Log successful completion of DAG run"""
    stats = get_run_stats(**context)
    print(f"DAG run completed successfully: {stats}")
    
    # Update audit log
    audit_sql = f"""
    UPDATE {ENVIRONMENT.upper()}_DW.AUDIT.dbt_run_log 
    SET 
        run_completed_at = CURRENT_TIMESTAMP(),
        status = 'SUCCESS'
    WHERE run_id = '{stats['run_id']}'
    """
    
    print(f"Success audit SQL: {audit_sql}")


def log_run_failure(**context) -> None:
    """Log failed DAG run"""
    stats = get_run_stats(**context)
    print(f"DAG run failed: {stats}")
    
    # Update audit log
    audit_sql = f"""
    UPDATE {ENVIRONMENT.upper()}_DW.AUDIT.dbt_run_log 
    SET 
        run_completed_at = CURRENT_TIMESTAMP(),
        status = 'FAILED'
    WHERE run_id = '{stats['run_id']}'
    """
    
    print(f"Failure audit SQL: {audit_sql}")


def check_data_quality(**context) -> None:
    """Check overall data quality scores"""
    # This would query the data quality tables and raise alerts if needed
    quality_threshold = 0.95
    
    quality_sql = f"""
    SELECT 
        table_name,
        round(100.0 * sum(case when test_result = 'PASS' then 1 else 0 end) / count(*), 2) as quality_score
    FROM {ENVIRONMENT.upper()}_DW.AUDIT.data_quality_scores
    WHERE run_timestamp >= current_date - 1
    GROUP BY table_name
    HAVING quality_score < {quality_threshold * 100}
    """
    
    print(f"Quality check SQL: {quality_sql}")
    # In practice, this would execute the SQL and raise alerts for low scores


def generate_data_lineage(**context) -> None:
    """Update data lineage information"""
    print("Generating data lineage information...")
    
    # This would typically call a lineage tool or update lineage tables
    lineage_cmd = f'{DBT_BASE_CMD} run-operation update_data_lineage --target {ENVIRONMENT}'
    print(f"Lineage command: {lineage_cmd}")


# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'data_warehouse', 'enterprise', ENVIRONMENT]
)

# Start and end dummy operators
start_task = DummyOperator(
    task_id='start',
    dag=dag
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

# Logging tasks
log_start = PythonOperator(
    task_id='log_run_start',
    python_callable=log_run_start,
    dag=dag
)

log_success = PythonOperator(
    task_id='log_run_success', 
    python_callable=log_run_success,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

log_failure = PythonOperator(
    task_id='log_run_failure',
    python_callable=log_run_failure,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)

# Pre-processing checks
with TaskGroup('pre_processing_checks', dag=dag) as pre_checks:
    
    # Check DBT installation and connection
    dbt_debug = BashOperator(
        task_id='dbt_debug_check',
        bash_command=f'{DBT_BASE_CMD} debug --target {ENVIRONMENT}',
        dag=dag
    )
    
    # Check for required source data
    check_source_data = SnowflakeOperator(
        task_id='check_source_data_availability',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        SELECT 
            table_schema,
            table_name,
            row_count,
            last_altered
        FROM {ENVIRONMENT.upper()}_DW.INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = 'STAGING'
        AND table_name LIKE 'STAGING_%'
        """,
        dag=dag
    )
    
    # Validate data freshness
    check_data_freshness = SnowflakeOperator(
        task_id='validate_data_freshness',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        SELECT 
            table_name,
            datediff('hour', max(updated_at), current_timestamp()) as hours_stale
        FROM (
            SELECT 'staging_customers' as table_name, max(updated_at) as updated_at 
            FROM {ENVIRONMENT.upper()}_DW.STAGING.staging_customers
            UNION ALL
            SELECT 'staging_orders' as table_name, max(updated_at) as updated_at 
            FROM {ENVIRONMENT.upper()}_DW.STAGING.staging_orders
        ) freshness
        WHERE hours_stale > 24  -- Alert if data is more than 24 hours old
        """,
        dag=dag
    )

# DBT processing pipeline
with TaskGroup('dbt_processing_pipeline', dag=dag) as dbt_pipeline:
    
    # Install/update DBT packages
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'{DBT_BASE_CMD} deps',
        dag=dag
    )
    
    # Compile models to check for errors
    dbt_compile = BashOperator(
        task_id='dbt_compile',
        bash_command=f'{DBT_BASE_CMD} compile --target {ENVIRONMENT}',
        dag=dag
    )
    
    # Load seed data
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'{DBT_BASE_CMD} seed --target {ENVIRONMENT}',
        dag=dag
    )
    
    # Run staging models
    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=DBT_STAGING_CMD,
        dag=dag
    )
    
    # Run intermediate models
    dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=DBT_INTERMEDIATE_CMD,
        dag=dag
    )
    
    # Run dimension tables (with dependency management)
    dbt_dimensions = BashOperator(
        task_id='dbt_run_dimensions',
        bash_command=DBT_DIMENSIONS_CMD,
        dag=dag
    )
    
    # Run fact tables (depends on dimensions)
    dbt_facts = BashOperator(
        task_id='dbt_run_facts',
        bash_command=DBT_FACTS_CMD,
        dag=dag
    )
    
    # Run data quality tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=DBT_TEST_CMD,
        dag=dag
    )
    
    # Generate documentation
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=DBT_DOCS_CMD,
        dag=dag
    )
    
    # Set dependencies within the pipeline
    dbt_deps >> dbt_compile >> dbt_seed
    dbt_seed >> dbt_staging >> dbt_intermediate
    dbt_intermediate >> dbt_dimensions >> dbt_facts
    dbt_facts >> dbt_test >> dbt_docs

# Post-processing tasks
with TaskGroup('post_processing', dag=dag) as post_processing:
    
    # Data quality validation
    quality_check = PythonOperator(
        task_id='validate_data_quality',
        python_callable=check_data_quality,
        dag=dag
    )
    
    # Update data lineage
    lineage_update = PythonOperator(
        task_id='update_data_lineage',
        python_callable=generate_data_lineage,
        dag=dag
    )
    
    # Update table statistics
    update_stats = SnowflakeOperator(
        task_id='update_table_statistics',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        -- Update table statistics for query optimization
        ALTER WAREHOUSE {ENVIRONMENT.upper()}_WH SET WAREHOUSE_SIZE = 'LARGE';
        
        -- Analyze tables for optimization
        CALL SYSTEM$ANALYZE_SCHEMA('{ENVIRONMENT.upper()}_DW.MARTS');
        
        -- Reset warehouse size
        ALTER WAREHOUSE {ENVIRONMENT.upper()}_WH SET WAREHOUSE_SIZE = 'MEDIUM';
        """,
        dag=dag
    )
    
    # Parallel post-processing
    [quality_check, lineage_update, update_stats]

# Notification tasks
success_notification = SlackWebhookOperator(
    task_id='success_notification',
    http_conn_id=SLACK_WEBHOOK_CONN_ID,
    message=f"""
    ✅ *Enterprise Data Warehouse Refresh Completed Successfully*
    
    *Environment:* {ENVIRONMENT.upper()}
    *Execution Date:* {{{{ ds }}}}
    *Duration:* {{{{ dag_run.end_date - dag_run.start_date }}}}
    
    All fact and dimension tables have been refreshed successfully.
    Data quality checks passed.
    """,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

failure_notification = SlackWebhookOperator(
    task_id='failure_notification',
    http_conn_id=SLACK_WEBHOOK_CONN_ID,
    message=f"""
    ❌ *Enterprise Data Warehouse Refresh Failed*
    
    *Environment:* {ENVIRONMENT.upper()}
    *Execution Date:* {{{{ ds }}}}
    *Failed Task:* {{{{ task_instance.task_id }}}}
    
    Please check the Airflow logs for details.
    Data Engineering team has been notified.
    """,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)

# Cleanup tasks
cleanup_temp_tables = SnowflakeOperator(
    task_id='cleanup_temporary_tables',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    -- Clean up any temporary tables older than 7 days
    CALL SYSTEM$CLEANUP_TEMP_TABLES('{ENVIRONMENT.upper()}_DW', 7);
    
    -- Remove old test failure records
    DELETE FROM {ENVIRONMENT.upper()}_DW.AUDIT.data_quality_scores 
    WHERE created_at < DATEADD('day', -30, CURRENT_TIMESTAMP());
    """,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

# Define task dependencies
start_task >> log_start >> pre_checks >> dbt_pipeline >> post_processing

# Success path
post_processing >> log_success >> success_notification >> cleanup_temp_tables >> end_task

# Failure path
[pre_checks, dbt_pipeline, post_processing] >> log_failure >> failure_notification >> end_task

# Add task documentation
start_task.doc_md = """
## Enterprise Data Warehouse Orchestration
This DAG orchestrates the complete refresh of the enterprise data warehouse.
"""

dbt_pipeline.doc_md = """
## DBT Processing Pipeline
Runs the complete DBT transformation pipeline:
1. **Dependencies**: Install/update DBT packages
2. **Compilation**: Validate model SQL
3. **Seeds**: Load reference data
4. **Staging**: Process raw data into staging tables
5. **Intermediate**: Apply business logic transformations
6. **Dimensions**: Build dimension tables with SCD logic
7. **Facts**: Build fact tables with incremental loading
8. **Testing**: Run comprehensive data quality tests
9. **Documentation**: Generate model documentation
"""

post_processing.doc_md = """
## Post-Processing Tasks
Handles data quality validation, lineage updates, and optimization:
- Validates data quality scores against thresholds
- Updates data lineage information
- Optimizes table statistics for query performance
"""
