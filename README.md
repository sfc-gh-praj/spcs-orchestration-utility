# spcs-orchestration-workflow-utility

This solution implements a SPCS job service workflow using task DAG (Directed Acyclic Graph) in Snowflake for conatinar jobs fan-out and fan-in scenario using SQL and Python stored procedures. It allows for the creation and management of compute pools, execution of containerized jobs, and logging of task statuses.


## Components

1. Compute Pools
2. Logging Tables
3. User-Defined Table Function (UDTF)
4. Stored Procedures
   - ExecuteJobService
   - create_job_tasks
5. Task DAG Creation
6. Utility Procedure (drop_job_tasks)

## Setup Instructions

1. Run the SQL scripts to create the necessary compute pools, tables, and functions.
2. Upload the `jobconfig.json` file to the JOBS stage.
3. Execute the `create_job_tasks` procedure to set up the task DAG.

## Detailed Procedure Descriptions

### ExecuteJobService Procedure

This stored procedure is responsible for executing a containerized job as a SPCS service job. Here's what it does:

1. Accepts parameters:
   - `service_name`: Name of the service job to be created
   - `pool_name`: Name of the compute pool where the service job will run
   - `table_name`: Name of the table where results will be stored. Used by the container
   - `retry_count`: Number of retry attempts if the service job fails

2. Creates and executes a Snowflake job service using the provided parameters.

3. Implements a retry mechanism:
   - If the job fails, it will retry up to the specified `retry_count`.
   - Each retry is logged with its attempt number.

4. Logs job status and errors:
   - Inserts job execution details into the `jobs_run_stats` table.
   - If the job fails, it captures error logs using `SYSTEM$GET_SERVICE_LOGS`.

5. Uses Snowflake's task runtime information to capture details about the current task execution context.

6. Returns the final job status ('DONE' or 'FAILED').

### create_job_tasks Procedure

This procedure is responsible for creating the task DAG based on a configuration file. Here's what it does:

1. Accepts a `file_name` parameter, which is the path to the `jobconfig.json` file.

2. Creates a root task named 'root_task' that runs every 59 minutes.

3. Reads the `jobconfig.json` file and processes each task configuration:
   - Creates a Snowflake task for each job specified in the config.
   - Sets up task dependencies based on the 'after_task_name' specified in the config.
   - Each task is set to call the `ExecuteJobService` procedure with the appropriate parameters.

4. Creates a finalizer task named 'GET_GRAPH_STATS':
   - This task runs after the root task completes.
   - It captures the execution stats of all tasks in the DAG.
   - Inserts these stats into the `task_logging_stats` table.

5. Resumes the 'GET_GRAPH_STATS' task and enables all dependent tasks of the root task.

### drop_job_tasks Procedure

This utility procedure is used to clean up the task DAG. Here's what it does:

1. Suspends the root task.

2. Queries the task dependency information to get all tasks related to 'root_task'.

3. Iterates through all dependent tasks and drops them.

4. Drops the 'GET_GRAPH_STATS' task.

5. Returns 'Done' upon completion.

## Usage

### Creating Compute Pools

```sql
CREATE COMPUTE POOL pr_std_pool_xs
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS;

CREATE COMPUTE POOL PR_STD_POOL_S
  MIN_NODES = 1
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_S;
```

### Create Python Stored Procedures

Create the  `ExecuteJobService` and `create_job_tasks` procedure which is in the notebook file. ExecuteJobService is used to run containerized jobs. It's typically called by the task DAG.

```sql
CALL ExecuteJobService('<service_name>', '<pool_name>', '<table_name>', <retry_count>);
```

### Creating the Task DAG

Use the `create_job_tasks` procedure to set up the task DAG based on the `jobconfig.json` file and which internally calls ExecuteJobService :

```sql
CALL create_job_tasks(build_scoped_file_url(@jobs, 'jobconfig.json'));
```

### Monitoring

- Check job run status: `SELECT * FROM jobs_run_stats ORDER BY created_date DESC;`
- View task logging status: `SELECT * FROM task_logging_stats ORDER BY CAST(QUERY_START_TIME AS DATETIME) DESC;`

### Cleanup

To drop all tasks associated with the root task:

```sql
CALL drop_job_tasks();
```

## Configuration

The `jobconfig.json` file should contain an array of task configurations. Example:

```json
[
  {
    "task_name": "t_myjob_1",
    "compute_pool_name": "PR_STD_POOL_XS",
    "job_name": "myjob_1",
    "table_name": "results_1",
    "retry_count": 0,
    "after_task_name": "root_task"
  }
]
```

## Notes

- The root task is scheduled to run every 59 minutes.
- A finalizer task (GET_GRAPH_STATS) tracks the status of all tasks and logs it into the `task_logging_stats` table.
- Ensure that the necessary permissions are granted for compute pool usage and task execution.

## Troubleshooting

- If a job fails, check the `jobs_run_stats` table for error messages.
- Use the `SYSTEM$GET_SERVICE_LOGS` function to retrieve detailed logs for failed service jobs.

For more detailed information on each component, refer to the inline comments in the SQL and Python code.
