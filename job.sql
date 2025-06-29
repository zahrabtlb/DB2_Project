/*
===================================================================================
-- Script Description
-- This script creates a SQL Server Agent Job to automate the entire ETL process
-- on a daily basis.
-- The job consists of two steps that are executed in sequence.
===================================================================================
*/

USE msdb;
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0


DECLARE @jobId BINARY(16)
DECLARE @jobName NVARCHAR(128) = N'ETL_Daily_Clinic_DW_Process'
DECLARE @dbName_SA NVARCHAR(128) = N'Clinic_SA'
DECLARE @dbName_DW NVARCHAR(128) = N'Clinic_DW'
DECLARE @owner_login_name NVARCHAR(128) = SUSER_SNAME()

-- Delete the job if it already exists
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = @jobName)
EXEC msdb.dbo.sp_delete_job @job_name = @jobName, @delete_unused_schedule = 1

-- Add the main job
EXEC @ReturnCode =  msdb.dbo.sp_add_job
    @job_name = @jobName,
    @enabled = 1,
    @notify_level_eventlog = 0,
    @description = N'This job executes the entire ETL process from source to staging and then to the data warehouse.',
    @category_name = N'[Uncategorized (Local)]',
    @owner_login_name = @owner_login_name,
    @job_id = @jobId OUTPUT

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Add the first job step (Run Staging Area ETL)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
    @job_id = @jobId,
    @step_name = N'1 - Run ETL for Staging Area (SA)',
    @step_id = 1,
    @cmdexec_success_code = 0,
    @on_success_action = 3, -- On success, go to the next step
    @on_fail_action = 2,    -- On failure, quit the job reporting failure
    @retry_attempts = 0,
    @subsystem = N'TSQL',
    @command = N'EXEC [Clinic_SA].[dbo].[ETL_Master_Run_SA];',
    @database_name = @dbName_SA

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Add the second job step (Run Data Warehouse ETL)
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
    @job_id = @jobId,
    @step_name = N'2 - Run ETL for Data Warehouse (DW)',
    @step_id = 2,
    @cmdexec_success_code = 0,
    @on_success_action = 1, -- On success, quit the job reporting success
    @on_fail_action = 2,    -- On failure, quit the job reporting failure
    @retry_attempts = 0,
    @subsystem = N'TSQL',
    @command = N'EXEC [Clinic_DW].[dbo].[ETL_Master_DailyLoad_DW];',
    @database_name = @dbName_DW

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Set the starting step for the job
EXEC @ReturnCode = msdb.dbo.sp_update_job
    @job_id = @jobId,
    @start_step_id = 1

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Create the daily schedule
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule
    @job_id = @jobId,
    @name = N'Daily_0200_AM_Schedule',
    @enabled = 1,
    @freq_type = 4, -- Daily
    @freq_interval = 1, -- Every 1 day
    @freq_subday_type = 1, -- At a specific time
    @freq_subday_interval = 0,
    @freq_relative_interval = 0,
    @freq_recurrence_factor = 0,
    @active_start_date = 20240101,
    @active_end_date = 99991231,
    @active_start_time = 20000, -- 02:00:00 (2 AM)
    @active_end_time = 235959

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback


-- Attach the job to the current server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver
    @job_id = @jobId,
    @server_name = N'(local)'

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

COMMIT TRANSACTION
GOTO EndSave

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION

EndSave:
    PRINT 'Job "ETL_Daily_Clinic_DW_Process" created successfully.'
GO
