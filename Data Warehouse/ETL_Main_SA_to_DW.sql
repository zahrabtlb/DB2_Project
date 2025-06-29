USE Clinic_DW;
GO

-- =========================================================================
-- MASTER PROCEDURE 1: ETL_Master_FirstLoad_DW
-- Description: Executes all 'FirstLoad' procedures for dimensions in the
--              correct dependency order. This should be run only once.
-- =========================================================================
CREATE OR ALTER PROCEDURE dbo.ETL_Main_FirstLoad_DW
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Main_FirstLoad_DW';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Main DW FirstLoad Process Started', 'All Dimensions', 0);

    -- Step 1: Load independent dimensions first
    EXEC dbo.FirstLoad_ETùL_Dim_Date;
    EXEC dbo.FirstLoad_ETL_Dim_Time;
    EXEC dbo.FirstLoad_ETL_Dim_Insurance;
    EXEC dbo.FirstLoad_ETL_Dim_VisitType;
    EXEC dbo.FirstLoad_ETL_Dim_Disease;
    EXEC dbo.FirstLoad_ETL_Dim_Service;      -- SCD Type 2
    EXEC dbo.FirstLoad_ETL_Dim_Department;   -- SCD Type 2
    
    -- Step 2: Load dependent dimensions
    EXEC dbo.FirstLoad_ETL_Dim_DoctorStaff;  -- SCD Type 3, depends on Department
    EXEC dbo.FirstLoad_ETL_Dim_Patient;      -- SCD Type 1 (Full Reload)

    -- =========================================================================
    
	-- Step 3: Patient mart facts
	EXEC dbo.FirstFill_Fact_Transaction_Visit
	EXEC dbo.Fill_Fact_Daily_Patient_FirstLoad
	EXEC dbo.First_Fill_FactAccPatient
	EXEC dbo.FirstLoad_ETL_Factless_Patient_MedicalHistory

	-- Step 4: finantial mart facts
	EXEC dbo.FirstLoad_ETL_Fact_Transaction_Service
	EXEC dbo.FirstLoad_ETL_Fact_Daily_Service
	EXEC dbo.First_Fill_FactAccService
	EXEC dbo.FirstLoad_ETL_Factless_Patient_Insurance
		-- ...
    -- =========================================================================

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Master DW FirstLoad Process Finished Successfully', 'All Dimensions', 0);

    SET NOCOUNT OFF;
END;
GO

-- =========================================================================
-- MASTER PROCEDURE 2: ETL_Master_DailyLoad_DW
-- Description: Executes all daily load procedures for dimensions in the
--              correct dependency order. This should be run on a schedule.
-- =========================================================================
CREATE OR ALTER PROCEDURE dbo.ETL_Main_DailyLoad_DW
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Main_DailyLoad_DW';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Master DW Daily Process Started', 'All Dimensions', 0);

    -- Step 1: Execute daily ETL for each dimension
    EXEC dbo.ETL_Dim_Insurance;
    EXEC dbo.ETL_Dim_VisitType;
    EXEC dbo.ETL_Dim_Disease;
    EXEC dbo.ETL_Dim_Service;          -- SCD Type 2
    EXEC dbo.ETL_Dim_Department;       -- SCD Type 2
    EXEC dbo.ETL_Dim_DoctorStaff;      -- SCD Type 3
    EXEC dbo.ETL_Dim_Patient;          -- SCD Type 1 (Full Reload)

    -- =========================================================================
	-- Step 3: Patient mart facts
	EXEC dbo.Fill_Fact_Daily_Patient_Incremental
	EXEC dbo.Fill_Fact_Transaction_Visit_Incremental
	EXEC dbo.Fill_Fact_Daily_Patient_Incremental
	EXEC dbo.ETL_Factless_Patient_MedicalHistory

	-- Step 4: finantial mart facts
	EXEC dbo.ETL_Fact_Transaction_Service
	EXEC dbo.ETL_Fact_Daily_Service_Incremental
	--EXEC dbo.Incrementally_Fill_FactAccService
	EXEC dbo.ETL_Factless_Patient_Insurance
    -- =========================================================================

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Master DW Daily Process Finished Successfully', 'All Dimensions', 0);

    SET NOCOUNT OFF;
END;
GO
