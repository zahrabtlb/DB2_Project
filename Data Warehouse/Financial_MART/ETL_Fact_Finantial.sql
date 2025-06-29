USE Clinic_DW
GO


--daily load facts 


--=========================================================================================================================================
--========================================================== Financial MART ===============================================================
--=========================================================================================================================================

--=========================================================================================================================================
--===================================================== Fact Transaction Services =========================================================
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.ETL_Fact_Transaction_Service
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @proc_name VARCHAR(100) = 'ETL_Fact_Transaction_Service';
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @max_date_in_fact INT;
    DECLARE @start_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;
    
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[Fact_Transaction_Service]', 0);
    
    -- Check if the fact table is empty. If so, log and exit.
    IF NOT EXISTS (SELECT 1 FROM [dbo].[Fact_Transaction_Service])
    BEGIN
        INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES (@proc_name, GETDATE(), 'ERROR: Fact table is empty. Run FirstLoad procedure first.', '[Fact_Transaction_Service]', -1);
        RETURN;
    END;

    -- Determine the date range for incremental load
    SELECT @max_date_in_fact = MAX(DateKey) FROM [Clinic_DW].[dbo].[Fact_Transaction_Service];
    
    SELECT @start_date = DATEADD(DAY, 1, CAST(CONVERT(VARCHAR(8), @max_date_in_fact, 112) AS DATE));
    SELECT @end_date = MAX(CAST(IssueDateTime AS DATE)) 
    FROM [Clinic_SA].[dbo].[InvoiceItems_sa] WHERE IssueDateTime IS NOT NULL;

    -- Loop through each new day if there is data to process
    IF @end_date >= @start_date
    WHILE @current_date <= @end_date
    BEGIN
        --TRUNCATE TABLE [dbo].[tmp_Fact_Transaction_Service];
        
        -- Insert data for the current day into the temp table
        INSERT INTO [dbo].[Fact_Transaction_Service]
        (
            PatientID, StaffID, ServiceSK, ServiceID, DateKey, TimeID, DepartmentSK, 
            DepartmentID, InsuranceCoID, Quantity, UnitCost, InsuranceCoverageAmount
        )
        SELECT
            v.PatientID,
            v.DoctorStaffID,
            ds.ServiceSK,
            vs.ServiceID,
            CAST(CONVERT(VARCHAR(8), v.VisitDateTime, 112) AS INT) AS DateKey,
			DATEPART(hour, vs.ExecutionDateTime) * 100 + DATEPART(minute, vs.ExecutionDateTime),
            dd.DepartmentSK,
            v.DepartmentID,
            pi.InsuranceCoID,
            ii.Quantity,
            ii.UnitPrice,
            ii.InsuranceCoverage
        FROM [Clinic_SA].[dbo].[InvoiceItems_sa] AS ii
        JOIN [Clinic_SA].[dbo].[VisitServices_sa] AS vs ON ii.VisitServiceID = vs.VisitServiceID 
			and vs.ExecutionDateTime >= CAST(@current_date AS DATETIME ) AND vs.ExecutionDateTime < CAST( (DATEADD(DAY,1,@current_date)) AS DATETIME)
        JOIN [Clinic_SA].[dbo].[Visits_sa] AS v ON vs.VisitID = v.VisitID
			and v.VisitDateTime >= CAST(@current_date AS DATETIME ) AND v.VisitDateTime < CAST( (DATEADD(DAY,1,@current_date)) AS DATETIME)
        LEFT JOIN [Clinic_SA].[dbo].[PatientInsurances_sa] AS pi ON ii.PatientInsuranceID = pi.PatientInsuranceID
        -- SCD Type 2 Join for Service
        JOIN [Clinic_DW].[dbo].[Dim_Service] AS ds ON vs.ServiceID = ds.ServiceID 
			AND  vs.ExecutionDateTime >= cast(ds.StartDate as datetime) AND  vs.ExecutionDateTime < cast(ISNULL(ds.EndDate, '9999-12-31') as datetime)
        JOIN [Clinic_DW].[dbo].[Dim_Department] AS dd ON v.DepartmentID = dd.DepartmentID 
			AND  vs.ExecutionDateTime >= CAST(dd.StartDate AS DATETIME ) AND  vs.ExecutionDateTime < CAST(ISNULL(dd.EndDate, '9999-12-31') AS DATETIME )
        WHERE ii.IssueDateTime >= CAST(@current_date AS DATETIME ) AND ii.IssueDateTime < CAST( (DATEADD(DAY,1,@current_date)) AS DATETIME)


        
        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    -- Log the final results
    SELECT @total_rows_inserted = COUNT(*) FROM [dbo].[Fact_Transaction_Service] f JOIN [Clinic_DW].[dbo].[Dim_Date] d ON f.DateKey = d.TimeKey WHERE d.FullDateAlternateKey >= @start_date;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[dbo].[Fact_Transaction_Service]', @total_rows_inserted);
    
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);

    SET NOCOUNT OFF;
END
GO


--=========================================================================================================================================
--========================================================= Fact DAILY Service ============================================================
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.ETL_Fact_Daily_Service_Incremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Fact_Daily_Service_Incremental';
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @max_date_in_fact INT;
    DECLARE @start_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Fact_Daily_Service', 0);

    SELECT @max_date_in_fact = MAX(DateKey) FROM [dbo].[Fact_Daily_Service];
    IF @max_date_in_fact IS NULL
    BEGIN
        INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES (@proc_name, GETDATE(), 'ERROR: Fact table is empty. Run FirstLoad.', 'Fact_Daily_Service', -1);
        RETURN;
    END;

    SELECT @start_date = DATEADD(DAY, 1, CAST(CONVERT(VARCHAR(8), @max_date_in_fact, 112) AS DATE));
    SELECT @end_date = MAX(d.FullDateAlternateKey) FROM [Clinic_DW].[dbo].[Fact_Transaction_Service] f JOIN [Clinic_DW].[dbo].[Dim_Date] d ON f.DateKey = d.TimeKey;

    IF @end_date >= @start_date
    BEGIN
        SET @current_date = @start_date;
        WHILE @current_date <= @end_date
        BEGIN
            INSERT INTO [dbo].[Fact_Daily_Service]
            (
                ServiceSK, ServiceID, DateKey, TotalServiceCount, TotalRevenue, InsurancePaidAmount, UniquePatientsCount, UniqueDoctorsCount, AvgUnitCost, DayCount
            )
            SELECT
                s.ServiceSK, s.ServiceID, d.TimeKey,
                ISNULL(f.TotalServiceCount, 0), ISNULL(f.TotalRevenue, 0), ISNULL(f.InsurancePaidAmount, 0),
                ISNULL(f.UniquePatientsCount, 0), ISNULL(f.UniqueDoctorsCount, 0),
                -- Calculate new weighted average cost
                CASE 
                    WHEN (ISNULL(prev_day.TotalServiceCount, 0) + ISNULL(f.TotalServiceCount, 0)) = 0 THEN ISNULL(prev_day.AvgUnitCost, s.BaseCost)
                    ELSE (ISNULL(prev_day.AvgUnitCost, s.BaseCost) * ISNULL(prev_day.TotalServiceCount, 0) + ISNULL(f.TotalRevenue, 0)) / (ISNULL(prev_day.TotalServiceCount, 0) + ISNULL(f.TotalServiceCount, 0))
                END,
                ISNULL(prev_day.DayCount, 0) + 1
            FROM
                [Clinic_DW].[dbo].[Dim_Service] s
            CROSS JOIN
                (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = @current_date) d
            LEFT JOIN
                (
                    SELECT ServiceSK, COUNT(ServiceID) AS TotalServiceCount, SUM(UnitCost * Quantity) AS TotalRevenue, SUM(InsuranceCoverageAmount) AS InsurancePaidAmount, COUNT(DISTINCT PatientID) AS UniquePatientsCount, COUNT(DISTINCT StaffID) AS UniqueDoctorsCount
                    FROM [Clinic_DW].[dbo].[Fact_Transaction_Service]
                    WHERE DateKey = (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = @current_date)
                    GROUP BY ServiceSK
                ) f ON s.ServiceSK = f.ServiceSK
            LEFT JOIN
                [dbo].[Fact_Daily_Service] prev_day ON s.ServiceSK = prev_day.ServiceSK
                AND prev_day.DateKey = (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = DATEADD(DAY, -1, @current_date))
            WHERE s.CurrentFlag = 1;

            SET @current_date = DATEADD(DAY, 1, @current_date);
        END;
    END;

    SELECT @total_rows_inserted = COUNT(*) FROM [dbo].[Fact_Daily_Service] f JOIN [Clinic_DW].[dbo].[Dim_Date] d ON f.DateKey = d.TimeKey WHERE d.FullDateAlternateKey >= @start_date;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', 'Fact_Daily_Service', @total_rows_inserted);

END;
GO


--=========================================================================================================================================
--========================================================== Fact ACC Service =============================================================
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.Incrementally_Fill_FactAccService
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc_name VARCHAR(100) = 'Incrementally_Fill_FactAccService';
	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
	VALUES (@proc_name, GETDATE(), 'procedure started', '[Fact_Acc_Service]', 0);

    IF NOT EXISTS (SELECT 1 FROM dbo.Fact_Daily_Service)
        RETURN;

    DECLARE @current_day DATE,
            @end_date    DATE;

    SELECT  @current_day = CONVERT(date, CAST(MIN(DateKey) AS char(8)), 112),
            @end_date    = CONVERT(date, CAST(MAX(DateKey) AS char(8)), 112)
    FROM    dbo.Fact_Daily_Service;

    /* resume from bookmark if present */
    SELECT @current_day =
           ISNULL( (SELECT DATEADD(DAY,1,MAX([Date])) FROM dbo.TimeAccFactService),
                   @current_day );

    IF @current_day IS NULL OR @current_day > @end_date
        RETURN;

    /*──────────────────────────────────────────────────────────
      B. Seed temp1 with *existing* accumulator converted to sums
    ──────────────────────────────────────────────────────────*/
    TRUNCATE TABLE temp1_FactAccService;

    INSERT INTO temp1_FactAccService
          (ServiceSK, ServiceID,
           TotalServiceCount, TotalRevenue, InsurancePaidAmount,
           UniquePatientsSum, UniqueDoctorsSum,
           AvgUnitCostSum, DayCount)
    SELECT
        ServiceSK,
        ServiceID,
        TotalServiceCount,
        TotalRevenue,
        InsurancePaidAmount,
        CAST(UniquePatientsAvg * DayCount AS BIGINT)  AS UniquePatientsSum,
        CAST(UniqueDoctorsAvg * DayCount AS BIGINT)   AS UniqueDoctorsSum,
        AvgUnitCost * DayCount                       AS AvgUnitCostSum,
        DayCount
    FROM dbo.Fact_ACC_Service;   /* yesterday’s running totals */


    /*──────────────────────────────────────────────────────────
      C. Day-by-day loop
    ──────────────────────────────────────────────────────────*/
    DECLARE @dateKeyInt INT;

    WHILE @current_day <= @end_date
    BEGIN
        /* C-1  copy yesterday → temp2 */
        TRUNCATE TABLE temp2_FactAccService;
        INSERT INTO temp2_FactAccService SELECT * FROM temp1_FactAccService;

        /* C-2  today’s delta → temp3 */
        TRUNCATE TABLE Temp.temp3_FactAccService;

        SET @dateKeyInt = CAST(CONVERT(char(8), @current_day, 112) AS INT);

        INSERT INTO temp3_FactAccService
              (ServiceSK, ServiceID,
               TotalServiceCount, TotalRevenue, InsurancePaidAmount,
               UniquePatientsSum, UniqueDoctorsSum,
               AvgUnitCostSum, DayCount)
        SELECT
            ServiceSK,
            ServiceID,
            SUM(TotalServiceCount),
            SUM(TotalRevenue),
            SUM(InsurancePaidAmount),
            SUM(UniquePatientsCount),
            SUM(UniqueDoctorsCount),
            SUM(AvgUnitCost),      -- keep summation for later averaging
            SUM(DayCount)          -- always 1
        FROM dbo.Fact_Daily_Service
        WHERE DateKey = @dateKeyInt
        GROUP BY ServiceSK, ServiceID;

        /* C-3  merge t2 + t3 → new temp1 (running sums) */
        TRUNCATE TABLE temp1_FactAccService;

        INSERT INTO temp1_FactAccService
              (ServiceSK, ServiceID,
               TotalServiceCount, TotalRevenue, InsurancePaidAmount,
               UniquePatientsSum, UniqueDoctorsSum,
               AvgUnitCostSum, DayCount)
        SELECT
            COALESCE(t3.ServiceSK, t2.ServiceSK)                        AS ServiceSK,
            COALESCE(t3.ServiceID, t2.ServiceID)                        AS ServiceID,
            ISNULL(t2.TotalServiceCount,0)   + ISNULL(t3.TotalServiceCount,0),
            ISNULL(t2.TotalRevenue,0)        + ISNULL(t3.TotalRevenue,0),
            ISNULL(t2.InsurancePaidAmount,0) + ISNULL(t3.InsurancePaidAmount,0),
            ISNULL(t2.UniquePatientsSum,0)   + ISNULL(t3.UniquePatientsSum,0),
            ISNULL(t2.UniqueDoctorsSum,0)    + ISNULL(t3.UniqueDoctorsSum,0),
            ISNULL(t2.AvgUnitCostSum,0)      + ISNULL(t3.AvgUnitCostSum,0),
            ISNULL(t3.DayCount,0)
        FROM temp2_FactAccService t2
        FULL OUTER JOIN temp3_FactAccService t3
               ON t2.ServiceSK = t3.ServiceSK
              AND t2.ServiceID = t3.ServiceID;

        /* advance */
        SET @current_day = DATEADD(DAY,1,@current_day);
    END   /* WHILE */


    /*──────────────────────────────────────────────────────────
      D. Build staging rows (derive averages once)
    ──────────────────────────────────────────────────────────*/
    TRUNCATE TABLE Staging_FactAccService;

    INSERT INTO Staging_FactAccService
          (ServiceSK, ServiceID,
           TotalServiceCount, TotalRevenue, InsurancePaidAmount,
           UniquePatientsAvg, UniqueDoctorsAvg,
           UnitCost, AvgUnitCost, DayCount)
    SELECT
        ServiceSK,
        ServiceID,
        TotalServiceCount,
        TotalRevenue,
        InsurancePaidAmount,
        CAST(ROUND(CAST(UniquePatientsSum AS float) / NULLIF(DayCount,0),0) AS INT),
        CAST(ROUND(CAST(UniqueDoctorsSum  AS float) / NULLIF(DayCount,0),0) AS INT),
        CAST(TotalRevenue / NULLIF(TotalServiceCount,0) AS NUMERIC(18,2)),
        CAST(AvgUnitCostSum / NULLIF(DayCount,0)        AS NUMERIC(18,2)),
        DayCount
    FROM temp1_FactAccService;


    /*──────────────────────────────────────────────────────────
      E. Swap into real fact  (PICK NEW row, no re-add)
    ──────────────────────────────────────────────────────────*/
    BEGIN TRAN;

        TRUNCATE TABLE dbo.Fact_ACC_Service;
        INSERT INTO dbo.Fact_ACC_Service
        SELECT * FROM staging_FactAccService;

        /* advance bookmark */
        TRUNCATE TABLE dbo.TimeAccFactService;
        INSERT INTO dbo.TimeAccFactService([Date]) VALUES (@end_date);

    COMMIT TRAN;
	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
	VALUES (@proc_name, GETDATE(), 'procedure finished', '[Fact_Acc_Service]', 0);
END;
GO


--=========================================================================================================================================
--==================================================== Factless Patient Insurance =========================================================
--=========================================================================================================================================


CREATE OR ALTER PROCEDURE dbo.ETL_Factless_Patient_Insurance
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Factless_Patient_Insurance';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[dbo].[Factless_Patient_Insurance]', 0);

    IF (NOT EXISTS (SELECT 1 FROM [dbo].[Factless_Patient_Insurance]) AND EXISTS (SELECT 1 FROM [dbo].[tmp_Factless_Patient_Insurance]))
	begin
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'Error: Factless_Patient_Insurance is empty and temp is full', '[dbo].[Factless_Patient_Insurance]', 0);
		return;
	end
    

    TRUNCATE TABLE [dbo].[tmp_Factless_Patient_Insurance];

    -- Copy existing data from the main fact table to the temp table
    INSERT INTO [dbo].[tmp_Factless_Patient_Insurance] (PatientID, InsuranceCoID)
    SELECT PatientID, InsuranceCoID
    FROM [dbo].[Factless_Patient_Insurance];
        
    -- Insert only new records from the source into the temp table
    INSERT INTO [dbo].[tmp_Factless_Patient_Insurance] (PatientID, InsuranceCoID)
    SELECT sa.PatientID, sa.InsuranceCoID
    FROM [Clinic_SA].[dbo].[PatientInsurances_sa] sa
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[tmp_Factless_Patient_Insurance] tmp 
            WHERE tmp.PatientID = sa.PatientID and tmp.InsuranceCoID = sa.InsuranceCoID);
        
    TRUNCATE TABLE [dbo].[Factless_Patient_Insurance];
    INSERT INTO [dbo].[Factless_Patient_Insurance] (PatientID, InsuranceCoID)
    SELECT PatientID, InsuranceCoID
    FROM [dbo].[tmp_Factless_Patient_Insurance];
    

    SELECT @number_of_rows = COUNT(*) FROM [dbo].[Factless_Patient_Insurance];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Daily Procedure finished', '[dbo].[Factless_Patient_Insurance]', @number_of_rows);

    SET NOCOUNT OFF;
END
GO

