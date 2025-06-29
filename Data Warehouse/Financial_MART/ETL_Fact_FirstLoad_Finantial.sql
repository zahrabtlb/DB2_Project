USE Clinic_DW
GO


--first load facts 


--=========================================================================================================================================
--========================================================== Financial MART ===============================================================
--=========================================================================================================================================

--=========================================================================================================================================
--===================================================== Fact Transaction Services =========================================================
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Fact_Transaction_Service
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc_name VARCHAR(100) = 'FirstLoad_ETL_Fact_Transaction_Service';
    DECLARE @number_of_rows INT;
    DECLARE @start_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[Fact_Transaction_Service]', 0);

    TRUNCATE TABLE [Clinic_DW].[dbo].[Fact_Transaction_Service];

    -- Determine the full date range from the anchor table (Invoices)
    SELECT @start_date = MIN(CAST(IssueDateTime AS DATE)), @end_date = MAX(CAST(IssueDateTime AS DATE)) 
    FROM [Clinic_SA].[dbo].[InvoiceItems_sa] WHERE IssueDateTime IS NOT NULL;

    IF @start_date IS NULL
    BEGIN
        INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES (@proc_name, GETDATE(), 'No data found in source (Invoices_sa)', '[Fact_Transaction_Service]', 0);
        RETURN;
    END

    SET @current_date = @start_date;

    -- Loop through each day
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
        JOIN [Clinic_DW].[dbo].[Dim_Service] AS ds ON vs.ServiceID = ds.ServiceID AND ds.CurrentFlag = 1
        -- SCD Type 2 Join for Department
        JOIN [Clinic_DW].[dbo].[Dim_Department] AS dd ON v.DepartmentID = dd.DepartmentID AND dd.CurrentFlag = 1
       	WHERE ii.IssueDateTime >= CAST(@current_date AS DATETIME ) AND ii.IssueDateTime < CAST( (DATEADD(DAY,1,@current_date)) AS DATETIME)


        
        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    SELECT @number_of_rows = COUNT(*) FROM [dbo].[Fact_Transaction_Service];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', '[dbo].[Fact_Transaction_Service]', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);

    SET NOCOUNT OFF;
END
GO


--=========================================================================================================================================
--========================================================= Fact DAILY Service ============================================================
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Fact_Daily_Service
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @proc_name VARCHAR(100) = 'FirstLoad_ETL_Fact_Daily_Service';
    DECLARE @number_of_rows INT;
    DECLARE @start_date DATE, @end_date DATE, @current_date DATE;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Fact_Daily_Service', 0);

    TRUNCATE TABLE [dbo].[Fact_Daily_Service];

    -- Determine the full date range from the transaction fact table
    SELECT @start_date = MIN(d.FullDateAlternateKey), @end_date = MAX(d.FullDateAlternateKey)
    FROM [Clinic_DW].[dbo].[Fact_Transaction_Service] f
    JOIN [Clinic_DW].[dbo].[Dim_Date] d ON f.DateKey = d.TimeKey;

    IF @start_date IS NULL
    BEGIN
        INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES (@proc_name, GETDATE(), 'Error: Fact_Transaction_Service is empty. Cannot proceed.', 'Fact_Daily_Service', -1);
        RETURN;
    END

    SET @current_date = @start_date;

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [dbo].[Fact_Daily_Service]
            (ServiceSK, ServiceID, DateKey, TotalServiceCount, TotalRevenue, InsurancePaidAmount, UniquePatientsCount, UniqueDoctorsCount, AvgUnitCost, DayCount)
        SELECT
            s.ServiceSK,
            s.ServiceID,
            d.TimeKey,
            ISNULL(f.TotalServiceCount, 0),
            ISNULL(f.TotalRevenue, 0),
            ISNULL(f.InsurancePaidAmount, 0),
            ISNULL(f.UniquePatientsCount, 0),
            ISNULL(f.UniqueDoctorsCount, 0),
            -- If there are transactions today, calculate the new average cost, otherwise carry over the previous day's average
            CASE 
                WHEN ISNULL(f.TotalServiceCount, 0) > 0 THEN ISNULL(f.TotalRevenue, 0) / f.TotalServiceCount
                ELSE ISNULL(prev_day.AvgUnitCost, s.BaseCost)
            END,
            ISNULL(prev_day.DayCount, 0) + 1
        FROM
            [Clinic_DW].[dbo].[Dim_Service] s
        CROSS JOIN
            (SELECT TimeKey, FullDateAlternateKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = @current_date) d
        LEFT JOIN
            (
                SELECT
                    ServiceSK, DateKey,
                    COUNT(ServiceID) AS TotalServiceCount,
                    SUM(UnitCost * Quantity) AS TotalRevenue,
                    SUM(InsuranceCoverageAmount) AS InsurancePaidAmount,
                    COUNT(DISTINCT PatientID) AS UniquePatientsCount,
                    COUNT(DISTINCT StaffID) AS UniqueDoctorsCount
                FROM [Clinic_DW].[dbo].[Fact_Transaction_Service]
                WHERE DateKey = (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = @current_date)
                GROUP BY ServiceSK, DateKey
            ) f ON s.ServiceSK = f.ServiceSK
        LEFT JOIN
            [dbo].[Fact_Daily_Service] prev_day ON s.ServiceSK = prev_day.ServiceSK
            AND prev_day.DateKey = (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = DATEADD(DAY, -1, @current_date))
        WHERE s.CurrentFlag = 1;

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    SELECT @number_of_rows = COUNT(*) FROM [dbo].[Fact_Daily_Service];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'First Load completed', 'Fact_Daily_Service', @number_of_rows);
END;
GO


--=========================================================================================================================================
--========================================================== Fact ACC Service =============================================================
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.First_Fill_FactAccService
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc_name VARCHAR(100) = 'First_Fill_FactAccService';
	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[Fact_Acc_Service]', 0);


    /*----------------------------------------------------------
      C. Discover date range in daily fact
    ----------------------------------------------------------*/
    DECLARE @current_date DATE,
            @end_date    DATE;

    SELECT  @current_date = CONVERT(date, CAST(MIN(DateKey) AS char(8)), 112),
            @end_date     = CONVERT(date, CAST(MAX(DateKey) AS char(8)), 112)
    FROM    dbo.Fact_Daily_Service;

    IF @current_date IS NULL
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'no data in Fact_Daily_Service', '[Fact_Acc_Service]', 0);
        RETURN;
    END


    TRUNCATE TABLE temp1_FactAccService;
    DECLARE @rowsToday INT, @msg NVARCHAR(200), @dateKeyInt INT;

    WHILE @current_date <= @end_date
    BEGIN
        /* E-1  copy yesterday -> temp2 */
        TRUNCATE TABLE temp2_FactAccService;
        INSERT INTO temp2_FactAccService SELECT * FROM temp1_FactAccService;

        /* E-2  aggregate today -> temp3 */
        TRUNCATE TABLE temp3_FactAccService;

        SET @dateKeyInt = CAST(CONVERT(char(8), @current_date, 112) AS INT);

        INSERT INTO temp3_FactAccService
              (ServiceSK, ServiceID,
               TotalServiceCount, TotalRevenue, InsurancePaidAmount,
               UniquePatientsSum, UniqueDoctorsSum,
               AvgUnitCostSum, DayCount)
        SELECT
               ServiceSK,
               ServiceID,
               SUM(TotalServiceCount),           -- totals for the day
               SUM(TotalRevenue),
               SUM(InsurancePaidAmount),
               SUM(UniquePatientsCount),
               SUM(UniqueDoctorsCount),
               SUM(AvgUnitCost),                 -- keep a simple sum; we'll average later
               SUM(DayCount)                     -- typically 1
        FROM   dbo.Fact_Daily_Service WITH (TABLOCK)
        WHERE  DateKey = @dateKeyInt
        GROUP  BY ServiceSK, ServiceID;

        /* E-3  merge temp2 + temp3 -> temp1 (running sums) */
        TRUNCATE TABLE temp1_FactAccService;

        INSERT INTO temp1_FactAccService
              (ServiceSK, ServiceID,
               TotalServiceCount, TotalRevenue, InsurancePaidAmount,
               UniquePatientsSum, UniqueDoctorsSum,
               AvgUnitCostSum, DayCount)
        SELECT
            COALESCE(t3.ServiceSK, t2.ServiceSK)                    AS ServiceSK,
            COALESCE(t3.ServiceID, t2.ServiceID)                    AS ServiceID,

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


		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'loaded data', '[Fact_Acc_Service]', @rowsToday);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END  


    /*----------------------------------------------------------
      F. Build final rows in staging (derive averages once)
    ----------------------------------------------------------*/
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

        /* arithmetic averages across all days (rounded) */
        CAST(ROUND(CAST(UniquePatientsSum AS float) / NULLIF(DayCount,0), 0) AS INT) AS UniquePatientsAvg,
        CAST(ROUND(CAST(UniqueDoctorsSum  AS float) / NULLIF(DayCount,0), 0) AS INT) AS UniqueDoctorsAvg,

        /* overall unit cost and avg-of-avgs */
        CAST(TotalRevenue / NULLIF(TotalServiceCount,0) AS NUMERIC(13,2))         AS UnitCost,
        CAST(AvgUnitCostSum / NULLIF(DayCount,0)        AS NUMERIC(13,2))         AS AvgUnitCost,

        DayCount
    FROM temp1_FactAccService;


    /*----------------------------------------------------------
      G. Atomic swap & bookmark
    ----------------------------------------------------------*/
    DECLARE @maxDateKey INT = (SELECT MAX(DateKey) FROM dbo.Fact_Daily_Service);

    BEGIN TRAN;

        TRUNCATE TABLE dbo.Fact_ACC_Service;
        INSERT INTO dbo.Fact_ACC_Service
        SELECT * FROM Staging_FactAccService;

        TRUNCATE TABLE dbo.TimeAccFactService;
        INSERT INTO dbo.TimeAccFactService([Date])
        VALUES (CONVERT(date, CAST(@maxDateKey AS char(8)), 112));

    COMMIT TRAN;


    DECLARE @finalCnt INT = (SELECT COUNT(*) FROM dbo.Fact_ACC_Service)

	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
	VALUES (@proc_name, GETDATE(), 'procedure finihed', '[Fact_Acc_Service]', @finalCnt);
END;
GO


--=========================================================================================================================================
--==================================================== Factless Patient Insurance =========================================================
--=========================================================================================================================================


CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Factless_Patient_Insurance
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'FirstLoad_ETL_Factless_Patient_Insurance';
    
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[dbo].[Factless_Patient_Insurance]', 0);

    TRUNCATE TABLE [dbo].[Factless_Patient_Insurance];

    -- Insert all data from the source, joining to Dim_Date to get the ExpireDateKey
    INSERT INTO [dbo].[Factless_Patient_Insurance] (PatientID, InsuranceCoID)
    SELECT sa.PatientID,sa.InsuranceCoID
    FROM [Clinic_SA].[dbo].[PatientInsurances_sa] sa

    SELECT @number_of_rows = COUNT(*) FROM [dbo].[Factless_Patient_Insurance];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', '[dbo].[Factless_Patient_Insurance]', @number_of_rows);
    
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);

    SET NOCOUNT OFF;
END
GO

