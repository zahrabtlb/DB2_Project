USE Clinic_DW
GO


--first load facts 

--=========================================================================================================================================
--========================================================= Patient MART ================================================================== 
--=========================================================================================================================================

--=========================================================================================================================================
--=================================================== FACT Transaction Visits ============================================================= 
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE FirstFill_Fact_Transaction_Visit
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES ('FirstFill_Fact_Transaction_Visit', GETDATE(), 'Start run procedure', 'Fact_Transaction_Visit', 0);

    DECLARE @maxDateSource DATE;
    DECLARE @currentDate DATE;
    DECLARE @rowCount INT;

    SELECT @maxDateSource = CAST(MAX(VisitDateTime) AS DATE) FROM Clinic_SA.dbo.Visits_sa;
    SELECT @currentDate =  CAST(MIN(VisitDateTime) AS DATE) FROM Clinic_SA.dbo.Visits_sa;
	IF @currentDate IS NULL 
	BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES ('FirstFill_Fact_Transaction_Visit', GETDATE(), 'Fail: no data in SA', 'Fact_Transaction_Visit', 0);
		RETURN;
	END

	truncate table Fact_Transaction_Visit
    
    WHILE @currentDate <= @maxDateSource
    BEGIN

       -- TRUNCATE TABLE Clinic_DW.dbo.visits_trans_temp;

    
        INSERT INTO Clinic_DW.dbo.Fact_Transaction_Visit (
            PatientID,
            StaffID,
            DateKey,
            TimeKey,
            VisitTypeID,
            DepartmentSK,
            DepartmentID,
            DiseaseID,
            VisitServiceQuantity,
            AdmissionServiceQuantity,
            UniqueServiceCount,
            UniqueDrugCount,
            MedicationDurationAvg,
            TotalCost
        )
        SELECT
            v.PatientID,
            v.DoctorStaffID AS StaffID,
          
            CAST(CONVERT(VARCHAR(8), v.VisitDateTime, 112) AS INT) AS DateKey,
          
            CAST(REPLACE(CONVERT(VARCHAR(8), v.VisitDateTime, 108), ':', '') AS INT)/100 AS TimeKey,
            v.VisitTypeID AS VisitTypeID,
            d.DepartmentSK,
            v.DepartmentID,
            ISNULL(pmh.DiseaseID, -1) AS DiseaseID,
            ISNULL(vs.VisitServiceQuantity, 0) AS VisitServiceQuantity,
            ISNULL(vs.AdmissionServiceQuantity, 0) AS AdmissionServiceQuantity,
            ISNULL(vs.UniqueServiceCount, 0) AS UniqueServiceCount,
            ISNULL(pd.UniqueDrugCount, 0) AS UniqueDrugCount,
            ISNULL(pd.MedicationDurationAvg, 0) AS MedicationDurationAvg,
            ISNULL(vs.TotalCost, 0) AS TotalCost
        FROM Clinic_SA.dbo.Visits_sa v
        LEFT JOIN Clinic_SA.dbo.PatientMedicalHistory_sa pmh ON v.DiagnosisHistoryID = pmh.HistoryID
        LEFT JOIN Clinic_DW.dbo.Dim_Department d ON v.DepartmentID = d.DepartmentID and d.CurrentFlag = 1
        LEFT JOIN (
            SELECT
                VisitID,
                SUM(CASE WHEN IsAdmissionService = 0 THEN Quantity ELSE 0 END) AS VisitServiceQuantity,
                SUM(CASE WHEN IsAdmissionService = 1 THEN Quantity ELSE 0 END) AS AdmissionServiceQuantity,
                COUNT(DISTINCT s.ServiceID) AS UniqueServiceCount,
                SUM(Quantity * BaseCost) AS TotalCost
            FROM Clinic_SA.dbo.VisitServices_sa vs
            INNER JOIN Clinic_DW.dbo.Dim_Service s ON vs.ServiceID = s.ServiceID
			where  vs.ExecutionDateTime >= CAST(@currentDate AS DATETIME ) AND vs.ExecutionDateTime < CAST( (DATEADD(DAY,1,@currentDate)) AS DATETIME)
            GROUP BY VisitID
        ) vs ON v.VisitID = vs.VisitID
        LEFT JOIN (
            SELECT
                pi.VisitID,
                COUNT(DISTINCT pi.DrugID) AS UniqueDrugCount,
                AVG(pi.Duration_Days) AS MedicationDurationAvg
            FROM Clinic_SA.dbo.PrescriptionItems_sa pi
			where  pi.IssueDateTime >= CAST(@currentDate AS DATETIME ) AND  pi.IssueDateTime < CAST( (DATEADD(DAY,1,@currentDate)) AS DATETIME)
            GROUP BY pi.VisitID
        ) pd ON v.VisitID = pd.VisitID
		WHERE v.VisitDateTime >= CAST(@currentDate AS DATETIME ) AND v.VisitDateTime < CAST( (DATEADD(DAY,1,@currentDate)) AS DATETIME)

        SET @rowCount = @@ROWCOUNT;

		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES ('Fill_Fact_Transaction_Visit_Incremental', GETDATE(),
                CONCAT('Inserted rows for date ', CONVERT(VARCHAR, @currentDate, 23)),
                'Fact_Transaction_Visit',
                @rowCount);

        SET @currentDate = DATEADD(DAY, 1, @currentDate);
    END


	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES ('FirstFill_Fact_Transaction_Visit', GETDATE(), 'End run procedure', 'Fact_Transaction_Visit', 0);
END;
GO


--=========================================================================================================================================
--====================================================== FACT DAILY Patient =============================================================== 
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE Fill_Fact_Daily_Patient_FirstLoad
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES ('Fill_Fact_Daily_Patient_FirstLoad', GETDATE(),'procedure started',  'Fact_Daily_Patient', 0);

    TRUNCATE TABLE Clinic_DW.dbo.Fact_Daily_Patient;

    DECLARE @minDate DATE;
    DECLARE @maxDate DATE;
    DECLARE @currentDate DATE;
    DECLARE @rowCount INT;

    SELECT @minDate = MIN(CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
           @maxDate = MAX(CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE))
    FROM Clinic_DW.dbo.Fact_Transaction_Visit;

    IF @minDate IS NULL OR @maxDate IS NULL
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES ('Fill_Fact_Daily_Patient_FirstLoad', GETDATE(),'error: Fact_Transaction_Visit is empty', 'Fact_Daily_Patient', 0);
        RETURN;
    END

    SET @currentDate = @minDate;

    WHILE @currentDate <= @maxDate
    BEGIN
        INSERT INTO [Clinic_DW].[dbo].[Fact_Daily_Patient]
        (
            PatientID, DateKey, VisitCount, VisitServiceQuantity, AdmissionServiceQuantity, TotalCost, LastVisitDays, NoVisitDays
        )
        SELECT
            p.PatientID,
            d.TimeKey AS DateKey,
            ISNULL(v.VisitCount, 0),
            ISNULL(v.VisitServiceQuantity, 0),
            ISNULL(v.AdmissionServiceQuantity, 0),
            ISNULL(v.TotalCost, 0),
            CASE WHEN v.VisitCount > 0 THEN 0 ELSE ISNULL(prev_day.LastVisitDays, 0) + 1 END,
            CASE WHEN v.VisitCount > 0 THEN ISNULL(prev_day.NoVisitDays, 0) ELSE ISNULL(prev_day.NoVisitDays, 0) + 1 END
        FROM
            -- 1. Create the scaffold: Every patient for the current day
            [Clinic_DW].[dbo].[Dim_Patient] p
        CROSS JOIN
            (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = @currentDate) d
        -- 2. Left Join to today's transactions
        LEFT JOIN
            (
                SELECT PatientID, DateKey, COUNT(*) AS VisitCount, SUM(VisitServiceQuantity) AS VisitServiceQuantity, SUM(AdmissionServiceQuantity) AS AdmissionServiceQuantity, SUM(TotalCost) AS TotalCost
                FROM [Clinic_DW].[dbo].[Fact_Transaction_Visit]
                WHERE DateKey = (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = @currentDate)
                GROUP BY PatientID, DateKey
            ) v ON p.PatientID = v.PatientID
        -- 3. Left Join to yesterday's snapshot to get previous state
        LEFT JOIN
            [Clinic_DW].[dbo].[Fact_Daily_Patient] prev_day ON p.PatientID = prev_day.PatientID
            AND prev_day.DateKey = (SELECT TimeKey FROM [Clinic_DW].[dbo].[Dim_Date] WHERE FullDateAlternateKey = DATEADD(DAY, -1, @currentDate));

        SET @currentDate = DATEADD(DAY, 1, @currentDate);
    END;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES ('Fill_Fact_Daily_Patient_FirstLoad', GETDATE(),'procedure finished', 'Fact_Daily_Patient', @rowCount);
END;
go

--=========================================================================================================================================
--======================================================= FACT ACC Patient ================================================================ 
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.First_Fill_FactAccPatient
AS
BEGIN
    SET NOCOUNT ON;

	DECLARE @proc_name VARCHAR(100) = 'Incrementally_Fill_FactAccPatient';
	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
	VALUES (@proc_name, GETDATE(), 'Procedure Started', '[Fact_Daily_Patient]', 0);

    DECLARE @current_date DATE,
            @end_date    DATE;

    SELECT  @current_date = CONVERT(DATE, CAST(MIN(DateKey) AS CHAR(8)), 112),
            @end_date    = CONVERT(DATE, CAST(MAX(DateKey) AS CHAR(8)), 112)
    FROM    dbo.Fact_Daily_Patient;

    IF @current_date IS NULL        -- no data at all
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'no data in fact_tran_Patient', '[Fact_Daily_Patient]', 0);
		RETURN;
    END


    /*---------------------------------------------------------
      3.  Main day-by-day loop
    ---------------------------------------------------------*/
    DECLARE @dateKeyInt  INT,
            @rowsToday   INT,
            @msg         NVARCHAR(200);

    WHILE @current_date <= @end_date
    BEGIN
        /* 3-A  current dateKey in yyyymmdd integer form */
        SET @dateKeyInt = CAST(CONVERT(CHAR(8), @current_date, 112) AS INT);

        /* 3-B  aggregate daily rows for that day */
        ;WITH daily AS (
            SELECT  PatientID,
                    SUM(VisitCount) AS VisitCnt,
                    SUM(VisitServiceQuantity) AS VisitSvcQty,
                    SUM(AdmissionServiceQuantity)AS AdmSvcQty,
                    SUM(TotalCost)AS TotCost,
                    MAX(LastVisitDays) AS LastVisitDays,
                    MAX(NoVisitDays) AS NoVisitDays
            FROM    dbo.Fact_Daily_Patient
            WHERE   DateKey = @dateKeyInt
            GROUP BY PatientID
        )
        /* 3-C  upsert into the accumulator */
        MERGE dbo.Fact_ACC_Patient AS tgt
        USING daily AS src
              ON tgt.PatientID = src.PatientID
        WHEN MATCHED THEN
            UPDATE SET
                tgt.TotalVisitCount          = tgt.TotalVisitCount  + src.VisitCnt,
                tgt.VisitServiceQuantity     = tgt.VisitServiceQuantity + src.VisitSvcQty,
                tgt.AdmissionServiceQuantity = tgt.AdmissionServiceQuantity + src.AdmSvcQty,
                tgt.TotalCost                = tgt.TotalCost + src.TotCost,
                tgt.LastVisitDays            = src.LastVisitDays,
                tgt.NoVisitDays              = src.NoVisitDays
        WHEN NOT MATCHED THEN
            INSERT ( PatientID,
                     TotalVisitCount, VisitServiceQuantity, AdmissionServiceQuantity,
                     TotalCost, LastVisitDays, NoVisitDays )
            VALUES ( src.PatientID,
                     src.VisitCnt, src.VisitSvcQty, src.AdmSvcQty,
                     src.TotCost,  src.LastVisitDays, src.NoVisitDays );

        SELECT @rowsToday = COUNT(*) FROM dbo.Fact_Daily_Patient WHERE DateKey = @dateKeyInt;

		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'data loaded', '[Fact_Daily_Patient]', @rowsToday);

        /* 3-E  next day */
        SET @current_date = DATEADD(DAY, 1, @current_date);
    END 


    /*---------------------------------------------------------
      4.  Bookmark the last day loaded
    ---------------------------------------------------------*/
    TRUNCATE TABLE dbo.TimeAccFactPatient;
    INSERT INTO dbo.TimeAccFactPatient ([Date]) VALUES (@end_date);


    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
	VALUES (@proc_name, GETDATE(), 'Procedure finished', '[Fact_Daily_Patient]', 0);
END;
GO


--=======================================================================================================================================
--==================================================== Factless Patient History =========================================================
--=======================================================================================================================================

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Factless_Patient_MedicalHistory
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'FirstLoad_ETL_Factless_Patient_MedicalHistory';
    
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[dbo].[Factless_Patient_MedicalHistory]', 0);

    TRUNCATE TABLE [dbo].[Factless_Patient_MedicalHistory];

    -- Insert all data from the source, joining to Dim_Date to get the DiagnosisDateKey
    INSERT INTO [dbo].[Factless_Patient_MedicalHistory] (PatientID, DiseaseID, DiagnosisDateKey)
    SELECT sa.PatientID, sa.DiseaseID, d.TimeKey
    FROM
        [Clinic_SA].[dbo].[PatientMedicalHistory_sa] sa
    LEFT JOIN
        [Clinic_DW].[dbo].[Dim_Date] d ON sa.DiagnosisDate = d.FullDateAlternateKey;

    SELECT @number_of_rows = COUNT(*) FROM [dbo].[Factless_Patient_MedicalHistory];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', '[dbo].[Factless_Patient_MedicalHistory]', @number_of_rows);
    
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);

    SET NOCOUNT OFF;
END
GO

