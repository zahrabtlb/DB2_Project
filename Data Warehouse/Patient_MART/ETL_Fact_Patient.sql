USE Clinic_DW
GO


--daily load facts 

--=========================================================================================================================================
--========================================================= Patient MART ================================================================== 
--=========================================================================================================================================

--=========================================================================================================================================
--=================================================== FACT Transaction Visits ============================================================= 
--=========================================================================================================================================

GO
CREATE OR ALTER PROCEDURE Fill_Fact_Transaction_Visit_Incremental
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES ('Fill_Fact_Transaction_Visit_Incremental', GETDATE(), 'Start run procedure', 'Fact_Transaction_Visit', 0);

    DECLARE @maxDateInFact INT;
    DECLARE @maxDateSource DATE;
    DECLARE @currentDate DATE;
    DECLARE @rowCount INT;

    SELECT @maxDateInFact = MAX(DateKey) FROM Clinic_DW.dbo.Fact_Transaction_Visit;

    IF @maxDateInFact IS NULL
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES ('Fill_Fact_Transaction_Visit_Incremental', GETDATE(), 'No data in Fact. First load required.', 'Fact_Transaction_Visit', 0);
        RETURN;
    END

    SELECT @maxDateSource = CAST(MAX(VisitDateTime) AS DATE) FROM Clinic_SA.dbo.Visits_sa;

    SET @currentDate = DATEADD(DAY, 1, CAST(CONVERT(VARCHAR(8), @maxDateInFact, 112) AS DATE));

    IF @currentDate > @maxDateSource
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES ('Fill_Fact_Transaction_Visit_Incremental', GETDATE(), 'No new data to process.', 'Fact_Transaction_Visit', 0);
        RETURN;
    END

    WHILE @currentDate <= @maxDateSource
    BEGIN
    
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
        LEFT JOIN Clinic_DW.dbo.Dim_Department d ON v.DepartmentID = d.DepartmentID
			AND  v.VisitDateTime >= cast(d.StartDate as datetime) AND  v.VisitDateTime < cast(ISNULL(d.EndDate, '9999-12-31') as datetime)
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
    VALUES ('Fill_Fact_Transaction_Visit_Incremental', GETDATE(), 'End run procedure', 'Fact_Transaction_Visit', 0);
END;
GO

--=========================================================================================================================================
--====================================================== FACT DAILY Patient =============================================================== 
--=========================================================================================================================================

CREATE OR ALTER PROCEDURE Fill_Fact_Daily_Patient_Incremental
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES ('Fill_Fact_Daily_Patient_Incremental', GETDATE(), 'procedure started', 'Fact_Daily_Patient', 0);

    DECLARE @maxDateInFact INT;
    DECLARE @maxDateSource DATE;
    DECLARE @currentDate DATE;
    DECLARE @rowCount INT;

    SELECT @maxDateInFact = MAX(DateKey) FROM Clinic_DW.dbo.Fact_Daily_Patient;

    IF @maxDateInFact IS NULL
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES ('Fill_Fact_Daily_Patient_Incremental', GETDATE(), 'no data in fact, first load needed', 'Fact_Daily_Patient', 0);
        RETURN;
    END

    SELECT @maxDateSource = CAST(CONVERT(VARCHAR(8), MAX(DateKey), 112) AS DATE) FROM Clinic_DW.dbo.Fact_Transaction_Visit;

    SET @currentDate = DATEADD(DAY, 1, CAST(CONVERT(VARCHAR(8), @maxDateInFact, 112) AS DATE));

    IF @currentDate > @maxDateSource
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
        VALUES ('Fill_Fact_Daily_Patient_Incremental', GETDATE(), 'fact already updated', 'Fact_Daily_Patient', 0);
        RETURN;
    END

    WHILE @currentDate <= @maxDateSource
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
    VALUES ('Fill_Fact_Daily_Patient_Incremental', GETDATE(),'procedure finished', 'Fact_Daily_Patient', @rowCount);
END
GO

--=========================================================================================================================================
--======================================================= FACT ACC Patient ================================================================ 
--=========================================================================================================================================



CREATE OR ALTER PROCEDURE dbo.Fill_Fact_Daily_Patient_Incremental
AS
BEGIN
    SET NOCOUNT ON;
	DECLARE @proc_name VARCHAR(100) = 'Incrementally_Fill_FactAccPatient';
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[Fact_Daily_Patient]', 0);


    /*—- bail out if nothing to do —-*/
    IF NOT EXISTS (SELECT 1 FROM dbo.Fact_Daily_Patient)
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'No data in Fact_trans_Visit', '[Fact_Daily_Patient]', 0);
        RETURN;
    END


    ------------------------------------------------------------
    -- 1.  Date-range discovery
    ------------------------------------------------------------
    DECLARE @current_day DATE,
            @end_date    DATE;
    SELECT  @current_day = CONVERT(DATE, CAST(MIN(DateKey) AS CHAR(8)), 112),
            @end_date    = CONVERT(DATE, CAST(MAX(DateKey) AS CHAR(8)), 112)
    FROM    dbo.Fact_Daily_Patient;

    /* resume from bookmark (if it exists) */
    SELECT @current_day =
           ISNULL(
               (SELECT DATEADD(DAY, 1, MAX([Date])) FROM dbo.TimeAccFactPatient),
               @current_day
           );

    IF @current_day IS NULL OR @current_day > @end_date
    BEGIN
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'already updated', '[Fact_Daily_Patient]', 0);
        RETURN;
    END


    ------------------------------------------------------------
    -- 2.  Seed temp1 with *current* accumulator (full balance)
    ------------------------------------------------------------
    TRUNCATE TABLE temp1_FactAccPatient;

    INSERT INTO temp1_FactAccPatient
    (
        PatientID, TotalVisitCount,
        VisitServiceQuantity, AdmissionServiceQuantity,
        TotalCost, LastVisitDays, NoVisitDays
    )
    SELECT
        PatientID, TotalVisitCount,
        VisitServiceQuantity, AdmissionServiceQuantity,
        TotalCost, LastVisitDays, NoVisitDays
    FROM dbo.Fact_ACC_Patient;   -- yesterday’s running totals


    ------------------------------------------------------------
    -- 3.  Main day-by-day loop
    ------------------------------------------------------------
    WHILE @current_day <= @end_date
    BEGIN
        /* 3-A  copy yesterday -> temp2 */
        TRUNCATE TABLE temp2_FactAccPatient;
        INSERT INTO temp2_FactAccPatient
        SELECT * FROM temp1_FactAccPatient;

        /* 3-B  load today’s delta -> temp3 */
        TRUNCATE TABLE temp3_FactAccPatient;
        INSERT INTO temp3_FactAccPatient
        (
            PatientID, TotalVisitCount,
            VisitServiceQuantity, AdmissionServiceQuantity,
            TotalCost, LastVisitDays, NoVisitDays
        )
        SELECT
            PatientID,
            VisitCount                  AS TotalVisitCount,
            VisitServiceQuantity,
            AdmissionServiceQuantity,
            TotalCost,
            LastVisitDays,
            NoVisitDays
        FROM dbo.Fact_Daily_Patient
        WHERE DateKey = CAST(CONVERT(CHAR(8), @current_day, 112) AS INT);

        /* 3-C  merge temp2 + temp3 -> temp1 (running total) */
        TRUNCATE TABLE Temp.temp1_FactAccPatient;
        INSERT INTO temp1_FactAccPatient
        (
            PatientID, TotalVisitCount,
            VisitServiceQuantity, AdmissionServiceQuantity,
            TotalCost, LastVisitDays, NoVisitDays
        )
        SELECT
            COALESCE(t2.PatientID, t1.PatientID) AS PatientID,
            ISNULL(t1.TotalVisitCount,0) + ISNULL(t2.TotalVisitCount,0) AS TotalVisitCount,
            ISNULL(t1.VisitServiceQuantity,0) + ISNULL(t2.VisitServiceQuantity,0) AS VisitServiceQuantity,
            ISNULL(t1.AdmissionServiceQuantity,0) + ISNULL(t2.AdmissionServiceQuantity,0) AS AdmissionServiceQuantity,
            ISNULL(t1.TotalCost,0) + ISNULL(t2.TotalCost,0) AS TotalCost,

            /* latest “days-since” values win */
            COALESCE(t2.LastVisitDays, t1.LastVisitDays) AS LastVisitDays,
            COALESCE(t2.NoVisitDays,   t1.NoVisitDays) AS NoVisitDays
        FROM temp2_FactAccPatient t1
        FULL OUTER JOIN temp3_FactAccPatient t2
             ON t1.PatientID = t2.PatientID;

        /* advance */
        SET @current_day = DATEADD(DAY, 1, @current_day);
    END


    ------------------------------------------------------------
    -- 4.  Atomic swap (Option B merge: PICK NEW, no re-adding)
    ------------------------------------------------------------
    BEGIN TRAN;

        TRUNCATE TABLE Temp.Staging_FactAccPatient;

        INSERT INTO Staging_FactAccPatient
        (
            PatientID, TotalVisitCount,
            VisitServiceQuantity, AdmissionServiceQuantity,
            TotalCost, LastVisitDays, NoVisitDays
        )
        SELECT
            COALESCE(n.PatientID, old.PatientID)AS PatientID,
            COALESCE(n.TotalVisitCount, old.TotalVisitCount) AS TotalVisitCount,
            COALESCE(n.VisitServiceQuantity, old.VisitServiceQuantity) AS VisitServiceQuantity,
            COALESCE(n.AdmissionServiceQuantity, old.AdmissionServiceQuantity) AS AdmissionServiceQuantity,
            COALESCE(n.TotalCost, old.TotalCost) AS TotalCost,

            COALESCE(n.LastVisitDays, old.LastVisitDays) AS LastVisitDays,
            COALESCE(n.NoVisitDays, old.NoVisitDays) AS NoVisitDays
        FROM dbo.Fact_ACC_Patient AS old
        FULL OUTER JOIN temp1_FactAccPatient AS n
             ON old.PatientID = n.PatientID;

        TRUNCATE TABLE dbo.Fact_ACC_Patient;
        INSERT INTO dbo.Fact_ACC_Patient
        SELECT * FROM Staging_FactAccPatient;

        TRUNCATE TABLE dbo.TimeAccFactPatient;
        INSERT INTO dbo.TimeAccFactPatient ([Date]) VALUES (@end_date);

    COMMIT TRAN;


    ------------------------------------------------------------
    -- 5.  End-of-run audit
    ------------------------------------------------------------
    DECLARE @row_count INT = (SELECT COUNT(*) FROM temp1_FactAccPatient)

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure finished', '[Fact_Daily_Patient]', @row_count);
END;
GO


--=================================================================================================================================
--====================================================== Factless Patient History ===============================================
--=================================================================================================================================


CREATE OR ALTER PROCEDURE dbo.ETL_Factless_Patient_MedicalHistory
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Factless_Patient_MedicalHistory';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '[dbo].[Factless_Patient_MedicalHistory]', 0);

    IF (NOT EXISTS (SELECT 1 FROM [dbo].[Factless_Patient_MedicalHistory]) AND EXISTS (SELECT 1 FROM [dbo].[tmp_Factless_Patient_MedicalHistory]))
	begin
		INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
		VALUES (@proc_name, GETDATE(), 'Error: Factless_Patient_MedicalHistory is empty and temp is full', '[dbo].[Factless_Patient_Insurance]', 0);
		return;
	end

    TRUNCATE TABLE [dbo].[tmp_Factless_Patient_MedicalHistory];

    -- Copy existing data from the main fact table to the temp table
    INSERT INTO [dbo].[tmp_Factless_Patient_MedicalHistory] (PatientID, DiseaseID, DiagnosisDateKey)
    SELECT PatientID, DiseaseID, DiagnosisDateKey
    FROM [dbo].[Factless_Patient_MedicalHistory];
        
    -- Insert only new records from the source into the temp table
    INSERT INTO [dbo].[tmp_Factless_Patient_MedicalHistory](PatientID, DiseaseID, DiagnosisDateKey)
    SELECT sa.PatientID, sa.DiseaseID, d.TimeKey
    FROM 
        [Clinic_SA].[dbo].[PatientMedicalHistory_sa] sa
    LEFT JOIN
        [Clinic_DW].[dbo].[Dim_Date] d ON sa.DiagnosisDate = d.FullDateAlternateKey
    WHERE NOT EXISTS (
            SELECT 1 FROM [dbo].[tmp_Factless_Patient_MedicalHistory] tmp 
            WHERE tmp.PatientID = sa.PatientID AND tmp.DiseaseID = sa.DiseaseID
                AND tmp.DiagnosisDateKey = d.TimeKey
				);
        
    TRUNCATE TABLE [dbo].[Factless_Patient_MedicalHistory];
        
    INSERT INTO [dbo].[Factless_Patient_MedicalHistory] (PatientID, DiseaseID, DiagnosisDateKey)
    SELECT PatientID, DiseaseID, DiagnosisDateKey
    FROM [dbo].[tmp_Factless_Patient_MedicalHistory];


    SELECT @number_of_rows = COUNT(*) FROM [dbo].[Factless_Patient_MedicalHistory];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Daily process finished', '[dbo].[Factless_Patient_MedicalHistory]', @number_of_rows);

    SET NOCOUNT OFF;
END
GO


