USE Clinic_SA
GO

CREATE OR ALTER PROCEDURE dbo.ETL_Main_SA
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc_name VARCHAR(100) = 'ETL_Main_SA';
    DECLARE @error_message NVARCHAR(MAX);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Main SA ETL Process Started', 'All SA Tables', 0);

        -- =========================================================================
        -- Step 1: Execute Full Reload ETLs for independent dimension-like tables
        -- =========================================================================
        EXEC dbo.ETL_Provinces_SA;
        EXEC dbo.ETL_Specializations_SA;
        EXEC dbo.ETL_Services_SA;
        EXEC dbo.ETL_Drugs_SA;
        EXEC dbo.ETL_Diseases_SA;
        EXEC dbo.ETL_Allergies_SA;
        EXEC dbo.ETL_InsuranceCompanies_SA;
        EXEC dbo.ETL_Departments_SA;
        EXEC dbo.ETL_VisitTypes_SA;
        
        -- =========================================================================
        -- Step 2: Execute Full Reload ETLs for dependent dimension-like tables
        -- =========================================================================
        EXEC dbo.ETL_Cities_SA;        
        EXEC dbo.ETL_Staff_SA;         
        EXEC dbo.ETL_Doctors_SA;       
        EXEC dbo.ETL_Patients_SA;      
        
        -- =========================================================================
        -- Step 3: Execute Full Reload ETLs for mapping tables
        -- =========================================================================
        EXEC dbo.ETL_PatientAddresses_SA;   
        EXEC dbo.ETL_PatientAllergies_SA;   
        EXEC dbo.ETL_PatientInsurances_SA;  

        -- =========================================================================
        -- Step 4: Execute Incremental Load ETLs for transactional tables
        -- =========================================================================
        EXEC dbo.ETL_PatientMedicalHistory_SA;
        EXEC dbo.ETL_Visits_SA;               
        EXEC dbo.ETL_Admissions_SA;           
        EXEC dbo.ETL_Vitals_SA;               
        EXEC dbo.ETL_PrescriptionItems_SA;    
        EXEC dbo.ETL_VisitServices_SA;        
        EXEC dbo.ETL_InvoiceItems_SA;         
        EXEC dbo.ETL_Payments_SA;             
        
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Master SA ETL Process Finished Successfully', 'All SA Tables', 0);

    SET NOCOUNT OFF;
END
GO
