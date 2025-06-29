USE Clinic_SA;
GO


-- ===================================================================================
-- SA ETL Stored Procedures
-- Description: This script creates one stored procedure for each staging table.
-- Two patterns are used: Full Reload for dimensions and Incremental for facts.
-- ===================================================================================

-- Pattern A: Full Reload Procedures (TRUNCATE & INSERT)
-- For Dimensions and small mapping tables.
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Patients_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Patients_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Patients_sa];
    INSERT INTO [Clinic_SA].[dbo].[Patients_sa] (PatientID, NationalCode, FirstName, LastName, FatherName, DateOfBirth,
			PhoneNumber, Gender, MaritalStatus, Occupation, BloodType, IsAlive, DateOfDeath, RegistrationDate)
    SELECT PatientID, NationalCode, FirstName, LastName, FatherName, DateOfBirth, PhoneNumber, Gender, MaritalStatus,
			Occupation, BloodType, IsAlive, DateOfDeath, RegistrationDate
    FROM [Clinic_DB].[dbo].[Patients];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Patients_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Patients_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Provinces_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Provinces_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Provinces_sa];
    INSERT INTO [Clinic_SA].[dbo].[Provinces_sa] (ProvinceID, ProvinceName)
    SELECT ProvinceID, ProvinceName FROM [Clinic_DB].[dbo].[Provinces];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Provinces_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Provinces_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Cities_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Cities_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Cities_sa];
    INSERT INTO [Clinic_SA].[dbo].[Cities_sa] (CityID, CityName, ProvinceID)
    SELECT CityID, CityName, ProvinceID FROM [Clinic_DB].[dbo].[Cities];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Cities_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Cities_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_PatientAddresses_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_PatientAddresses_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[PatientAddresses_sa];
    INSERT INTO [Clinic_SA].[dbo].[PatientAddresses_sa] 
			(AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary)
    SELECT AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary 
	FROM [Clinic_DB].[dbo].[PatientAddresses];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[PatientAddresses_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[PatientAddresses_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Allergies_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Allergies_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Allergies_sa];
    INSERT INTO [Clinic_SA].[dbo].[Allergies_sa] (AllergyID, AllergyName, AllergyType)
    SELECT AllergyID, AllergyName, AllergyType FROM [Clinic_DB].[dbo].[Allergies];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Allergies_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Allergies_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_PatientAllergies_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_PatientAllergies_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[PatientAllergies_sa];
    INSERT INTO [Clinic_SA].[dbo].[PatientAllergies_sa] (PatientAllergyID, PatientID, AllergyID, Severity)
    SELECT PatientAllergyID, PatientID, AllergyID, Severity FROM [Clinic_DB].[dbo].[PatientAllergies];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[PatientAllergies_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[PatientAllergies_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Diseases_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Diseases_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Diseases_sa];
    INSERT INTO [Clinic_SA].[dbo].[Diseases_sa] (DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description])
    SELECT DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description] FROM [Clinic_DB].[dbo].[Diseases];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Diseases_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Diseases_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Drugs_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Drugs_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Drugs_sa];
    INSERT INTO [Clinic_SA].[dbo].[Drugs_sa] (DrugID, DrugName, BrandName, Manufacturer, DosageForm)
    SELECT DrugID, DrugName, BrandName, Manufacturer, DosageForm FROM [Clinic_DB].[dbo].[Drugs];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Drugs_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Drugs_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Services_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Services_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Services_sa];
    INSERT INTO [Clinic_SA].[dbo].[Services_sa] (ServiceID, ServiceName, ServiceCategory, BaseCost)
    SELECT ServiceID, ServiceName, ServiceCategory, BaseCost FROM [Clinic_DB].[dbo].[Services];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Services_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Services_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Departments_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Departments_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Departments_sa];
    INSERT INTO [Clinic_SA].[dbo].[Departments_sa] (DepartmentID, DepartmentName, ManagerStaffID)
    SELECT DepartmentID, DepartmentName, ManagerStaffID FROM [Clinic_DB].[dbo].[Departments];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Departments_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Departments_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Staff_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Staff_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Staff_sa];
    INSERT INTO [Clinic_SA].[dbo].[Staff_sa] (StaffID, NationalCode, FirstName, LastName, DateOfBirth, Gender, Role,
			DepartmentID, PhoneNumber, Email, HireDate, Salary, IsActive)
    SELECT StaffID, NationalCode, FirstName, LastName, DateOfBirth, Gender, Role, DepartmentID, PhoneNumber, Email, 
			HireDate, Salary, IsActive FROM [Clinic_DB].[dbo].[Staff];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Staff_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Staff_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Specializations_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Specializations_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Specializations_sa];
    INSERT INTO [Clinic_SA].[dbo].[Specializations_sa] (SpecializationID, SpecializationName)
    SELECT SpecializationID, SpecializationName FROM [Clinic_DB].[dbo].[Specializations];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Specializations_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Specializations_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Doctors_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Doctors_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[Doctors_sa];
    INSERT INTO [Clinic_SA].[dbo].[Doctors_sa] (DoctorStaffID, MedicalLicenseNumber, SpecializationID, MedicalDegree, 
			PracticeLicenseExpiryDate, YearsOfExperience)
    SELECT DoctorStaffID, MedicalLicenseNumber, SpecializationID, MedicalDegree, PracticeLicenseExpiryDate, YearsOfExperience
	FROM [Clinic_DB].[dbo].[Doctors];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[Doctors_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[Doctors_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_VisitTypes_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_VisitTypes_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[VisitTypes_sa];
    INSERT INTO [Clinic_SA].[dbo].[VisitTypes_sa] (VisitTypeID, VisitTypeName)
    SELECT VisitTypeID, VisitTypeName FROM [Clinic_DB].[dbo].[VisitTypes];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[VisitTypes_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[VisitTypes_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_InsuranceCompanies_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_InsuranceCompanies_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[InsuranceCompanies_sa];
    INSERT INTO [Clinic_SA].[dbo].[InsuranceCompanies_sa] (InsuranceCoID, CompanyName)
    SELECT InsuranceCoID, CompanyName FROM [Clinic_DB].[dbo].[InsuranceCompanies];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[InsuranceCompanies_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[InsuranceCompanies_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_PatientInsurances_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(100) = 'ETL_PatientInsurances_SA';
    TRUNCATE TABLE [Clinic_SA].[dbo].[PatientInsurances_sa];
    INSERT INTO [Clinic_SA].[dbo].[PatientInsurances_sa] 
			(PatientInsuranceID, PatientID, InsuranceCoID, PolicyNumber, ExpiryDate, IsActive)
    SELECT PatientInsuranceID, PatientID, InsuranceCoID, PolicyNumber, ExpiryDate, IsActive 
	FROM [Clinic_DB].[dbo].[PatientInsurances];
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_SA].[dbo].[PatientInsurances_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Full reload completed', '[Clinic_SA].[dbo].[PatientInsurances_sa]', @number_of_rows);
END;
GO
-------------------------------------------------------------------------------------


-- Pattern C: Incremental Load Procedures (WHILE LOOP)
-- For Transactional (Fact-like) tables based on a date column.
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_PatientMedicalHistory_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @number_of_rows INT = 0;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_PatientMedicalHistory_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(DiagnosisDate) FROM [Clinic_DB].[dbo].[PatientMedicalHistory];
    SELECT @end_date = CAST(MAX(DiagnosisDate) AS DATE) FROM [Clinic_DB].[dbo].[PatientMedicalHistory];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(DiagnosisDate) AS DATE)) 
	FROM  [Clinic_SA].[dbo].[PatientMedicalHistory_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[PatientMedicalHistory_sa]

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[PatientMedicalHistory_sa] (HistoryID, PatientID, DiseaseID, DiagnosisDate, RecoveryDate)
        SELECT HistoryID, PatientID, DiseaseID, DiagnosisDate, RecoveryDate
        FROM [Clinic_DB].[dbo].[PatientMedicalHistory]
        WHERE DiagnosisDate >= @current_date AND DiagnosisDate < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[PatientMedicalHistory_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[PatientMedicalHistory_sa]', @total_rows_inserted);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Visits_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Visits_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(VisitDateTime) FROM [Clinic_DB].[dbo].[Visits];
    SELECT @end_date = CAST(MAX(VisitDateTime) AS DATE) FROM [Clinic_DB].[dbo].[Visits];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(VisitDateTime) AS DATE)) 
	FROM  [Clinic_SA].[dbo].[Visits_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[Visits_sa]

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[Visits_sa] 
			(VisitID, PatientID, DoctorStaffID, VisitDateTime, VisitTypeID, DepartmentID, DiagnosisHistoryID)
        SELECT VisitID, PatientID, DoctorStaffID, VisitDateTime, VisitTypeID, DepartmentID, DiagnosisHistoryID
        FROM [Clinic_DB].[dbo].[Visits]
        WHERE VisitDateTime >= @current_date AND VisitDateTime < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;
    
    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[Visits_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[Visits_sa]', @total_rows_inserted);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Vitals_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Vitals_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(RecordDateTime) FROM [Clinic_DB].[dbo].[Vitals];
    SELECT @end_date = CAST(MAX(RecordDateTime) AS DATE) FROM [Clinic_DB].[dbo].[Vitals];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(RecordDateTime) AS DATE)) 
	FROM  [Clinic_SA].[dbo].[Vitals_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[Vitals_sa]

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[Vitals_sa] (VitalID, VisitID, RecordDateTime, Height_cm, 
				Weight_kg, BloodPressure_Systolic, BloodPressure_Diastolic, HeartRate_bpm, Temperature_Celsius)
        SELECT VitalID, VisitID, RecordDateTime, Height_cm, Weight_kg, BloodPressure_Systolic, 
				BloodPressure_Diastolic, HeartRate_bpm, Temperature_Celsius
        FROM [Clinic_DB].[dbo].[Vitals]
        WHERE RecordDateTime >= @current_date AND RecordDateTime < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;

    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[Vitals_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[Vitals_sa]', @total_rows_inserted);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Admissions_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Admissions_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(AdmissionDateTime) FROM [Clinic_DB].[dbo].[Admissions];
    SELECT @end_date = CAST(MAX(AdmissionDateTime) AS DATE) FROM [Clinic_DB].[dbo].[Admissions];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(AdmissionDateTime) AS DATE)) 
	FROM  [Clinic_SA].[dbo].[Admissions_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[Admissions_sa]

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[Admissions_sa] 
			(AdmissionID, VisitID, AdmissionDateTime, DischargeDateTime, DepartmentID, RoomNumber, BedNumber)
        SELECT AdmissionID, VisitID, AdmissionDateTime, DischargeDateTime, DepartmentID, RoomNumber, BedNumber
        FROM [Clinic_DB].[dbo].[Admissions]
        WHERE AdmissionDateTime >= @current_date AND AdmissionDateTime < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;
    
    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[Admissions_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[Admissions_sa]', @total_rows_inserted);
END;
GO
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_PrescriptionItems_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_PrescriptionItems_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(IssueDateTime) FROM [Clinic_DB].[dbo].[Prescriptions];
    SELECT @end_date = CAST(MAX(IssueDateTime) AS DATE) FROM [Clinic_DB].[dbo].[Prescriptions];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(IssueDateTime) AS DATE)) 
	FROM  [Clinic_SA].[dbo].[PrescriptionItems_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[PrescriptionItems_sa]

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[PrescriptionItems_sa] (PrescriptionItemID, PrescriptionID, DrugID, Dosage, Frequency, VisitID, IssueDateTime)
        SELECT PrescriptionItemID, p1.PrescriptionID, DrugID, Dosage, Frequency, VisitID, IssueDateTime
        FROM [Clinic_DB].[dbo].[Prescriptions] p1 join [Clinic_DB].[dbo].[PrescriptionItems] p2 
		on (p1.PrescriptionID = p2.PrescriptionID)
        WHERE IssueDateTime >= @current_date AND IssueDateTime < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;
    
    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[PrescriptionItems_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[PrescriptionsItems_sa]', @total_rows_inserted);
END;
GO

-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_VisitServices_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_VisitServices_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(ExecutionDateTime) FROM [Clinic_DB].[dbo].[VisitServices];
    SELECT @end_date = CAST(MAX(ExecutionDateTime) AS DATE) FROM [Clinic_DB].[dbo].[VisitServices];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(ExecutionDateTime) AS DATE))	
	FROM  [Clinic_SA].[dbo].[VisitServices_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[VisitServices_sa]

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[VisitServices_sa] 
			(VisitServiceID, VisitID, ServiceID, ExecutionDateTime, Quantity, IsAdmissionService)
        SELECT VisitServiceID, VisitID, ServiceID, ExecutionDateTime, Quantity, IsAdmissionService
        FROM [Clinic_DB].[dbo].[VisitServices]
        WHERE ExecutionDateTime >= @current_date AND ExecutionDateTime < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;
    
    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[VisitServices_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[VisitServices_sa]', @total_rows_inserted);
END;
GO

-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_InvoiceItems_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_InvoiceItems_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(IssueDateTime) FROM [Clinic_DB].[dbo].[Invoices];
    SELECT @end_date = CAST(MAX(IssueDateTime) AS DATE) FROM [Clinic_DB].[dbo].[Invoices];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(IssueDateTime) AS DATE)) 
	FROM  [Clinic_SA].[dbo].[InvoiceItems_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[InvoiceItems_sa]


    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[InvoiceItems_sa] (LineItemID, InvoiceID, VisitServiceID, UnitPrice, Quantity, InsuranceCoverage,VisitID, IssueDateTime, PatientInsuranceID)
        SELECT LineItemID, i1.InvoiceID, VisitServiceID, UnitPrice, Quantity, InsuranceCoverage,VisitID, IssueDateTime, PatientInsuranceID
        FROM [Clinic_DB].[dbo].[Invoices] i1 join [Clinic_DB].[dbo].[InvoiceItems] i2 on (i1.InvoiceID = i2.InvoiceID)
        WHERE IssueDateTime >= @current_date AND IssueDateTime < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;
    
    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[InvoiceItems_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[InvoiceItems_sa]', @total_rows_inserted);
END;
GO


-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.ETL_Payments_SA
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_rows_inserted INT = 0;
    DECLARE @proc_name VARCHAR(100) = 'ETL_Payments_SA';
    DECLARE @minimum_date DATE;
    DECLARE @end_date DATE;
    DECLARE @current_date DATE;

    SELECT @minimum_date = MIN(PaymentDateTime) FROM [Clinic_DB].[dbo].[Payments];
    SELECT @end_date = CAST(MAX(PaymentDateTime) AS DATE) FROM [Clinic_DB].[dbo].[Payments];
    SELECT @current_date = ISNULL((SELECT DATEADD(DAY, 1, CAST(MAX(PaymentDateTime) AS DATE)) 
	FROM [Clinic_SA].[dbo].[Payments_sa]), @minimum_date);
	select @total_rows_inserted = count(*) from [Clinic_SA].[dbo].[Payments_sa]


    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO [Clinic_SA].[dbo].[Payments_sa] 
			(PaymentID, InvoiceID, PaymentDateTime, Amount, PaymentMethod, PayerType, ReferenceNumber)
        SELECT PaymentID, InvoiceID, PaymentDateTime, Amount, PaymentMethod, PayerType, ReferenceNumber
        FROM [Clinic_DB].[dbo].[Payments]
        WHERE PaymentDateTime >= @current_date AND PaymentDateTime < DATEADD(DAY, 1, @current_date);

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;
    
    SELECT @total_rows_inserted = COUNT(*) - @total_rows_inserted FROM [Clinic_SA].[dbo].[Payments_sa];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Incremental load completed', '[Clinic_SA].[dbo].[Payments_sa]', @total_rows_inserted);
END;
GO
