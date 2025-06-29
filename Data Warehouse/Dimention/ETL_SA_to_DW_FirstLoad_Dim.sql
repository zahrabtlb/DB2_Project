use Clinic_DW
go


--#########################################################################################
----------------------------------Firstload ETL Dimentions---------------------------------
--#########################################################################################

create OR ALTER procedure FirstLoad_ETùL_Dim_Date

as
begin
	DECLARE @number_of_rows INT;
	TRUNCATE TABLE [dbo].[Dim_Date];

	BULK INSERT [dbo].[Dim_Date]
	FROM 'C:\Users\RSKALA\Desktop\Clinic_DataWareHouse\Date.txt' --address of Date.txt
	WITH
	(
		FIRSTROW = 2,
		FIELDTERMINATOR = '\t',
		ROWTERMINATOR = '\n',
		CODEPAGE = '65001' 
	);
	
	select @number_of_rows = count(*) from [Dim_Date]
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values ('FirstLoad_ETL_Dim_Date', GETDATE(), 'Dim_Date', @number_of_rows)

end
GO

--********************************************************************************************************************

CREATE OR ALTER PROCEDURE FirstLoad_ETL_Dim_Time
AS
BEGIN
    SET NOCOUNT ON;
	DECLARE @number_of_rows INT;
    TRUNCATE TABLE dbo.Dim_Time;
    DECLARE @HourCounter TINYINT = 0;
    DECLARE @MinuteCounter TINYINT = 0;
    WHILE @HourCounter < 24
    BEGIN
        SET @MinuteCounter = 0;
        WHILE @MinuteCounter < 60
        BEGIN
            INSERT INTO Dim_Time (TimeKey, HourNo, MinuteNo)
            VALUES ((@HourCounter * 100) + @MinuteCounter, @HourCounter, @MinuteCounter);
            SET @MinuteCounter = @MinuteCounter + 1;
        END
        SET @HourCounter = @HourCounter + 1;
    END
	select @number_of_rows = count(*) from [Dim_Time]
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values ('FirstLoad_ETL_Dim_Time', GETDATE(), 'Dim_Time', @number_of_rows)

    SET NOCOUNT OFF;
END
GO

--***********************************************************************************************************************************************************

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Dim_Patient
AS
BEGIN
    SET NOCOUNT ON;

	declare @number_of_rows int;
	declare @proc_name varchar(50);
	set @proc_name = 'FirstLoad_ETL_Dim_Patient'

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Dim_Patient', 0);

    TRUNCATE TABLE dbo.Dim_Patient;
	TRUNCATE TABLE dbo.Addresses_temp;

	insert into Addresses_temp (AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary)
	select AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary
	from(
		select AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary,
				ROW_NUMBER() OVER(PARTITION BY PatientID ORDER BY IsPrimary DESC, pa.AddressID DESC) as rn
		from [Clinic_SA].[dbo].[PatientAddresses_sa] pa
	) as add1
	where rn = 1

	select @number_of_rows = count(*) from Addresses_temp
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values (@proc_name, GETDATE(), 'Addresses_temp', @number_of_rows)


    INSERT INTO [Clinic_DW].[dbo].[Dim_Patient] (
        PatientID, NationalCode, FirstName, LastName, FatherName, DateOfBirth,
        phoneNumber, Gender, MaritalStatus, Occupation, BloodType, IsAlive,
        DateOfDeath, RegistrationDate, AddressID, AddressLine, AddressType,
        PostalCode, CityID, CityName, ProvinceID, ProvinceName
    )
    SELECT 
            p.PatientID, p.NationalCode, p.FirstName, p.LastName, p.FatherName, p.DateOfBirth, p.PhoneNumber,
			p.Gender, p.MaritalStatus, p.Occupation, p.BloodType, p.IsAlive, p.DateOfDeath, p.RegistrationDate,
            pa.AddressID, pa.AddressLine, pa.AddressType, pa.PostalCode,
            c.CityID, c.CityName,
            pr.ProvinceID, pr.ProvinceName
        FROM 
            [Clinic_SA].[dbo].Patients_sa p
        LEFT JOIN 
            Addresses_temp pa ON p.PatientID = pa.PatientID
        LEFT JOIN 
             [Clinic_SA].[dbo].Cities_sa c ON pa.CityID = c.CityID
        LEFT JOIN 
             [Clinic_SA].[dbo].Provinces_sa pr ON c.ProvinceID = pr.ProvinceID

	select @number_of_rows = count(*) from [Clinic_DW].[dbo].[Dim_Patient] 
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values (@proc_name, GETDATE(), 'Dim_Patient', @number_of_rows)

	INSERT INTO [Clinic_DW].[dbo].[log] ([procedure_name], date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', 'Dim_Patient', 0);

    SET NOCOUNT OFF;
END
GO

--**********************************************************************************************************************************************************


CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Dim_Service
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows int;
    DECLARE @proc_name varchar(50);
    SET @proc_name = 'FirstLoad_ETL_Dim_Service';
	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Dim_Service', 0);

    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Service];

    INSERT INTO [Clinic_DW].[dbo].[Dim_Service] 
		(ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag)
    SELECT 
		ROW_NUMBER() OVER (ORDER BY ServiceID),
        ServiceID, ServiceName, ServiceCategory, BaseCost, GETDATE(), NULL, 1         
    FROM 
        [Clinic_SA].[dbo].[Services_sa];

    SELECT @number_of_rows = count(*) from [Clinic_DW].[dbo].[Dim_Service] 
    INSERT INTO [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
    VALUES (@proc_name, GETDATE(), 'Dim_Service', @number_of_rows);

	INSERT INTO [Clinic_DW].[dbo].[log] ([procedure_name], date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', 'Dim_Service', 0);

    SET NOCOUNT OFF;
END
GO

--************************************************************************************************************************************************************

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Dim_Department
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'FirstLoad_ETL_Dim_Department';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Dim_Department', 0);

    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Department];

    -- Insert all data directly from the source to the destination
    INSERT INTO [Clinic_DW].[dbo].[Dim_Department]
		(DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag)
    SELECT 
         ROW_NUMBER() OVER (ORDER BY d.DepartmentID),
		 d.DepartmentID, d.DepartmentName, d.ManagerStaffID, s.FirstName + ' ' + s.LastName, s.[Role], GETDATE(), NULL, 1
    FROM 
        [Clinic_SA].[dbo].[Departments_sa] d
    LEFT JOIN
        [Clinic_SA].[dbo].[Staff_sa] s ON d.ManagerStaffID = s.StaffID;

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_Department];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', 'Dim_Department', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', 'Dim_Department', 0);

    SET NOCOUNT OFF;
END
GO

--***********************************************************************************************************************************************

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Dim_Insurance
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'FirstLoad_ETL_Dim_Insurance';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Dim_Insurance', 0);

    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Insurance];

    -- Insert all data directly from the source to the destination
    INSERT INTO [Clinic_DW].[dbo].[Dim_Insurance] (InsuranceCoID, CompanyName)
    SELECT 
        InsuranceCoID, CompanyName
    FROM 
        [Clinic_SA].[dbo].[InsuranceCompanies_sa];

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_Insurance];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', 'Dim_Insurance', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', 'Dim_Insurance', 0);

    SET NOCOUNT OFF;
END
GO

--********************************************************************************************************************************************************

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Dim_DoctorStaff
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'FirstLoad_ETL_Dim_DoctorStaff';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Dim_DoctorStaff', 0);

    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_DoctorStaff];

    --  Insert all data directly from the source to the destination
    INSERT INTO [Clinic_DW].[dbo].[Dim_DoctorStaff]
    (
        StaffID, NationalCode, FirstName, LastName, Gender, DateOfBirth, [Role],
        CurrentDepartmentID, CurrentDepartmentName, 
        OriginalDepartmentID, OriginalDepartmentName, EffectiveDate,
        phoneNumber, Email, HireDate, Salary, IsActive, IsDoctor, MedicalLicenceNumber,
        SpecializationID, SpecializationName, MedicalDegree, PracticeLicenceExpiryDate, YearsOfExperience
    )
    SELECT 
        s.StaffID, s.NationalCode, s.FirstName, s.LastName, s.Gender, s.DateOfBirth, s.[Role],
        s.DepartmentID, d.DepartmentName, NULL, NULL,                    
        NULL, s.PhoneNumber, s.Email, s.HireDate, s.Salary, s.IsActive,
        CASE WHEN doc.DoctorStaffID IS NOT NULL THEN 1 ELSE 0 END, -- IsDoctor
        doc.MedicalLicenseNumber, doc.SpecializationID, spec.SpecializationName,
        doc.MedicalDegree, doc.PracticeLicenseExpiryDate, doc.YearsOfExperience
    FROM 
        [Clinic_SA].[dbo].[Staff_sa] s
    LEFT JOIN [Clinic_SA].[dbo].[Departments_sa] d ON s.DepartmentID = d.DepartmentID
    LEFT JOIN [Clinic_SA].[dbo].[Doctors_sa] doc ON s.StaffID = doc.DoctorStaffID
    LEFT JOIN [Clinic_SA].[dbo].[Specializations_sa] spec ON doc.SpecializationID = spec.SpecializationID;

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_DoctorStaff];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', 'Dim_DoctorStaff', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', 'Dim_DoctorStaff', 0);

    SET NOCOUNT OFF;
END
GO

--*******************************************************************************************************************************************************

CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Dim_Disease
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'FirstLoad_ETL_Dim_Disease';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Dim_Disease', 0);

    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Disease];

    -- Insert all data directly from the source to the destination
    INSERT INTO [Clinic_DW].[dbo].[Dim_Disease]
		(DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description])
    SELECT 
        DiseaseID, ICD10_Code, DiseaseName, IsChronic , [Description]
    FROM 
        [Clinic_SA].[dbo].[Diseases_sa];

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_Disease];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', 'Dim_Disease', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', 'Dim_Disease', 0);

    SET NOCOUNT OFF;
END
GO

--********************************************************************************************************************************************************************


CREATE OR ALTER PROCEDURE dbo.FirstLoad_ETL_Dim_VisitType
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'FirstLoad_ETL_Dim_VisitType';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', 'Dim_VisitType', 0);

    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_VisitType];

    --  Insert all data directly from the source to the destination
    INSERT INTO [Clinic_DW].[dbo].[Dim_VisitType] (VisitTypeID, VisitTypeName)
    SELECT VisitTypeID, VisitTypeName
    FROM [Clinic_SA].[dbo].[VisitTypes_sa];

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_VisitType];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Initial data loaded', 'Dim_VisitType', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', 'Dim_VisitType', 0);

    SET NOCOUNT OFF;
END
GO


--*******************************************************************************************************************************************************************
