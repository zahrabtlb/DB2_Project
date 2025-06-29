use Clinic_DW
go


--###################################################################################################
--------------------------------------ETL Dimentions daily load--------------------------------------
--###################################################################################################

CREATE OR ALTER PROCEDURE dbo.ETL_Dim_Patient
AS
BEGIN
    SET NOCOUNT ON;

	declare @number_of_rows int;
	declare @proc_name varchar(50);
	set @proc_name = 'ETL_Dim_Patient'

	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '', 0);

	TRUNCATE TABLE dbo.Addresses_temp;
	TRUNCATE TABLE dbo.Patient_temp;

	insert into Addresses_temp (AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary)
	select AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary
	from(
		select AddressID, PatientID, CityID, AddressLine, PostalCode, AddressType, IsPrimary,
				ROW_NUMBER() OVER(PARTITION BY PatientID ORDER BY IsPrimary DESC, pa.AddressID DESC) as rn
		from Clinic_SA.dbo.PatientAddresses_sa pa
	) as add1
	where rn = 1

	select @number_of_rows = count(*) from Addresses_temp
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values (@proc_name, GETDATE(), 'Addresses_temp', @number_of_rows)

	-----------------------------------------------------------------

    INSERT INTO [dbo].[Patient_temp] (
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

	select @number_of_rows = count(*) from Patient_temp
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values (@proc_name, GETDATE(), 'Patient_temp', @number_of_rows)

	---------------------------------------------------------------------

	insert into [dbo].[Patient_temp](
	    PatientID, NationalCode, FirstName, LastName, FatherName, DateOfBirth,
        phoneNumber, Gender, MaritalStatus, Occupation, BloodType, IsAlive,
        DateOfDeath, RegistrationDate, AddressID, AddressLine, AddressType,
        PostalCode, CityID, CityName, ProvinceID, ProvinceName
	)
	select
	    PatientID, NationalCode, FirstName, LastName, FatherName, DateOfBirth,
        phoneNumber, Gender, MaritalStatus, Occupation, BloodType, IsAlive,
        DateOfDeath, RegistrationDate, AddressID, AddressLine, AddressType,
        PostalCode, CityID, CityName, ProvinceID, ProvinceName
	from [Clinic_DW].[dbo].[Dim_Patient]
	WHERE PatientID not in (select PatientID from Patient_temp)

	select @number_of_rows = count(*) from Patient_temp
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values (@proc_name, GETDATE(), 'Patient_temp', @number_of_rows)

	--------------------------------------------------------------------

	TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Patient];
    INSERT INTO [Clinic_DW].[dbo].[Dim_Patient]
    (
		PatientID, NationalCode, FirstName, LastName, FatherName, DateOfBirth,
		phoneNumber, Gender, MaritalStatus, Occupation, BloodType, IsAlive,
		DateOfDeath, RegistrationDate, AddressID, AddressLine, AddressType,
		PostalCode, CityID, CityName, ProvinceID, ProvinceName
    )
    SELECT 
		PatientID, NationalCode, FirstName, LastName, FatherName, DateOfBirth,
		phoneNumber, Gender, MaritalStatus, Occupation, BloodType, IsAlive,
		DateOfDeath, RegistrationDate, AddressID, AddressLine, AddressType,
		PostalCode, CityID, CityName, ProvinceID, ProvinceName
    FROM [dbo].[Patient_temp];

	select @number_of_rows = count(*) from Dim_Patient
	insert into [Clinic_DW].[dbo].[log] ([procedure_name], [date_affected], [table_name], [rows_affected])
	values (@proc_name, GETDATE(), 'Dim_Patient', @number_of_rows)

	INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);

    SET NOCOUNT OFF;
END
GO

--***********************************************************************************************************************************************************
--***********************************************************************************************************************************************************


CREATE OR ALTER PROCEDURE dbo.ETL_Dim_Service
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'ETL_Dim_Service';
    DECLARE @max_sk INT;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started, scd 2', '', 0);

    -- =========================================================================
    -- 1. PREPARATION: Truncate temp tables & load snapshots
    -- =========================================================================
    TRUNCATE TABLE dbo.tmp1_Service;
    TRUNCATE TABLE dbo.tmp2_Service;
    TRUNCATE TABLE dbo.tmp3_Service;

    -- Step 1.1: Load current state of the Dimension into tmp1
    INSERT INTO dbo.tmp1_Service
        (ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag)
    SELECT
        ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag
    FROM
        [Clinic_DW].[dbo].[Dim_Service];

    SELECT @number_of_rows = count(*) from tmp1_Service;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded DW snapshot to tmp1', 'tmp1_Service', @number_of_rows);

    -- Step 1.2: Load source data into tmp2
    INSERT INTO dbo.tmp2_Service
        (ServiceID, ServiceName, ServiceCategory, BaseCost)
    SELECT
        ServiceID, ServiceName, ServiceCategory, BaseCost
    FROM
        [Clinic_SA].[dbo].[Services_sa];

    SELECT @number_of_rows = count(*) from tmp2_Service;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded SA data to tmp2', 'tmp2_Service', @number_of_rows);

    -- =========================================================================
    -- 2. TRANSFORMATION: Apply SCD Type 2 logic and build tmp3
    -- =========================================================================

    -- Step 2.1: Insert unchanged current records and all historical records into tmp3
    INSERT INTO dbo.tmp3_Service
        (ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag)
    SELECT
        t1.ServiceSK, t1.ServiceID, t1.ServiceName, t1.ServiceCategory, t1.BaseCost, t1.StartDate, t1.EndDate, t1.CurrentFlag
    FROM 
        dbo.tmp1_Service t1
    LEFT JOIN 
        dbo.tmp2_Service t2 ON t1.ServiceID = t2.ServiceID
    WHERE
        t1.CurrentFlag = 0 -- Keep all historical records
        OR
        (t1.CurrentFlag = 1 AND t2.ServiceID IS NOT NULL AND t1.BaseCost = t2.BaseCost) -- Keep unchanged current records
        OR
        (t1.CurrentFlag = 1 AND t2.ServiceID IS NULL); -- Keep records that are no longer in source (deleted)

    -- Step 2.2: Expire old versions of records where BaseCost has changed
    INSERT INTO dbo.tmp3_Service
        (ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag)
    SELECT
        t1.ServiceSK, t1.ServiceID, t1.ServiceName, t1.ServiceCategory, t1.BaseCost, t1.StartDate, GETDATE(), 0
    FROM 
        dbo.tmp1_Service t1
    JOIN 
        dbo.tmp2_Service t2 ON t1.ServiceID = t2.ServiceID
    WHERE
        t1.CurrentFlag = 1 AND t1.BaseCost <> t2.BaseCost;

    -- Step 2.3: Get the current maximum surrogate key before inserting new versions
    SELECT @max_sk = ISNULL(MAX(ServiceSK), 0) FROM dbo.tmp3_Service;

    -- Step 2.4: Insert new versions for records where BaseCost has changed
    INSERT INTO dbo.tmp3_Service
        (ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag)
    SELECT
        @max_sk + ROW_NUMBER() OVER (ORDER BY t2.ServiceID),
        t2.ServiceID, t2.ServiceName, t2.ServiceCategory, t2.BaseCost, GETDATE(), NULL, 1
    FROM 
        dbo.tmp2_Service t2
    JOIN 
        dbo.tmp1_Service t1 ON t1.ServiceID = t2.ServiceID
    WHERE
        t1.CurrentFlag = 1 AND t1.BaseCost <> t2.BaseCost;

    -- Step 2.5: Get the new maximum surrogate key before inserting brand new records
    SELECT @max_sk = ISNULL(MAX(ServiceSK), 0) FROM dbo.tmp3_Service;

    -- Step 2.6: Insert brand new records (exist in source, but not in dimension)
    INSERT INTO dbo.tmp3_Service
        (ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag)
    SELECT
        @max_sk + ROW_NUMBER() OVER (ORDER BY t2.ServiceID),
        t2.ServiceID, t2.ServiceName, t2.ServiceCategory, t2.BaseCost, GETDATE(), NULL, 1
    FROM
        dbo.tmp2_Service t2
    WHERE NOT EXISTS (SELECT 1 FROM dbo.tmp1_Service t1 WHERE t1.ServiceID = t2.ServiceID);

    -- Log the build of tmp3
    SELECT @number_of_rows = count(*) from tmp3_Service;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Built final staging data', 'tmp3_Service', @number_of_rows);

    -- =========================================================================
    -- 3. LOAD: Truncate final dimension and load from tmp3
    -- =========================================================================
    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Service];

    INSERT INTO [Clinic_DW].[dbo].[Dim_Service]
        (ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag)
    SELECT
        ServiceSK, ServiceID, ServiceName, ServiceCategory, BaseCost, StartDate, EndDate, CurrentFlag
    FROM
        dbo.tmp3_Service;

    -- Log the final load
    SELECT @number_of_rows = count(*) from Dim_Service;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Final Load to Dim_Service', 'Dim_Service', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);
    
    SET NOCOUNT OFF;
END
GO

--********************************************************************************************************************************************************
--***********************************************************************************************************************************************************

CREATE OR ALTER PROCEDURE dbo.ETL_Dim_Department
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'ETL_Dim_Department';
    DECLARE @max_sk INT;

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started, scd 2', '', 0);

    -- =========================================================================
    -- 1. PREPARATION: Truncate temp tables & load snapshots
    -- =========================================================================
    TRUNCATE TABLE dbo.tmp1_Department;
    TRUNCATE TABLE dbo.tmp2_Department;
    TRUNCATE TABLE dbo.tmp3_Department;

    -- Step 1.1: Load current state of the Dimension into tmp1
    INSERT INTO dbo.tmp1_Department
        (DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag)
    SELECT
        DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag
    FROM
        [Clinic_DW].[dbo].[Dim_Department];

    SELECT @number_of_rows = COUNT(*) FROM dbo.tmp1_Department;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded DW snapshot to tmp1', 'tmp1_Department', @number_of_rows);

    -- Step 1.2: Load source data into tmp2
    INSERT INTO dbo.tmp2_Department
        (DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole)
    SELECT
        d.DepartmentID, d.DepartmentName, d.ManagerStaffID, s.FirstName + ' ' + s.LastName, s.[Role]
    FROM
        [Clinic_SA].[dbo].[Departments_sa] d
    LEFT JOIN
        [Clinic_SA].[dbo].[Staff_sa] s ON d.ManagerStaffID = s.StaffID;

    SELECT @number_of_rows = COUNT(*) FROM dbo.tmp2_Department;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded SA data to tmp2', 'tmp2_Department', @number_of_rows);

    -- =========================================================================
    -- 2. TRANSFORMATION: Apply SCD Type 2 logic and build tmp3
    -- =========================================================================

    -- Step 2.1: Insert unchanged current records and all historical records into tmp3
    INSERT INTO dbo.tmp3_Department
        (DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag)
    SELECT
        t1.DepartmentSK, t1.DepartmentID, t1.DepartmentName, t1.ManagerStaffID, t1.ManagerFullName, t1.ManagerRole, t1.StartDate, t1.EndDate, t1.CurrentFlag
    FROM 
        dbo.tmp1_Department t1
    LEFT JOIN 
        dbo.tmp2_Department t2 ON t1.DepartmentID = t2.DepartmentID
    WHERE
        t1.CurrentFlag = 0 -- Keep all historical records
        OR
        (t1.CurrentFlag = 1 AND t2.DepartmentID IS NOT NULL AND ISNULL(t1.ManagerStaffID, -1) = ISNULL(t2.ManagerStaffID, -1)) -- Keep unchanged current records
        OR
        (t1.CurrentFlag = 1 AND t2.DepartmentID IS NULL); -- Keep records that are no longer in source (deleted)

    -- Step 2.2: Expire old versions of records where ManagerStaffID has changed
    INSERT INTO dbo.tmp3_Department
        (DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag)
    SELECT
        t1.DepartmentSK, t1.DepartmentID, t1.DepartmentName, t1.ManagerStaffID, t1.ManagerFullName, t1.ManagerRole, t1.StartDate, GETDATE(), 0
    FROM 
        dbo.tmp1_Department t1
    JOIN 
        dbo.tmp2_Department t2 ON t1.DepartmentID = t2.DepartmentID
    WHERE
        t1.CurrentFlag = 1 AND ISNULL(t1.ManagerStaffID, -1) <> ISNULL(t2.ManagerStaffID, -1);

    -- Step 2.3: Get the current maximum surrogate key before inserting new versions
    SELECT @max_sk = ISNULL(MAX(DepartmentSK), 0) FROM dbo.tmp3_Department;

    -- Step 2.4: Insert new versions for records where ManagerStaffID has changed
    INSERT INTO dbo.tmp3_Department
        (DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag)
    SELECT
        @max_sk + ROW_NUMBER() OVER (ORDER BY t2.DepartmentID),
        t2.DepartmentID, t2.DepartmentName, t2.ManagerStaffID, t2.ManagerFullName, t2.ManagerRole, GETDATE(), NULL, 1
    FROM 
        dbo.tmp2_Department t2
    JOIN 
        dbo.tmp1_Department t1 ON t1.DepartmentID = t2.DepartmentID
    WHERE
        t1.CurrentFlag = 1 AND ISNULL(t1.ManagerStaffID, -1) <> ISNULL(t2.ManagerStaffID, -1);

    -- Step 2.5: Get the new maximum surrogate key before inserting brand new records
    SELECT @max_sk = ISNULL(MAX(DepartmentSK), 0) FROM dbo.tmp3_Department;

    -- Step 2.6: Insert brand new records (exist in source, but not in dimension)
    INSERT INTO dbo.tmp3_Department
        (DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag)
    SELECT
        @max_sk + ROW_NUMBER() OVER (ORDER BY t2.DepartmentID),
        t2.DepartmentID, t2.DepartmentName, t2.ManagerStaffID, t2.ManagerFullName, t2.ManagerRole, GETDATE(), NULL, 1
    FROM
        dbo.tmp2_Department t2
    WHERE NOT EXISTS (SELECT 1 FROM dbo.tmp1_Department t1 WHERE t1.DepartmentID = t2.DepartmentID);

    -- Log the build of tmp3
    SELECT @number_of_rows = COUNT(*) FROM dbo.tmp3_Department;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Built final staging data', 'tmp3_Department', @number_of_rows);

    -- =========================================================================
    -- 3. LOAD: Truncate final dimension and load from tmp3
    -- =========================================================================
    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Department];

    INSERT INTO [Clinic_DW].[dbo].[Dim_Department]
        (DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag)
    SELECT
        DepartmentSK, DepartmentID, DepartmentName, ManagerStaffID, ManagerFullName, ManagerRole, StartDate, EndDate, CurrentFlag
    FROM
        dbo.tmp3_Department;

    -- Log the final load
    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_Department];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Final Load to Dim_Department', 'Dim_Department', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);
    
    SET NOCOUNT OFF;
END
GO

--********************************************************************************************************************************************
--***********************************************************************************************************************************************************


CREATE OR ALTER PROCEDURE dbo.ETL_Dim_Insurance
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @new_total INT;
    DECLARE @proc_name VARCHAR(50) = 'ETL_Dim_Insurance';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '', 0);

    -- =========================================================================
    -- 1. PREPARATION: Load source and history into staging table
    -- =========================================================================
    
    TRUNCATE TABLE dbo.Insurance_temp;

    -- Step 1.1: Load current data from the source into the staging table
    INSERT INTO dbo.Insurance_temp (InsuranceCoID, CompanyName)
    SELECT InsuranceCoID, CompanyName
    FROM [Clinic_SA].[dbo].[InsuranceCompanies_sa];

    SELECT @number_of_rows = COUNT(*) FROM dbo.Insurance_temp;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded SA data to temp', 'Insurance_temp', @number_of_rows);

    -- Step 1.2: Add historical records (deleted from source) from the dimension to the staging table
    INSERT INTO dbo.Insurance_temp (InsuranceCoID, CompanyName)
    SELECT d.InsuranceCoID, d.CompanyName
    FROM  [Clinic_DW].[dbo].[Dim_Insurance] d
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dbo.Insurance_temp temp 
        WHERE temp.InsuranceCoID = d.InsuranceCoID
    );
    
    SELECT @new_total = COUNT(*) FROM dbo.Insurance_temp;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Added historical data to temp', 'Insurance_temp', @new_total - @number_of_rows);

    -- =========================================================================
    -- 2. LOAD: Truncate final dimension and load from the complete staging table
    -- =========================================================================
    
    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Insurance];

    INSERT INTO [Clinic_DW].[dbo].[Dim_Insurance] (InsuranceCoID, CompanyName)
    SELECT InsuranceCoID, CompanyName
    FROM dbo.Insurance_temp;

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_Insurance];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Final Load to Dim_Insurance', 'Dim_Insurance', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);
    
    SET NOCOUNT OFF;
END
GO

--***********************************************************************************************************************************************************
--***********************************************************************************************************************************************************


CREATE OR ALTER PROCEDURE dbo.ETL_Dim_DoctorStaff
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @proc_name VARCHAR(50) = 'ETL_Dim_DoctorStaff';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started, scd 3', '', 0);

    -- =========================================================================
    -- 1. PREPARATION: Truncate temp tables & load snapshots
    -- =========================================================================
    TRUNCATE TABLE dbo.tmp1_DoctorStaff;
    TRUNCATE TABLE dbo.tmp2_DoctorStaff;
    TRUNCATE TABLE dbo.tmp3_DoctorStaff;

    -- Step 1.1: Load current state of the Dimension into tmp1
    INSERT INTO dbo.tmp1_DoctorStaff
    SELECT * FROM [Clinic_DW].[dbo].[Dim_DoctorStaff];

    SELECT @number_of_rows = COUNT(*) FROM dbo.tmp1_DoctorStaff;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded DW snapshot to tmp1', 'tmp1_DoctorStaff', @number_of_rows);

    -- Step 1.2: Load source data into tmp2
    INSERT INTO dbo.tmp2_DoctorStaff
        (StaffID, NationalCode, FirstName, LastName, Gender, DateOfBirth, [Role], CurrentDepartmentID, CurrentDepartmentName,
		phoneNumber, Email, HireDate, Salary, IsActive, IsDoctor, MedicalLicenceNumber, SpecializationID, SpecializationName,
		MedicalDegree, PracticeLicenceExpiryDate, YearsOfExperience)
    SELECT 
        s.StaffID, s.NationalCode, s.FirstName, s.LastName, s.Gender, s.DateOfBirth, s.[Role],
        s.DepartmentID, d.DepartmentName, s.PhoneNumber, s.Email, s.HireDate, s.Salary, s.IsActive,
        CASE WHEN doc.DoctorStaffID IS NOT NULL THEN 1 ELSE 0 END, doc.MedicalLicenseNumber,
        doc.SpecializationID, spec.SpecializationName, doc.MedicalDegree, doc.PracticeLicenseExpiryDate, doc.YearsOfExperience
    FROM 
        [Clinic_SA].[dbo].[Staff_sa] s
    LEFT JOIN [Clinic_SA].[dbo].[Departments_sa] d ON s.DepartmentID = d.DepartmentID
    LEFT JOIN [Clinic_SA].[dbo].[Doctors_sa] doc ON s.StaffID = doc.DoctorStaffID
    LEFT JOIN [Clinic_SA].[dbo].[Specializations_sa] spec ON doc.SpecializationID = spec.SpecializationID;

    SELECT @number_of_rows = COUNT(*) FROM dbo.tmp2_DoctorStaff;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded SA data to tmp2', 'tmp2_DoctorStaff', @number_of_rows);
    
    -- =========================================================================
    -- 2. TRANSFORMATION: Use FULL OUTER JOIN to build the final state in tmp3
    -- =========================================================================
    INSERT INTO dbo.tmp3_DoctorStaff
        (StaffID, NationalCode, FirstName, LastName, Gender, DateOfBirth, [Role], CurrentDepartmentID, CurrentDepartmentName,
		OriginalDepartmentID, OriginalDepartmentName, EffectiveDate, phoneNumber, Email, HireDate, Salary, IsActive, IsDoctor,
		MedicalLicenceNumber, SpecializationID, SpecializationName, MedicalDegree, PracticeLicenceExpiryDate, YearsOfExperience)
    SELECT
        ISNULL(dw.StaffID, sa.StaffID),
        ISNULL(sa.NationalCode, dw.NationalCode), -- Assume other fields are SCD Type 1 (overwrite)
        ISNULL(sa.FirstName, dw.FirstName),
        ISNULL(sa.LastName, dw.LastName),
        ISNULL(sa.Gender, dw.Gender),
        ISNULL(sa.DateOfBirth, dw.DateOfBirth),
        ISNULL(sa.[Role], dw.[Role]),
        
        -- Current Department: Always take the value from source
        ISNULL(sa.CurrentDepartmentID, dw.CurrentDepartmentID),
        ISNULL(sa.CurrentDepartmentName, dw.CurrentDepartmentName),

        -- Original Department (SCD Type 3 Logic)
        CASE WHEN sa.CurrentDepartmentID IS NOT NULL AND ISNULL(sa.CurrentDepartmentID, -1) <> ISNULL(dw.CurrentDepartmentID, -1) 
		THEN dw.CurrentDepartmentID ELSE dw.OriginalDepartmentID END,
        CASE WHEN sa.CurrentDepartmentID IS NOT NULL AND ISNULL(sa.CurrentDepartmentID, -1) <> ISNULL(dw.CurrentDepartmentID, -1)
		THEN dw.CurrentDepartmentName ELSE dw.OriginalDepartmentName END,
        
        -- Effective Date (SCD Type 3 Logic)
        CASE WHEN sa.CurrentDepartmentID IS NOT NULL AND ISNULL(sa.CurrentDepartmentID, -1) <> ISNULL(dw.CurrentDepartmentID, -1) 
		THEN GETDATE() ELSE dw.EffectiveDate END,

        ISNULL(sa.phoneNumber, dw.phoneNumber),
        ISNULL(sa.Email, dw.Email),
        ISNULL(sa.HireDate, dw.HireDate),
        ISNULL(sa.Salary, dw.Salary),
        ISNULL(sa.IsActive, dw.IsActive),
        ISNULL(sa.IsDoctor, dw.IsDoctor),
        ISNULL(sa.MedicalLicenceNumber, dw.MedicalLicenceNumber),
        ISNULL(sa.SpecializationID, dw.SpecializationID),
        ISNULL(sa.SpecializationName, dw.SpecializationName),
        ISNULL(sa.MedicalDegree, dw.MedicalDegree),
        ISNULL(sa.PracticeLicenceExpiryDate, dw.PracticeLicenceExpiryDate),
        ISNULL(sa.YearsOfExperience, dw.YearsOfExperience)
    FROM 
        dbo.tmp1_DoctorStaff AS dw
    FULL OUTER JOIN 
        dbo.tmp2_DoctorStaff AS sa ON dw.StaffID = sa.StaffID;

    SELECT @number_of_rows = COUNT(*) FROM dbo.tmp3_DoctorStaff;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Built final staging data', 'tmp3_DoctorStaff', @number_of_rows);

    -- =========================================================================
    -- 3. LOAD: Truncate final dimension and load from tmp3
    -- =========================================================================
    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_DoctorStaff];

    INSERT INTO [Clinic_DW].[dbo].[Dim_DoctorStaff]
    SELECT * FROM dbo.tmp3_DoctorStaff;

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_DoctorStaff];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Final Load to Dim_DoctorStaff', 'Dim_DoctorStaff', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);

    SET NOCOUNT OFF;
END
GO

--********************************************************************************************************************************************************************
--********************************************************************************************************************************************************************

CREATE OR ALTER PROCEDURE dbo.ETL_Dim_Disease
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @new_total INT;
    DECLARE @proc_name VARCHAR(50) = 'ETL_Dim_Disease';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '', 0);

    -- =========================================================================
    -- 1. PREPARATION: Load source and history into staging table
    -- =========================================================================
    
    TRUNCATE TABLE dbo.Disease_temp;

    -- Step 1.1: Load current data from the source into the staging table
    INSERT INTO dbo.Disease_temp
        (DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description])
    SELECT 
        DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description]
    FROM 
        [Clinic_SA].[dbo].[Diseases_sa];

    SELECT @number_of_rows = COUNT(*) FROM dbo.Disease_temp;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded SA data to temp', 'Disease_temp', @number_of_rows);

    -- Step 1.2: Add historical records (deleted from source) from the dimension to the staging table
    INSERT INTO dbo.Disease_temp
        (DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description])
    SELECT
        d.DiseaseID, d.ICD10_Code, d.DiseaseName, d.IsChronic, d.[Description]
    FROM 
        [Clinic_DW].[dbo].[Dim_Disease] d
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dbo.Disease_temp temp 
        WHERE temp.DiseaseID = d.DiseaseID
    );
    
    SELECT @new_total = COUNT(*) FROM dbo.Disease_temp;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Added historical data to temp', 'Disease_temp', @new_total - @number_of_rows);

    -- =========================================================================
    -- 2. LOAD: Truncate final dimension and load from the complete staging table
    -- =========================================================================
    
    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_Disease];

    INSERT INTO [Clinic_DW].[dbo].[Dim_Disease]
    (DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description])
    SELECT 
        DiseaseID, ICD10_Code, DiseaseName, IsChronic, [Description]
    FROM 
        dbo.Disease_temp;

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_Disease];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Final Load to Dim_Disease', 'Dim_Disease', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);
    
    SET NOCOUNT OFF;
END
GO


--********************************************************************************************************************************************************************
--********************************************************************************************************************************************************************


CREATE OR ALTER PROCEDURE dbo.ETL_Dim_VisitType
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @number_of_rows INT;
    DECLARE @new_total INT;
    DECLARE @proc_name VARCHAR(50) = 'ETL_Dim_VisitType';

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Started', '', 0);

    -- =========================================================================
    -- 1. PREPARATION: Load source and history into staging table
    -- =========================================================================
    
    TRUNCATE TABLE dbo.VisitType_temp;

    INSERT INTO dbo.VisitType_temp (VisitTypeID, VisitTypeName)
    SELECT VisitTypeID, VisitTypeName
    FROM [Clinic_SA].[dbo].[VisitTypes_sa];

    SELECT @number_of_rows = COUNT(*) FROM dbo.VisitType_temp;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Loaded SA data to temp', 'VisitType_temp', @number_of_rows);

    -- Add historical records (deleted from source) from the dimension to the staging table
    INSERT INTO dbo.VisitType_temp (VisitTypeID, VisitTypeName)
    SELECT d.VisitTypeID, d.VisitTypeName
    FROM  [Clinic_DW].[dbo].[Dim_VisitType] d
    WHERE NOT EXISTS (SELECT 1 FROM dbo.VisitType_temp temp WHERE temp.VisitTypeName = d.VisitTypeName);
    
    SELECT @new_total = COUNT(*) FROM dbo.VisitType_temp;
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Added historical data to temp', 'VisitType_temp', @new_total - @number_of_rows);

    -- =========================================================================
    -- 2. LOAD: Truncate final dimension and load from the complete staging table
    -- =========================================================================
    
    TRUNCATE TABLE [Clinic_DW].[dbo].[Dim_VisitType];

    INSERT INTO [Clinic_DW].[dbo].[Dim_VisitType](VisitTypeID, VisitTypeName)
    SELECT VisitTypeID, VisitTypeName
    FROM dbo.VisitType_temp;

    SELECT @number_of_rows = COUNT(*) FROM [Clinic_DW].[dbo].[Dim_VisitType];
    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Final Load to Dim_VisitType', 'Dim_VisitType', @number_of_rows);

    INSERT INTO [Clinic_DW].[dbo].[log] (procedure_name, date_affected, [description], table_name, rows_affected)
    VALUES (@proc_name, GETDATE(), 'Procedure Finished', '', 0);
    
    SET NOCOUNT OFF;
END
GO
