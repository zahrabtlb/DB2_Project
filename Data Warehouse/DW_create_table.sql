USE master;
GO

-- Force switch to SINGLE_USER and kill all other connections
ALTER DATABASE Clinic_DW 
SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

-- Now it's safe to drop the database
DROP DATABASE Clinic_DW;
GO

-- (Optional) Recreate the database
CREATE DATABASE Clinic_DW;
GO



use Clinic_DW
go
--################################################################################################
--###################################### Dimention Tables ########################################
--################################################################################################
DROP TABLE IF EXISTS [dbo].[Dim_Date]
DROP TABLE IF EXISTS [dbo].[Dim_Time]
DROP TABLE IF EXISTS [dbo].[Dim_Patient]
DROP TABLE IF EXISTS [dbo].[Dim_Service]
DROP TABLE IF EXISTS [dbo].[Dim_Department]
DROP TABLE IF EXISTS [dbo].[Dim_Insurance]
DROP TABLE IF EXISTS [dbo].[Dim_DoctorStaff]
DROP TABLE IF EXISTS [dbo].[Dim_Disease]
DROP TABLE IF EXISTS [dbo].[Dim_VisitType]
DROP TABLE IF EXISTS [dbo].[Log]



--create table Dim_Date
CREATE TABLE [dbo].[Dim_Date](
	[TimeKey]					[int],
	[FullDateAlternateKey]		[nvarchar](10),
	[PersianFullDateAlternateKey][nvarchar](10),
	[DayNumberOfWeek]			[int],
	[PersianDayNumberOfWeek]	[int],
	[EnglishDayNameOfWeek]		[nvarchar](10),
	[PersianDayNameOfWeek]		[nvarchar](10),
	[DayNumberOfMonth]			[int],
	[PersianDayNumberOfMonth]	[int],
	[DayNumberOfYear]			[int],
	[PersianDayNumberOfYear]	[int],
	[WeekNumberOfYear]			[int],
	[PersianWeekNumberOfYear]	[int],
	[EnglishMonthName]			[nvarchar](10),
	[PersianMonthName]			[nvarchar](10),
	[MonthNumberOfYear]			[int],
	[PersianMonthNumberOfYear]	[int],
	[CalendarQuarter]			[int],
	[PersianCalendarQuarter]	[int],
	[CalendarYear]				[int],
	[PersianCalendarYear]		[int],
	[CalendarSemester]			[int],
	[PersianCalendarSemester]	[int]
);

CREATE TABLE [dbo].Dim_Time (   -- grain = minute
    TimeKey         SMALLINT   ,-- 1420
    HourNo          TINYINT    ,
    MinuteNo        TINYINT     
);


CREATE TABLE [dbo].[Dim_Patient](
	[PatientID]		int NOT NULL,
	[NationalCode]	nvarchar(12),
	[FirstName]		nvarchar(60),
	[LastName]		nvarchar(60),
	[FatherName]	nvarchar(60),
	[DateOfBirth]	date NULL,
	[phoneNumber]	nvarchar(20),
	[Gender]		nvarchar(10),
	[MaritalStatus] nvarchar(25),
	[Occupation]	nvarchar(60),
	[BloodType]		nvarchar(5),
	[IsAlive]		bit,
	[DateOfDeath]	date,
	[RegistrationDate]date,
	[AddressID]		int,
	[AddressLine]	nvarchar(255),
	[AddressType]	nvarchar(15),
	[PostalCode]	nvarchar(15),
	[CityID]		int NULL,
	[CityName]		nvarchar(60),
	[ProvinceID]	int NULL,
	[ProvinceName]	nvarchar(60)
)

CREATE TABLE [dbo].[Dim_Service](
	[ServiceSK]			int,
	[ServiceID]			int,
	[ServiceName]		nvarchar(90),
	[ServiceCategory]	nvarchar(60),
	[BaseCost]			decimal(13, 2),
	[StartDate]			date,
	[EndDate]			date,
	[CurrentFlag]		bit
)

CREATE TABLE [dbo].[Dim_Department](
	[DepartmentSK]		int,
	[DepartmentID]		int,
	[DepartmentName]	nvarchar(90),
	[ManagerStaffID]	int,
	[ManagerFullName]	nvarchar(101),
	[ManagerRole]		nvarchar(50),
	[StartDate]			date,
	[EndDate]			date,
	[CurrentFlag]		bit
)

CREATE TABLE [dbo].[Dim_Insurance](
	[InsuranceCoID] int,
	[CompanyName]	nvarchar(100)
)

CREATE TABLE [dbo].[Dim_DoctorStaff](
	[StaffID]				int,
	[NationalCode]			nvarchar(12),
	[FirstName]				nvarchar(60),
	[LastName]				nvarchar(60),
	[Gender]				nvarchar(10),
	[DateOfBirth]			date,
	[Role]					nvarchar(60),
	[CurrentDepartmentID]	int,
	[CurrentDepartmentName] nvarchar(90),
	[OriginalDepartmentID]	int,
	[OriginalDepartmentName]nvarchar(90),
	[EffectiveDate]			date,
	[phoneNumber]			nvarchar(20),
	[Email]					nvarchar(50),
	[HireDate]				date,
	[Salary]				decimal(12, 2),
	[IsActive]				bit,
	[IsDoctor]				bit,
	[MedicalLicenceNumber]	nvarchar(60),
	[SpecializationID]		int,
	[SpecializationName]	nvarchar(50),
	[MedicalDegree]			nvarchar(60),
	[PracticeLicenceExpiryDate] date,
	[YearsOfExperience]		int
)

CREATE TABLE [dbo].[Dim_Disease](
	[DiseaseID]		int,
	[ICD10_Code]	nvarchar(10),
	[DiseaseName]	nvarchar(90),
	[IsChronic]		bit,
	[Description]	ntext
)


CREATE TABLE [dbo].[Dim_VisitType](
	[VisitTypeID]	int,
	[VisitTypeName]	nvarchar(60)
)

GO


--################################################################################################
--######################################### Fact Tables ##########################################
--################################################################################################

DROP TABLE IF EXISTS [dbo].[Fact_Transaction_Visit]
DROP TABLE IF EXISTS [dbo].[Fact_Transaction_Service]
DROP TABLE IF EXISTS [dbo].[Fact_Daily_Patient]
DROP TABLE IF EXISTS [dbo].[Fact_Daily_Service]
DROP TABLE IF EXISTS [dbo].[Fact_ACC_Patient]
DROP TABLE IF EXISTS [dbo].[Fact_ACC_Service]
DROP TABLE IF EXISTS [dbo].[Factless_Patient_Insurance]
DROP TABLE IF EXISTS [dbo].[Factless_Patient_MedicalHistory]
DROP TABLE IF EXISTS [dbo].[TimeAccFactPatient]
DROP TABLE IF EXISTS [dbo].[TimeAccFactService]

-- 1. Financial Mart

CREATE TABLE [dbo].[Fact_Transaction_Service](
	[PatientID]			int,
	[StaffID]			int,
	[ServiceSK]			int,
	[ServiceID]			int,
	[DateKey]			int,
	[TimeID]			int,
	[DepartmentSK]		int,
	[DepartmentID]		int,
	[InsuranceCoID]		int,
	[Quantity]			int,
	[UnitCost]			decimal(13, 2),
	[InsuranceCoverageAmount] decimal(13, 2)
)
CREATE TABLE [dbo].[Fact_Daily_Service](
	[ServiceSK] int,
	[ServiceID] int,
	[DateKey] int,
	[TotalServiceCount] int,
	[TotalRevenue] numeric(13, 2),
	[InsurancePaidAmount] numeric(13, 2),
	[UniquePatientsCount] int,
	[UniqueDoctorsCount] int,
	[AvgUnitCost] numeric(13, 2),
	[DayCount] int
);
CREATE TABLE [dbo].[Fact_ACC_Service](
	[ServiceSK] int,
	[ServiceID] int,
	[TotalServiceCount] int,
	[TotalRevenue] numeric(13, 2),
	[InsurancePaidAmount] numeric(13, 2),
	[UniquePatientsAvg] int,
	[UniqueDoctorsAvg] int,
	[UnitCost] numeric(13, 2),   
	[AvgUnitCost] numeric(13, 2),
	[DayCount] int
);

CREATE TABLE [dbo].[Factless_Patient_Insurance](
	[PatientID]			int,
	[InsuranceCoID]		int,
)



-- 2. Patient Mart


CREATE TABLE [dbo].[Fact_Transaction_Visit](
	[PatientID] int,
	[StaffID] int,
	[DateKey] int,
	[TimeKey] int,
	[VisitTypeID] int,
	[DepartmentSK] int,
	[DepartmentID] int,
	[DiseaseID] int,
	[VisitServiceQuantity] int,
	[AdmissionServiceQuantity] int,
	[UniqueServiceCount] int,
	[UniqueDrugCount] int,
	[medicationDurationAvg] int,
	[TotalCost] numeric(13, 2)
)
CREATE TABLE  [dbo].[Fact_Daily_Patient] (
    PatientID INT,
    DateKey INT,
    VisitCount INT,
    VisitServiceQuantity INT,
    AdmissionServiceQuantity INT,
    TotalCost NUMERIC(13,2),
    LastVisitDays INT,
    NoVisitDays INT
);


CREATE TABLE [dbo].[Fact_ACC_Patient] (
    PatientID	INT,
    TotalVisitCount INT,
    VisitServiceQuantity INT,
    AdmissionServiceQuantity INT,
    TotalCost NUMERIC(13,2),
    LastVisitDays INT,
    NoVisitDays INT
)

CREATE TABLE [dbo].[Factless_Patient_MedicalHistory](
	[PatientID]			int,
	[DiseaseID]			int,
	[DiagnosisDateKey]	int
)



--/*********************    Log Table    *********************/--


create  table [Log](
	[procedure_name] varchar(100),
	[date_affected] datetime,
	[description] varchar(256),
	[table_name] varchar(100),
	[rows_affected] varchar(50)
)



------------------------------------------
CREATE TABLE dbo.TimeAccFactPatient (
    [Date] DATE PRIMARY KEY
);
GO
CREATE TABLE dbo.TimeAccFactService (
    [Date] DATE PRIMARY KEY
);
GO
