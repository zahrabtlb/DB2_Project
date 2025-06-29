use Clinic_DW
go

--================create temp tables datawarehouse======================


DROP TABLE IF EXISTS Patient_temp
DROP TABLE IF EXISTS Addresses_temp
DROP TABLE IF EXISTS tmp1_Service
DROP TABLE IF EXISTS tmp2_Service
DROP TABLE IF EXISTS tmp3_Service
DROP TABLE IF EXISTS tmp1_Department
DROP TABLE IF EXISTS tmp2_Department
DROP TABLE IF EXISTS tmp3_Department
DROP TABLE IF EXISTS Insurance_temp
DROP TABLE IF EXISTS tmp1_DoctorStaff
DROP TABLE IF EXISTS tmp2_DoctorStaff
DROP TABLE IF EXISTS tmp3_DoctorStaff
DROP TABLE IF EXISTS Disease_temp
DROP TABLE IF EXISTS VisitType_temp




CREATE TABLE Patient_temp (
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

CREATE TABLE Addresses_temp (
  AddressID		INT ,
  PatientID		INT,
  CityID		INT,
  AddressLine	VARCHAR(255),
  PostalCode	VARCHAR(10),
  AddressType	VARCHAR(10),
  IsPrimary		BIT
);
------------------------------------------------------------------------
CREATE TABLE tmp1_Service(
	[ServiceSK]			int,
	[ServiceID]			int,
	[ServiceName]		nvarchar(90),
	[ServiceCategory]	nvarchar(60),
	[BaseCost]			decimal(13, 2),
	[StartDate]			date,
	[EndDate]			date,
	[CurrentFlag]		bit
)
CREATE TABLE tmp2_Service (
	[ServiceID]			INT,
	[ServiceName]		VARCHAR(80),
	[ServiceCategory]	VARCHAR(50),
	[BaseCost]			DECIMAL(12,2)
);
CREATE TABLE tmp3_Service(
	[ServiceSK]			int,
	[ServiceID]			int,
	[ServiceName]		nvarchar(90),
	[ServiceCategory]	nvarchar(60),
	[BaseCost]			decimal(13, 2),
	[StartDate]			date,
	[EndDate]			date,
	[CurrentFlag]		bit
)
------------------------------------------------------------------------


CREATE TABLE tmp1_Department(
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
CREATE TABLE tmp2_Department(
	[DepartmentID]		int,
	[DepartmentName]	nvarchar(90),
	[ManagerStaffID]	int,
	[ManagerFullName]	nvarchar(101),
	[ManagerRole]		nvarchar(50),
)
CREATE TABLE tmp3_Department(
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

------------------------------------------------------------------------


CREATE TABLE Insurance_temp(
	[InsuranceCoID] int,
	[CompanyName]	nvarchar(100)
)

-----------------------------------------------------
CREATE TABLE tmp1_DoctorStaff(
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
CREATE TABLE tmp2_DoctorStaff(
	[StaffID]				int,
	[NationalCode]			nvarchar(12),
	[FirstName]				nvarchar(60),
	[LastName]				nvarchar(60),
	[Gender]				nvarchar(10),
	[DateOfBirth]			date,
	[Role]					nvarchar(60),
	[CurrentDepartmentID]	int,
	[CurrentDepartmentName] nvarchar(90),
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
CREATE TABLE tmp3_DoctorStaff(
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

------------------------------------------------------------------------

CREATE TABLE Disease_temp(
	[DiseaseID]		int,
	[ICD10_Code]	nvarchar(10),
	[DiseaseName]	nvarchar(90),
	[IsChronic]		bit,
	[Description]	ntext
)

------------------------------------------------------------------------

CREATE TABLE VisitType_temp(
	[VisitTypeID]	int,
	[VisitTypeName]	nvarchar(60)
)

--========================================================================================================

-- Temp table for Factless_Patient_Insurance
CREATE TABLE [dbo].[tmp_Factless_Patient_Insurance](
	[PatientID] int NOT NULL,
	[InsuranceCoID] int NOT NULL,
	[ExpireDateKey] int NULL
);
GO

-- Temp table for Factless_Patient_MedicalHistory
CREATE TABLE [dbo].[tmp_Factless_Patient_MedicalHistory](
	[PatientID] int NOT NULL,
	[DiseaseID] int NOT NULL,
	[DiagnosisDateKey] int NULL
);
GO
------------------------------------------------------------------------


CREATE TABLE temp1_FactAccPatient  (
    PatientID                INT,
    TotalVisitCount          INT,
    VisitServiceQuantity     INT,
    AdmissionServiceQuantity INT,
    TotalCost                NUMERIC(13,2),
    LastVisitDays            INT,
    NoVisitDays              INT
);
GO


CREATE TABLE temp2_FactAccPatient  (
    PatientID                INT,
    TotalVisitCount          INT,
    VisitServiceQuantity     INT,
    AdmissionServiceQuantity INT,
    TotalCost                NUMERIC(13,2),
    LastVisitDays            INT,
    NoVisitDays              INT
);
GO

CREATE TABLE temp3_FactAccPatient  (
    PatientID                INT,
    TotalVisitCount          INT,
    VisitServiceQuantity     INT,
    AdmissionServiceQuantity INT,
    TotalCost                NUMERIC(13,2),
    LastVisitDays            INT,
    NoVisitDays              INT
);
GO


CREATE TABLE Staging_FactAccPatient (
    PatientID                INT,
    TotalVisitCount          INT,
    VisitServiceQuantity     INT,
    AdmissionServiceQuantity INT,
    TotalCost                NUMERIC(13,2),
    LastVisitDays            INT,
    NoVisitDays              INT
);
GO


----------------------------------------------------------

--no use
CREATE TABLE [dbo].[visits_trans_temp](
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
---------------------------------------------------------

CREATE TABLE temp1_FactAccService(
    ServiceSK            INT,
    ServiceID            INT,
    TotalServiceCount    BIGINT,
    TotalRevenue         NUMERIC(18,2),
    InsurancePaidAmount  NUMERIC(18,2),
    UniquePatientsSum    BIGINT,
    UniqueDoctorsSum     BIGINT,
    AvgUnitCostSum       NUMERIC(18,2),
    DayCount             INT
);

CREATE TABLE temp2_FactAccService(
    ServiceSK            INT,
    ServiceID            INT,
    TotalServiceCount    BIGINT,
    TotalRevenue         NUMERIC(18,2),
    InsurancePaidAmount  NUMERIC(18,2),
    UniquePatientsSum    BIGINT,
    UniqueDoctorsSum     BIGINT,
    AvgUnitCostSum       NUMERIC(18,2),
    DayCount             INT
);

CREATE TABLE temp3_FactAccService(
    ServiceSK            INT,
    ServiceID            INT,
    TotalServiceCount    BIGINT,
    TotalRevenue         NUMERIC(18,2),
    InsurancePaidAmount  NUMERIC(18,2),
    UniquePatientsSum    BIGINT,
    UniqueDoctorsSum     BIGINT,
    AvgUnitCostSum       NUMERIC(18,2),
    DayCount             INT
);


CREATE TABLE Staging_FactAccService(
    ServiceSK            INT,
    ServiceID            INT,
    TotalServiceCount    BIGINT,
    TotalRevenue         NUMERIC(18,2),
    InsurancePaidAmount  NUMERIC(18,2),
    UniquePatientsAvg    INT,
    UniqueDoctorsAvg     INT,
    UnitCost             NUMERIC(18,2),
    AvgUnitCost          NUMERIC(18,2),
    DayCount             INT
);

CREATE TABLE Staging_Fact_ACC_Service (
    ServiceSK            INT,
    ServiceID            INT,
    TotalServiceCount    INT,
    TotalRevenue         NUMERIC(13,2),
    InsurancePaidAmount  NUMERIC(13,2),
    UniquePatientsAvg    INT,
    UniqueDoctorsAvg     INT,
    UnitCost             NUMERIC(13,2),
    AvgUnitCost          NUMERIC(13,2),
    DayCount             INT
);