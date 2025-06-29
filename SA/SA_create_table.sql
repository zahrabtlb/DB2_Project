USE master;
GO

 --Force switch to SINGLE_USER and kill all other connections
ALTER DATABASE Clinic_SA
SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

-- Now it's safe to drop the database
DROP DATABASE Clinic_SA;
GO

-- (Optional) Recreate the database
CREATE DATABASE Clinic_SA;
GO


-- ===================================================================================
-- Staging Area (SA) Table Creation
-- Description: This script creates all the necessary tables in the Clinic_SA database.
-- ===================================================================================

USE Clinic_SA;
GO


DROP TABLE IF EXISTS dbo.Payments_sa;
DROP TABLE IF EXISTS dbo.InvoiceItems_sa;
DROP TABLE IF EXISTS dbo.Invoices_sa;
DROP TABLE IF EXISTS dbo.PatientInsurances_sa;
DROP TABLE IF EXISTS dbo.InsuranceCompanies_sa;
DROP TABLE IF EXISTS dbo.VisitServices_sa;
DROP TABLE IF EXISTS dbo.PrescriptionItems_sa;
DROP TABLE IF EXISTS dbo.Prescriptions_sa;
DROP TABLE IF EXISTS dbo.Admissions_sa;
DROP TABLE IF EXISTS dbo.Vitals_sa;
DROP TABLE IF EXISTS dbo.Visits_sa;
DROP TABLE IF EXISTS dbo.VisitTypes_sa;
DROP TABLE IF EXISTS dbo.Doctors_sa;
DROP TABLE IF EXISTS dbo.Specializations_sa;
DROP TABLE IF EXISTS dbo.Staff_sa;
DROP TABLE IF EXISTS dbo.Departments_sa;
DROP TABLE IF EXISTS dbo.Services_sa;
DROP TABLE IF EXISTS dbo.Drugs_sa;
DROP TABLE IF EXISTS dbo.PatientMedicalHistory_sa;
DROP TABLE IF EXISTS dbo.Diseases_sa;
DROP TABLE IF EXISTS dbo.PatientAllergies_sa;
DROP TABLE IF EXISTS dbo.Allergies_sa;
DROP TABLE IF EXISTS dbo.PatientAddresses_sa;
DROP TABLE IF EXISTS dbo.Cities_sa;
DROP TABLE IF EXISTS dbo.Provinces_sa;
DROP TABLE IF EXISTS dbo.Patients_sa;
GO

-- Create Tables
CREATE TABLE Patients_sa (
    PatientID		INT,
    NationalCode	NVARCHAR(10),
    FirstName		NVARCHAR(50),
    LastName		NVARCHAR(50),
    FatherName		NVARCHAR(50),
    DateOfBirth		DATE,
    PhoneNumber		NVARCHAR(15),
    Gender			NCHAR(10),
    MaritalStatus	NVARCHAR(20),
    Occupation		NVARCHAR(50),
    BloodType		NVARCHAR(3),
    IsAlive			BIT,
    DateOfDeath		DATE,
    RegistrationDate DATETIME
);

CREATE TABLE Provinces_sa (
    ProvinceID		INT,
    ProvinceName	NVARCHAR(50)
);

CREATE TABLE Cities_sa (
    CityID		INT,
    CityName	NVARCHAR(50),
    ProvinceID	INT
);

CREATE TABLE PatientAddresses_sa (
    AddressID	INT,
    PatientID	INT,
    CityID		INT,
    AddressLine NVARCHAR(255),
    PostalCode	NVARCHAR(10),
    AddressType NVARCHAR(10),
    IsPrimary	BIT
);

CREATE TABLE Allergies_sa (
    AllergyID	INT,
    AllergyName NVARCHAR(50),
    AllergyType NVARCHAR(50)
);

CREATE TABLE PatientAllergies_sa (
    PatientAllergyID INT,
    PatientID		INT,
    AllergyID		INT,
    Severity		NVARCHAR(20)
);

CREATE TABLE Diseases_sa (
    DiseaseID		INT,
    ICD10_Code		NVARCHAR(10),
    DiseaseName		NVARCHAR(80),
    IsChronic		BIT,
    [Description]	NTEXT
);

CREATE TABLE PatientMedicalHistory_sa (
    HistoryID		INT,
    PatientID		INT,
    DiseaseID		INT,
    DiagnosisDate	DATE,
    RecoveryDate	DATE
);

CREATE TABLE Drugs_sa (
    DrugID		INT,
    DrugName	NVARCHAR(80),
    BrandName	NVARCHAR(80),
    Manufacturer NVARCHAR(80),
    DosageForm	NVARCHAR(50)
);

CREATE TABLE Services_sa (
    ServiceID		INT,
    ServiceName		NVARCHAR(80),
    ServiceCategory NVARCHAR(50),
    BaseCost		DECIMAL(12, 2)
);

CREATE TABLE Departments_sa (
    DepartmentID	INT,
    DepartmentName	NVARCHAR(50),
    ManagerStaffID	INT
);

CREATE TABLE Staff_sa (
    StaffID			INT,
    NationalCode	NVARCHAR(10),
    FirstName		NVARCHAR(50),
    LastName		NVARCHAR(50),
    DateOfBirth		DATE,
    Gender			NCHAR(10),
    Role			NVARCHAR(50),
    DepartmentID	INT,
    PhoneNumber		NVARCHAR(15),
    Email			NVARCHAR(50),
    HireDate		DATE,
    Salary			DECIMAL(12, 2),
    IsActive		BIT
);

CREATE TABLE Specializations_sa (
    SpecializationID	INT,
    SpecializationName	NVARCHAR(50)
);

CREATE TABLE Doctors_sa (
    DoctorStaffID			INT,
    MedicalLicenseNumber	NVARCHAR(50),
    SpecializationID		INT,
    MedicalDegree			NVARCHAR(50),
    PracticeLicenseExpiryDate DATE,
    YearsOfExperience		INT
);

CREATE TABLE VisitTypes_sa (
    VisitTypeID		INT,
    VisitTypeName	NVARCHAR(50)
);

CREATE TABLE Visits_sa (
    VisitID			INT,
    PatientID		INT,
    DoctorStaffID	INT,
    VisitDateTime	DATETIME,
    VisitTypeID		INT,
    DepartmentID	INT,
    DiagnosisHistoryID INT
);

CREATE TABLE Vitals_sa (
    VitalID					INT,
    VisitID					INT,
    RecordDateTime			DATETIME,
    Height_cm				DECIMAL(5, 2),
    Weight_kg				DECIMAL(5, 2),
    BloodPressure_Systolic	INT,
    BloodPressure_Diastolic INT,
    HeartRate_bpm			INT,
    Temperature_Celsius		DECIMAL(4, 2)
);

CREATE TABLE Admissions_sa (
    AdmissionID			INT,
    VisitID				INT,
    AdmissionDateTime	DATETIME,
    DischargeDateTime	DATETIME,
    DepartmentID		INT,
    RoomNumber			NVARCHAR(20),
    BedNumber			NVARCHAR(20)
);

--CREATE TABLE Prescriptions_sa (
--    PrescriptionID		INT,
--    VisitID				INT,
--    IssueDateTime		DATETIME
--);

CREATE TABLE PrescriptionItems_sa (
    PrescriptionItemID	INT,
    PrescriptionID		INT,
    DrugID				INT,
    Dosage				NVARCHAR(50),
    Frequency			NVARCHAR(50),
    Duration_Days		INT,
	VisitID				INT,
    IssueDateTime		DATETIME
);

CREATE TABLE VisitServices_sa (
    VisitServiceID		INT,
    VisitID				INT,
    ServiceID			INT,
    ExecutionDateTime	DATETIME,
    Quantity			INT,
    IsAdmissionService	BIT
);

CREATE TABLE InsuranceCompanies_sa (
    InsuranceCoID		INT,
    CompanyName			NVARCHAR(80)
);

CREATE TABLE PatientInsurances_sa (
    PatientInsuranceID	INT,
    PatientID			INT,
    InsuranceCoID		INT,
    PolicyNumber		NVARCHAR(50),
    ExpiryDate			DATE,
    IsActive			BIT
);

--CREATE TABLE Invoices_sa (
--    InvoiceID			INT,
--    VisitID				INT,
--    IssueDateTime		DATETIME,
--    PatientInsuranceID	INT
--);

--CREATE TABLE InvoiceItems_sa (
--    LineItemID			INT,
--    InvoiceID			INT,
--    VisitServiceID		INT,
--    UnitPrice			DECIMAL(10, 2),
--    Quantity			INT,
--    InsuranceCoverage	DECIMAL(12, 2)
--);----------------------------------------------------

CREATE TABLE InvoiceItems_sa (
    LineItemID			INT,
    InvoiceID			INT,
    VisitServiceID		INT,
    UnitPrice			DECIMAL(10, 2),
    Quantity			INT,
    InsuranceCoverage	DECIMAL(12, 2),
    VisitID				INT,
    IssueDateTime		DATETIME,
    PatientInsuranceID	INT
);

CREATE TABLE Payments_sa (
    PaymentID			INT,
    InvoiceID			INT,
    PaymentDateTime		DATETIME,
    Amount				DECIMAL(12, 2),
    PaymentMethod		NVARCHAR(20),
    PayerType			NVARCHAR(20),
    ReferenceNumber		NVARCHAR(50)
);
GO