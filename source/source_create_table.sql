USE master;
GO

-- Force switch to SINGLE_USER and kill all other connections
ALTER DATABASE Clinic_DB 
SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

-- Now it's safe to drop the database
DROP DATABASE Clinic_DB;
GO

-- (Optional) Recreate the database
CREATE DATABASE Clinic_DB;
GO



use Clinic_DB
go
--************************************patient module*******************************************
CREATE TABLE Patients (
  PatientID INT PRIMARY KEY IDENTITY(1,1),
  NationalCode VARCHAR(10) NOT NULL UNIQUE,
  FirstName VARCHAR(50) NOT NULL,
  LastName VARCHAR(50) NOT NULL,
  FatherName VARCHAR(50),
  DateOfBirth DATE NOT NULL,
  PhoneNumber varchar(15),
  Gender CHAR(1),
  MaritalStatus VARCHAR(20),
  Occupation VARCHAR(50),
  BloodType VARCHAR(3),
  IsAlive BIT DEFAULT 1,
  DateOfDeath DATE,
  RegistrationDate DATETIME DEFAULT GETDATE(),
  Notes TEXT
);

CREATE TABLE PatientContacts (
  ContactID INT PRIMARY KEY IDENTITY(1,1),
  PatientID INT NOT NULL,
  FullName VARCHAR(100) NOT NULL,
  Relationship VARCHAR(50),
  PhoneNumber VARCHAR(20) NOT NULL,
  IsEmergencyContact BIT DEFAULT 0,
  FOREIGN KEY (PatientID) REFERENCES Patients(PatientID)
);

CREATE TABLE Provinces (
  ProvinceID INT PRIMARY KEY IDENTITY(1,1),
  ProvinceName VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE Cities (
  CityID INT PRIMARY KEY IDENTITY(1,1),
  CityName VARCHAR(50) NOT NULL,
  ProvinceID INT NOT NULL,
  FOREIGN KEY (ProvinceID) REFERENCES Provinces(ProvinceID)
);

CREATE TABLE PatientAddresses (
  AddressID INT PRIMARY KEY IDENTITY(1,1),
  PatientID INT NOT NULL,
  CityID INT NOT NULL,
  AddressLine VARCHAR(255) NOT NULL,
  PostalCode VARCHAR(10),
  AddressType VARCHAR(10) DEFAULT 'Home',
  IsPrimary BIT DEFAULT 0,
  FOREIGN KEY (PatientID) REFERENCES Patients(PatientID),
  FOREIGN KEY (CityID) REFERENCES Cities(CityID)
);

--*********************************Medical Catalogs & History******************************************
CREATE TABLE Allergies (
  AllergyID INT PRIMARY KEY IDENTITY(1,1),
  AllergyName VARCHAR(50) NOT NULL,
  AllergyType VARCHAR(50)
);

CREATE TABLE PatientAllergies (
  PatientAllergyID INT PRIMARY KEY IDENTITY(1,1),
  PatientID INT NOT NULL,
  AllergyID INT NOT NULL,
  Severity VARCHAR(20),
  Notes TEXT,
  FOREIGN KEY (PatientID) REFERENCES Patients(PatientID),
  FOREIGN KEY (AllergyID) REFERENCES Allergies(AllergyID)
);

CREATE TABLE Diseases (
  DiseaseID INT PRIMARY KEY IDENTITY(1,1),
  ICD10_Code VARCHAR(10) UNIQUE,
  DiseaseName VARCHAR(80) NOT NULL,
  IsChronic BIT DEFAULT 0,
  Description TEXT
);

CREATE TABLE PatientMedicalHistory (
  HistoryID INT PRIMARY KEY IDENTITY(1,1),
  PatientID INT NOT NULL,
  DiseaseID INT NOT NULL,
  DiagnosisDate DATE,
  RecoveryDate DATE,
  Notes TEXT,
  FOREIGN KEY (PatientID) REFERENCES Patients(PatientID),
  FOREIGN KEY (DiseaseID) REFERENCES Diseases(DiseaseID)
);

CREATE TABLE Drugs (
  DrugID INT PRIMARY KEY IDENTITY(1,1),
  DrugName VARCHAR(80) NOT NULL,
  BrandName VARCHAR(80),
  Manufacturer VARCHAR(80),
  DosageForm VARCHAR(50)
);

CREATE TABLE Services (
  ServiceID INT PRIMARY KEY IDENTITY(1,1),
  ServiceName VARCHAR(80) NOT NULL,
  ServiceCategory VARCHAR(50),
  BaseCost DECIMAL(12,2)
);

--****************************************staff and organization****************************************
CREATE TABLE Departments (
  DepartmentID INT PRIMARY KEY IDENTITY(1,1),
  DepartmentName VARCHAR(50) UNIQUE NOT NULL,
  ManagerStaffID INT NULL,
);

CREATE TABLE Staff (
  StaffID INT PRIMARY KEY IDENTITY(1,1),
  NationalCode VARCHAR(10) UNIQUE NOT NULL,
  FirstName VARCHAR(50) NOT NULL,
  LastName VARCHAR(50) NOT NULL,
  DateOfBirth DATE NOT NULL,
  Gender CHAR(1),
  Role VARCHAR(50) NOT NULL, -- Doctor, Nurse, Admin, Technician
  DepartmentID INT NULL,
  PhoneNumber VARCHAR(15),
  Email VARCHAR(50) UNIQUE,
  HireDate DATE,
  Salary DECIMAL(12,2),
  IsActive BIT DEFAULT 1,
  FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);
alter table Departments 
add constraint FK_Departments_ManagerStaffID
FOREIGN KEY (ManagerStaffID) REFERENCES Staff(StaffID)


CREATE TABLE Specializations (
  SpecializationID INT PRIMARY KEY IDENTITY(1,1),
  SpecializationName VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE Doctors (
  DoctorStaffID INT PRIMARY KEY,
  MedicalLicenseNumber VARCHAR(50) UNIQUE NOT NULL,
  SpecializationID INT NULL,
  MedicalDegree VARCHAR(50),
  PracticeLicenseExpiryDate DATE,
  YearsOfExperience INT,
  FOREIGN KEY (DoctorStaffID) REFERENCES Staff(StaffID),
  FOREIGN KEY (SpecializationID) REFERENCES Specializations(SpecializationID)
);



--****************************************Visits, Admissions & Clinical Records*************************

CREATE TABLE VisitTypes(
	[VisitTypeID] int PRIMARY KEY IDENTITY(1,1),
	[VisitTypeName] nvarchar(60) NOT NULL
);

CREATE TABLE Visits (
  VisitID INT PRIMARY KEY IDENTITY(1,1),
  PatientID INT NOT NULL,
  DoctorStaffID INT NULL,
  VisitDateTime DATETIME NOT NULL,
  VisitTypeID INT NOT NULL,
  DepartmentID INT NULL,
  Notes TEXT,
  DiagnosisHistoryID INT NULL,
  FOREIGN KEY (PatientID) REFERENCES Patients(PatientID),
  FOREIGN KEY (DoctorStaffID) REFERENCES Staff(StaffID),
  FOREIGN KEY (VisitTypeID) REFERENCES VisitTypes(VisitTypeID),
  FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID),
  FOREIGN KEY (DiagnosisHistoryID) REFERENCES PatientMedicalHistory(HistoryID)
);

CREATE TABLE Vitals (
  VitalID INT PRIMARY KEY IDENTITY(1,1),
  VisitID INT NOT NULL,
  RecordDateTime DATETIME NOT NULL,
  Height_cm DECIMAL(5,2),
  Weight_kg DECIMAL(5,2),
  BloodPressure_Systolic INT,
  BloodPressure_Diastolic INT,
  HeartRate_bpm INT,
  Temperature_Celsius DECIMAL(4,2),
  FOREIGN KEY (VisitID) REFERENCES Visits(VisitID)
);

CREATE TABLE Admissions (
  AdmissionID INT PRIMARY KEY IDENTITY(1,1),
  VisitID INT UNIQUE NOT NULL,
  AdmissionDateTime DATETIME NOT NULL,
  DischargeDateTime DATETIME NULL,
  DepartmentID INT NULL,
  RoomNumber VARCHAR(20),
  BedNumber VARCHAR(20),
  FOREIGN KEY (VisitID) REFERENCES Visits(VisitID),
  FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

CREATE TABLE Prescriptions (
  PrescriptionID INT PRIMARY KEY IDENTITY(1,1),
  VisitID INT NOT NULL,
  IssueDateTime DATETIME NOT NULL,
  FOREIGN KEY (VisitID) REFERENCES Visits(VisitID)
);

CREATE TABLE PrescriptionItems (
  PrescriptionItemID INT PRIMARY KEY IDENTITY(1,1),
  PrescriptionID INT NOT NULL,
  DrugID INT NOT NULL,
  Dosage VARCHAR(50),
  Frequency VARCHAR(50),
  Duration_Days INT,
  Notes TEXT,
  FOREIGN KEY (PrescriptionID) REFERENCES Prescriptions(PrescriptionID),
  FOREIGN KEY (DrugID) REFERENCES Drugs(DrugID)
);

CREATE TABLE VisitServices (
  VisitServiceID INT PRIMARY KEY IDENTITY(1,1),
  VisitID INT NOT NULL,
  ServiceID INT NOT NULL,
  ExecutionDateTime DATETIME NULL,
  Quantity INT DEFAULT 1,
  IsAdmissionService BIT DEFAULT 0,
  FOREIGN KEY (VisitID) REFERENCES Visits(VisitID),
  FOREIGN KEY (ServiceID) REFERENCES Services(ServiceID)
);

--******************************************** Financial & Billing **************************************

CREATE TABLE InsuranceCompanies (
  InsuranceCoID INT PRIMARY KEY IDENTITY(1,1),
  CompanyName VARCHAR(80) UNIQUE NOT NULL
);

CREATE TABLE PatientInsurances (
  PatientInsuranceID INT PRIMARY KEY IDENTITY(1,1),
  PatientID INT NOT NULL,
  InsuranceCoID INT NOT NULL,
  PolicyNumber VARCHAR(50) NOT NULL,
  ExpiryDate DATE,
  IsActive BIT DEFAULT 1,
  FOREIGN KEY (PatientID) REFERENCES Patients(PatientID),
  FOREIGN KEY (InsuranceCoID) REFERENCES InsuranceCompanies(InsuranceCoID)
);

CREATE TABLE Invoices (
  InvoiceID INT PRIMARY KEY IDENTITY(1,1),
  VisitID INT NOT NULL,
  IssueDateTime DATETIME NOT NULL,
  PatientInsuranceID INT NULL,
  FOREIGN KEY (VisitID) REFERENCES Visits(VisitID),
  FOREIGN KEY (PatientInsuranceID) REFERENCES PatientInsurances(PatientInsuranceID)
);

CREATE TABLE InvoiceItems (
  LineItemID INT PRIMARY KEY IDENTITY(1,1),
  InvoiceID INT NOT NULL,
  VisitServiceID INT UNIQUE NOT NULL,
  UnitPrice DECIMAL(10,2),
  Quantity INT,
  InsuranceCoverage DECIMAL(12,2) DEFAULT 0.00,
  Notes VARCHAR(255),
  FOREIGN KEY (InvoiceID) REFERENCES Invoices(InvoiceID),
  FOREIGN KEY (VisitServiceID) REFERENCES VisitServices(VisitServiceID)
);

CREATE TABLE Payments (
  PaymentID INT PRIMARY KEY IDENTITY(1,1),
  InvoiceID INT NOT NULL,
  PaymentDateTime DATETIME NOT NULL,
  Amount DECIMAL(12,2) NOT NULL,
  PaymentMethod VARCHAR(20), -- Cash, Credit Card, Bank Transfer
  PayerType VARCHAR(20), -- Patient, InsuranceCompany
  ReferenceNumber VARCHAR(50),
  Notes TEXT,
  FOREIGN KEY (InvoiceID) REFERENCES Invoices(InvoiceID)
);

