USE Clinic_SA
GO


--TEST SA

EXEC dbo.ETL_Main_SA


select * from dbo.Payments_sa order by PaymentID;
select * from dbo.InvoiceItems_sa;
select * from dbo.PatientInsurances_sa;
select * from dbo.InsuranceCompanies_sa;
select * from dbo.VisitServices_sa order by VisitServiceID;
select * from dbo.PrescriptionItems_sa;
select * from dbo.Admissions_sa;
select * from dbo.Vitals_sa;
select * from dbo.Visits_sa;
select * from dbo.VisitTypes_sa;
select * from dbo.Doctors_sa;
select * from dbo.Specializations_sa;
select * from dbo.Staff_sa;
select * from dbo.Departments_sa;
select * from dbo.Services_sa;
select * from dbo.Drugs_sa;
select * from dbo.PatientMedicalHistory_sa;
select * from dbo.Diseases_sa;
select * from dbo.PatientAllergies_sa;
select * from dbo.Allergies_sa;
select * from dbo.PatientAddresses_sa;
select * from dbo.Cities_sa;
select * from dbo.Provinces_sa;
select * from dbo.Patients_sa;

select * from [Clinic_DW].[dbo].[Log] order by date_affected desc


--============================================================================
use Clinic_DW 
go

exec ETL_Main_FirstLoad_DW


select * from [dbo].[Dim_Date]
select * from [dbo].[Dim_Time]
select * from [dbo].[Dim_Patient]
select * from [dbo].[Dim_Service]
select * from [dbo].[Dim_Department]
select * from [dbo].[Dim_Insurance]
select * from [dbo].[Dim_DoctorStaff]
select * from [dbo].[Dim_Disease]
select * from [dbo].[Dim_VisitType]
select * from [dbo].[Log] order by date_affected desc



use Clinic_DB
go

UPDATE Departments SET ManagerStaffID = (SELECT StaffID FROM Staff WHERE NationalCode = '1212121212') WHERE DepartmentName = 'داخلی';
update Staff set DepartmentID = 1 where NationalCode = '1010101010';
update Services set BaseCost = 70000.00 where ServiceID = 1;


use Clinic_DW
go 

--WARNING ALL DATA WILL BE GONE 
exec ETL_Main_FirstLoad_DW

exec ETL_Main_DailyLoad_DW

--exec FirstLoad_ETL_Factless_Patient_Insurance
--exec FirstLoad_ETL_Factless_Patient_MedicalHistory

--exec FirstFill_Fact_Transaction_Visit
--exec Fill_Fact_Daily_Patient_FirstLoad
--exec First_Fill_FactAccPatient

--exec FirstLoad_ETL_Fact_Transaction_Service
--exec FirstLoad_ETL_Fact_Daily_Service
--exec First_Fill_FactAccService

select top 100 * from [dbo].[Fact_Transaction_Visit]
select top 100 * from [dbo].[Fact_Transaction_Service]
select * from [dbo].[Fact_Daily_Patient] order by DateKey desc , PatientID
select * from [dbo].[Fact_Daily_Service] order by DateKey desc, ServiceID
select * from [dbo].[Fact_ACC_Patient]
select * from [dbo].[Fact_ACC_Service]
select * from [dbo].[Factless_Patient_Insurance]
select * from [dbo].[Factless_Patient_MedicalHistory]

select * from Clinic_DB.dbo.PatientInsurances
select * from Clinic_DB.dbo.PatientMedicalHistory
select* from Clinic_DB.dbo.Visits v1 join Clinic_DB.dbo.VisitServices v2
on v1.VisitID = v2.VisitID
where v2.IsAdmissionService = 1

select sum(f.Quantity*f.UnitCost) from Fact_Transaction_Service f
where ServiceID= 2 and DateKey = 20110101

select * from Clinic_DB.dbo.Services

select * from [dbo].[Fact_ACC_Service]

select* from Fact_Daily_Service order by DateKey desc ,ServiceID
