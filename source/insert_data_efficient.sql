/*
================================================================================
اسکریپت تولید داده نمونه برای دیتابیس بیمارستان (hospital_DB)
================================================================================
توضیحات:
این اسکریپت برای پر کردن جداول با داده‌های نمونه طراحی شده است.
- جداول پایه (مثل استان، شهر، تخصص): تعداد محدودی رکورد دارند.
- جدول بیماران: ۱۰۰ بیمار نمونه.
- جداول تراکنشی (ویزیت، پذیرش، نسخه و...): ۱,۰۰۰,۰۰۰ ویزیت و داده‌های مرتبط.
- بازه زمانی تمام رویدادها: بین 2009/03/21 و 2015/01/01.
- ترتیب منطقی تاریخ‌ها رعایت شده است.

توجه: اجرای کامل این اسکریپت به دلیل حجم بالای داده ممکن است زمان‌بر باشد.
================================================================================
*/

-- استفاده از دیتابیس مورد نظر
USE Clinic_DB;
GO

-- غیرفعال کردن نمایش تعداد رکوردهای تحت تاثیر برای افزایش سرعت
SET NOCOUNT ON;
GO

--********************************************************************************
-- بخش ۱: ورود داده‌های پایه و کاتالوگ‌ها
-- این جداول معمولا داده‌های ثابتی دارند و پیش‌نیاز بقیه جداول هستند.
--********************************************************************************

PRINT 'در حال ورود داده‌های پایه (استان‌ها، شهرها، تخصص‌ها و...)'

-- Provinces
INSERT INTO Provinces (ProvinceName) VALUES
('تهران'), ('اصفهان'), ('فارس'), ('خراسان رضوی'), ('آذربایجان شرقی'), ('مازندران'), ('خوزستان'), ('کرمان'), ('یزد'), ('گیلان');

-- Cities
INSERT INTO Cities (ProvinceID, CityName) VALUES
(1, 'تهران'), (1, 'شهریار'), (1, 'اسلامشهر'),
(2, 'اصفهان'), (2, 'کاشان'), (2, 'خمینی شهر'),
(3, 'شیراز'), (3, 'مرودشت'),
(4, 'مشهد'), (4, 'نیشابور'),
(5, 'تبریز'), (5, 'مراغه'),
(6, 'ساری'), (6, 'بابل'),
(7, 'اهواز'), (7, 'آبادان'),
(8, 'کرمان'), (9, 'یزد'), (10, 'رشت');

-- Allergies
INSERT INTO Allergies (AllergyName, AllergyType) VALUES
('پنی‌سیلین', 'دارویی'), ('آسپرین', 'دارویی'), ('گرده گیاهان', 'محیطی'), ('گرد و غبار', 'محیطی'),
('شیر', 'غذایی'), ('تخم مرغ', 'غذایی'), ('بادام زمینی', 'غذایی'), ('نیش زنبور', 'حشرات'), ('لاتکس', 'تماسی'), ('فلزات', 'تماسی');

-- Diseases
INSERT INTO Diseases (ICD10_Code, DiseaseName, IsChronic) VALUES
('I10', 'فشار خون بالا', 1), ('E11', 'دیابت نوع دو', 1), ('J45', 'آسم', 1), ('J03', 'عفونت گلو', 0),
('A09', 'گاستروانتریت', 0), ('M54', 'کمردرد', 0), ('R51', 'سردرد', 0), ('I21', 'سکته قلبی', 0), ('C50', 'سرطان سینه', 1), ('F32', 'افسردگی', 1);

-- Drugs
INSERT INTO Drugs (DrugName, BrandName, Manufacturer, DosageForm) VALUES
('آتورواستاتین', 'لیپیتور', 'Pfizer', 'قرص'), ('متفورمین', 'گلوکوفاژ', 'Merck', 'قرص'), ('آملودیپین', 'نورواسک', 'Pfizer', 'قرص'),
('امپرازول', 'پریلوسک', 'AstraZeneca', 'کپسول'), ('سرتالین', 'زولوفت', 'Pfizer', 'قرص'), ('ایبوپروفن', 'ادویل', 'Generic', 'قرص'),
('استامینوفن', 'تایلنول', 'Generic', 'قرص'), ('آزیترومایسین', 'زیترومکس', 'Pfizer', 'قرص'), ('سالبوتامول', 'ونتولین', 'GSK', 'اسپری'), ('لوراتادین', 'کلاریتین', 'Bayer', 'قرص');

-- Services
INSERT INTO Services (ServiceName, ServiceCategory, BaseCost) VALUES
('ویزیت عمومی', 'ویزیت', 50000.00), ('ویزیت تخصصی', 'ویزیت', 85000.00), ('آزمایش خون کامل', 'آزمایشگاه', 120000.00),
('رادیوگرافی قفسه سینه', 'تصویربرداری', 95000.00), ('سونوگرافی شکم', 'تصویربرداری', 150000.00), ('نوار قلب', 'تشخیصی', 60000.00),
('تزریقات', 'پرستاری', 15000.00), ('پانسمان', 'پرستاری', 25000.00), ('یک شب بستری', 'بستری', 400000.00);

-- Specializations
INSERT INTO Specializations (SpecializationName) VALUES
('قلب و عروق'), ('داخلی'), ('اطفال'), ('زنان و زایمان'), ('ارتوپدی'), ('مغز و اعصاب'), ('گوش، حلق و بینی'), ('پوست'), ('عفونی'), ('جراحی عمومی');

-- InsuranceCompanies
INSERT INTO InsuranceCompanies (CompanyName) VALUES
('بیمه تامین اجتماعی'), ('بیمه خدمات درمانی'), ('بیمه ایران'), ('بیمه آسیا'), ('بیمه البرز'), ('بیمه دانا'), ('بیمه پارسیان'), ('بیمه کارآفرین'), ('بیمه سینا'), ('بیمه آتیه سازان حافظ');

-- Departments
INSERT INTO Departments (DepartmentName) VALUES
('داخلی'), ('اطفال'), ('اورژانس'), ('قلب'), ('زنان'), ('ارتوپدی'), ('آزمایشگاه'), ('تصویربرداری');

INSERT INTO VisitTypes (VisitTypeName)
VALUES
('Scheduled'), ('Emergency'), ('Follow-up'), ('Consultation'), ('Check-up');

GO

--********************************************************************************
-- بخش ۲: ورود داده‌های کارکنان و پزشکان
--********************************************************************************

PRINT 'در حال ورود داده‌های کارکنان و پزشکان...'

-- Staff
-- ابتدا کارکنان را بدون مدیر بخش وارد می‌کنیم
delete from Staff
INSERT INTO Staff (NationalCode, FirstName, LastName, Role, DepartmentID, HireDate, IsActive, Email, DateOfBirth, Gender) VALUES
-- مدیران
('1111111111', 'علی', 'محمدی', 'Admin', 1, '2008-01-10', 1, '1@gmail.com', '1980-01-10', 'M'),
('2222222222', 'سارا', 'احمدی', 'Admin', 2, '2008-02-15', 1, '2@gmail.com', '1970-01-10', 'F'),
-- پزشکان
('3333333333', 'رضا', 'کریمی', 'Doctor', 1, '2009-05-20', 1, '3@gmail.com', '1975-01-10', 'M'),
('4444444444', 'مریم', 'صادقی', 'Doctor', 5, '2010-07-22', 1, '4@gmail.com', '1981-01-10', 'F'),
('5555555555', 'حسن', 'حسینی', 'Doctor', 3, '2011-09-11', 1, '5@gmail.com', '1971-01-10', 'M'),
('6666666666', 'فاطمه', 'جعفری', 'Doctor', 7, '2012-11-30', 1, '6@gmail.com', '1979-01-10', 'F'),
('7777777777', 'مهدی', 'کاظمی', 'Doctor', 4, '2009-03-01', 1, '7@gmail.com', '1978-01-10', 'M'),
('8888888888', 'زهرا', 'قاسمی', 'Doctor', 2, '2013-01-15', 1, '8@gmail.com', '1968-01-10', 'F'),
-- پرستاران و سایر
('9999999999', 'نیما', 'نادری', 'Nurse', 4, '2010-04-10', 1, '9@gmail.com', '1982-01-10', 'M'),
('1010101010', 'لیلا', 'ابراهیمی', 'Technician', 8, '2011-06-05', 1, '10@gmail.com', '1977-01-10', 'F'),
('1212121212', 'پریسا', 'مرادی', 'Nurse', 1, '2012-08-19', 1, '11@gmail.com', '1976-01-10', 'F'),
('1313131313', 'سعید', 'شریفی', 'Admin', 8, '2013-10-25', 1, '12@gmail.com', '1975-01-10', 'M');

-- به‌روزرسانی مدیران بخش‌ها
UPDATE Departments SET ManagerStaffID = (SELECT StaffID FROM Staff WHERE NationalCode = '1111111111') WHERE DepartmentName = 'داخلی';
UPDATE Departments SET ManagerStaffID = (SELECT StaffID FROM Staff WHERE NationalCode = '9999999999') WHERE DepartmentName = 'قلب';
GO

-- Doctors
INSERT INTO Doctors (DoctorStaffID, MedicalLicenseNumber, SpecializationID, YearsOfExperience) VALUES
((SELECT StaffID FROM Staff WHERE NationalCode = '3333333333'), 'D333', 2, 10),
((SELECT StaffID FROM Staff WHERE NationalCode = '4444444444'), 'D444', 1, 8),
((SELECT StaffID FROM Staff WHERE NationalCode = '5555555555'), 'D555', 3, 7),
((SELECT StaffID FROM Staff WHERE NationalCode = '6666666666'), 'D666', 5, 6),
((SELECT StaffID FROM Staff WHERE NationalCode = '7777777777'), 'D777', 2, 11),
((SELECT StaffID FROM Staff WHERE NationalCode = '8888888888'), 'D888', 10, 5);
GO

--********************************************************************************
-- بخش ۳: ورود داده‌های بیماران (۱۰۰ نفر)
--********************************************************************************
PRINT 'در حال ورود داده‌های ۱۰۰ بیمار نمونه...';
DECLARE @regStartDate DATETIME = '2009-03-21';
DECLARE @regEndDate DATETIME = '2010-03-21';
-- ایجاد جداول موقت برای نام‌ها برای تولید داده رندوم
CREATE TABLE #FirstNames (Name VARCHAR(50), Gendr CHAR(1));
INSERT INTO #FirstNames VALUES ('محمد', 'M'), ('علی', 'M'), ('حسین', 'M'), ('امیر', 'M'), ('رضا', 'M'), ('مهدی', 'M'), ('حسن', 'M'),
								('سعید', 'M'), ('پویا', 'M'), ('آرش', 'M'), ('فاطمه', 'F'), ('مریم', 'F'),
								('زهرا', 'F'), ('مهسا', 'F'), ('نرگس', 'F'), ('ریحانه', 'F'), ('هستی', 'F'), ('مرضیه', 'F'), ('لیلا', 'F');
CREATE TABLE #LastNames (Name VARCHAR(50));
INSERT INTO #LastNames VALUES ('اکبری'), ('محمدی'), ('احمدی'), ('صادقی'), ('کریمی'), ('حسینی'), ('جعفری'), ('کاظمی'), ('قاسمی'), ('نادری');

DECLARE @PatientCounter INT = 1;
WHILE @PatientCounter <= 200
BEGIN
    DECLARE @FirstName VARCHAR(50) = (SELECT TOP 1 Name FROM #FirstNames ORDER BY NEWID());
	DECLARE @Gender char(1) = (select Gendr from #FirstNames where Name = @FirstName)
    DECLARE @LastName VARCHAR(50) = (SELECT TOP 1 Name FROM #LastNames ORDER BY NEWID());
    DECLARE @FatherName VARCHAR(50) = (SELECT TOP 1 Name FROM #FirstNames where Gendr='M' ORDER BY NEWID());
    DECLARE @NationalCode VARCHAR(10) = CAST(ABS(CHECKSUM(NEWID())) % 9000000000 + 1000000000 AS VARCHAR(10));
    DECLARE @DOB DATE = DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 30000 + 1000), GETDATE());
    DECLARE @PhoneNumber VARCHAR(20) = '09' + CAST(ABS(CHECKSUM(NEWID())) % 900000000 + 100000000 AS VARCHAR(9));
	DECLARE @RegistrationDate DATETIME = DATEADD(SECOND, ABS(CHECKSUM(NEWID())) % (DATEDIFF(SECOND, @regStartDate, @regEndDate)), @regStartDate);



    INSERT INTO Patients (NationalCode, FirstName, LastName, FatherName, DateOfBirth, PhoneNumber, Gender, MaritalStatus, BloodType)
    VALUES (@NationalCode, @FirstName, @LastName, @FatherName, @DOB, @PhoneNumber, @Gender, 
            CASE WHEN RAND() > 0.5 THEN 'Married' ELSE 'Single' END,
            CHOOSE(ABS(CHECKSUM(NEWID())) % 8 + 1, 'A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-'));

    DECLARE @NewPatientID INT = SCOPE_IDENTITY();
    
    -- افزودن آدرس برای بیمار
    INSERT INTO PatientAddresses (PatientID, CityID, AddressLine, IsPrimary)
    VALUES (@NewPatientID, (SELECT TOP 1 CityID FROM Cities ORDER BY NEWID()), 'خیابان اصلی، کوچه فرعی، پلاک ' + CAST(ABS(CHECKSUM(NEWID())) % 100 + 1 AS VARCHAR), 1);

    -- افزودن بیمه برای بیمار
    INSERT INTO PatientInsurances (PatientID, InsuranceCoID, PolicyNumber, ExpiryDate, IsActive)
    VALUES (@NewPatientID, (SELECT TOP 1 InsuranceCoID FROM InsuranceCompanies ORDER BY NEWID()), CAST(ABS(CHECKSUM(NEWID())) AS VARCHAR(20)), DATEADD(YEAR, 1, GETDATE()), 1);

	
    -- افزودن اطلاعات تماس برای بیمار
    INSERT INTO PatientContacts (PatientID, FullName, Relationship, PhoneNumber, IsEmergencyContact)
    VALUES (@NewPatientID, 
            (SELECT TOP 1 Name FROM #FirstNames ORDER BY NEWID()) + ' ' + (SELECT TOP 1 Name FROM #LastNames ORDER BY NEWID()),
            CHOOSE(ABS(CHECKSUM(NEWID())) % 4 + 1, 'همسر', 'فرزند', 'پدر', 'دوست'),
            '09' + CAST(ABS(CHECKSUM(NEWID())) % 900000000 + 100000000 AS VARCHAR(9)),
            1);

    -- افزودن حساسیت برای بیمار (با احتمال 20%)
    IF RAND() < 0.2
    BEGIN
        INSERT INTO PatientAllergies (PatientID, AllergyID, Severity)
        VALUES (@NewPatientID, (SELECT TOP 1 AllergyID FROM Allergies ORDER BY NEWID()), CHOOSE(ABS(CHECKSUM(NEWID())) % 3 + 1, 'خفیف', 'متوسط', 'شدید'));
    END

    -- افزودن سابقه بیماری برای بیمار (با احتمال 30%)
    IF RAND() < 0.3
    BEGIN
        DECLARE @DiagnosisDate DATE = DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 1000 + 1), @RegistrationDate); -- تاریخ تشخیص تا 3 سال قبل ثبت نام
        INSERT INTO PatientMedicalHistory (PatientID, DiseaseID, DiagnosisDate)
        VALUES (@NewPatientID, (SELECT TOP 1 DiseaseID FROM Diseases ORDER BY NEWID()), @DiagnosisDate);
    END

    SET @PatientCounter = @PatientCounter + 1;
END

DROP TABLE #FirstNames;
DROP TABLE #LastNames;
GO

/*
================================================================================
اسکریپت بهینه (Set-Based) برای تولید داده‌های وابسته (سازگار با شِمای جدید)
================================================================================
عملکرد:
این اسکریپت ابتدا جدول `Visits` را با استفاده از یک روش سریع و مبتنی بر
مجموعه با ۱ میلیون رکورد پر می‌کند. سپس، تمام جداول وابسته را نیز
به صورت Set-Based تولید می‌کند.

نحوه استفاده:
1. ابتدا جداول پایه (Patients, Doctors, Services, VisitTypes و...) را ایجاد
   و با داده‌های اولیه پر کنید.
2. سپس کل این اسکریپت را یکجا اجرا نمایید.
================================================================================
*/
USE Clinic_DB;
GO
SET NOCOUNT ON;
GO

PRINT 'شروع فرآیند تولید داده‌های وابسته به صورت Set-Based...';

--********************************************************************************
-- بخش جدید و بهینه: تولید ۱ میلیون ویزیت به صورت Set-Based
--********************************************************************************
PRINT '*. در حال تولید ۱ میلیون رکورد برای جدول Visits...';

-- حذف و ایجاد مجدد جدول برای اطمینان از خالی بودن آن
IF (OBJECT_ID('Visits') IS NOT NULL)
BEGIN
    DELETE FROM Visits;
    DBCC CHECKIDENT ('Clinic_DB.dbo.Visits', RESEED, 0);
END

-- تعریف بازه زمانی
DECLARE @StartDate DATETIME = '2010-03-21';
DECLARE @EndDate DATETIME = '2013-01-01';
DECLARE @SecondsRange INT = DATEDIFF(SECOND, @StartDate, @EndDate);
DECLARE @TotalVisits INT = 1000000;

-- تولید ۱ میلیون ویزیت به صورت یکجا
-- از یک CTE برای تولید شماره ردیف‌های مورد نیاز استفاده می‌کنیم
;WITH
Numbers AS (
    SELECT TOP (@TotalVisits) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
    FROM sys.all_objects o1, sys.all_objects o2
),
-- آماده‌سازی داده‌های منبع با شماره ردیف برای اتصال تصادفی
PatientsWithRN AS (
    SELECT PatientID, ROW_NUMBER() OVER (ORDER BY PatientID) as rn FROM Patients
),
DoctorsWithRN AS (
    SELECT DoctorStaffID, ROW_NUMBER() OVER (ORDER BY DoctorStaffID) as rn FROM Doctors
),
DepartmentsWithRN AS (
    SELECT DepartmentID, ROW_NUMBER() OVER (ORDER BY DepartmentID) as rn FROM Departments WHERE DepartmentName NOT IN ('آزمایشگاه', 'تصویربرداری', 'داروخانه')
),
VisitTypesWithRN AS (
    SELECT VisitTypeID, ROW_NUMBER() OVER (ORDER BY VisitTypeID) as rn FROM VisitTypes
)
INSERT INTO Visits (PatientID, DoctorStaffID, VisitDateTime, VisitTypeID, DepartmentID)
SELECT
    p.PatientID,
    d.DoctorStaffID,
    DATEADD(SECOND, ABS(CHECKSUM(NEWID()) + n.rn) % @SecondsRange, @StartDate),
    vt.VisitTypeID,
    dep.DepartmentID
FROM Numbers n
JOIN PatientsWithRN p ON p.rn = (n.rn % (SELECT COUNT(*) FROM PatientsWithRN)) + 1
JOIN DoctorsWithRN d ON d.rn = (n.rn % (SELECT COUNT(*) FROM DoctorsWithRN)) + 1
JOIN DepartmentsWithRN dep ON dep.rn = (n.rn % (SELECT COUNT(*) FROM DepartmentsWithRN)) + 1
JOIN VisitTypesWithRN vt ON vt.rn = (n.rn % (SELECT COUNT(*) FROM VisitTypesWithRN)) + 1;

PRINT 'تولید داده ویزیت با موفقیت به پایان رسید.';

--********************************************************************************
-- بخش ۱: تولید داده‌های بالینی (Vitals, Prescriptions, Admissions)
--********************************************************************************
PRINT '1. در حال تولید Vitals, Prescriptions و Admissions...';

-- ثبت علائم حیاتی برای تمام ویزیت‌ها
INSERT INTO Vitals (VisitID, RecordDateTime, Height_cm, Weight_kg, BloodPressure_Systolic, BloodPressure_Diastolic, HeartRate_bpm, Temperature_Celsius)
SELECT
    VisitID,
    VisitDateTime,
    150 + RAND(CHECKSUM(NEWID())) * 40,
    50 + RAND(CHECKSUM(NEWID())) * 60,
    100 + RAND(CHECKSUM(NEWID())) * 40,
    70 + RAND(CHECKSUM(NEWID())) * 30,
    60 + RAND(CHECKSUM(NEWID())) * 40,
    36.5 + RAND(CHECKSUM(NEWID())) * 2
FROM Visits;

-- تولید نسخه‌ها برای حدود 70% ویزیت‌ها
DECLARE @PrescriptionsOutput TABLE (PrescriptionID INT, VisitID INT);
INSERT INTO Prescriptions (VisitID, IssueDateTime)
OUTPUT inserted.PrescriptionID, inserted.VisitID
INTO @PrescriptionsOutput
SELECT VisitID, VisitDateTime FROM Visits WHERE VisitID % 10 < 7;

-- تولید اقلام نسخه (1 تا 3 قلم برای هر نسخه)
INSERT INTO PrescriptionItems (PrescriptionID, DrugID, Dosage, Frequency, Duration_Days)
SELECT p.PrescriptionID, d.DrugID, '1 عدد', 'هر 8 ساعت', 7
FROM @PrescriptionsOutput p
CROSS JOIN (VALUES (1), (2), (3)) AS ItemNumbers(n)
CROSS APPLY (
    SELECT TOP 1 DrugID 
    FROM Drugs 
    ORDER BY ABS(CAST(CHECKSUM(NEWID()) AS BIGINT) + p.VisitID + ItemNumbers.n)) AS d
WHERE ItemNumbers.n <= (1 + ABS(CAST(CHECKSUM(NEWID()) AS BIGINT) + p.VisitID) % 3);

-- تولید پذیرش برای حدود 10% ویزیت‌ها
DECLARE @AdmissionsOutput TABLE (AdmissionID INT, VisitID INT, AdmissionDateTime DATETIME, DischargeDateTime DATETIME);
INSERT INTO Admissions (VisitID, AdmissionDateTime, DischargeDateTime, DepartmentID, RoomNumber, BedNumber)
OUTPUT inserted.AdmissionID, inserted.VisitID, inserted.AdmissionDateTime, inserted.DischargeDateTime
INTO @AdmissionsOutput
SELECT
    v.VisitID, v.VisitDateTime,
    DATEADD(DAY, 1 + (ABS(CHECKSUM(NEWID())) % 10), v.VisitDateTime), -- تاریخ ترخیص 1 تا 10 روز بعد
    v.DepartmentID,
    CAST(100 + (v.VisitID % 50) AS VARCHAR),
    CAST(1 + (v.VisitID % 2) AS VARCHAR)
FROM Visits v
WHERE v.VisitID % 10 = 0; -- انتخاب 10% رکوردها

--********************************************************************************
-- بخش ۲: تولید خدمات و صورتحساب‌ها (مهم‌ترین بخش)
--********************************************************************************
PRINT '2. در حال تولید VisitServices, Invoices و InvoiceItems...';

-- مرحله الف: تمام خدمات (ویزیت، خدمات جانبی، بستری) را در یک جدول موقت جمع‌آوری می‌کنیم
DECLARE @AllServicesToInsert TABLE (VisitID INT, ServiceID INT, Quantity INT, ExecutionDateTime DATETIME, IsAdmissionService BIT);

-- 1. خدمت اصلی "ویزیت تخصصی" برای تمام ویزیت‌ها
INSERT INTO @AllServicesToInsert (VisitID, ServiceID, Quantity, ExecutionDateTime, IsAdmissionService)
SELECT v.VisitID, (SELECT ServiceID FROM Services WHERE ServiceName = 'ویزیت تخصصی'), 1, v.VisitDateTime, 0
FROM Visits v;

-- 2. خدمات جانبی (آزمایش/تصویربرداری) برای حدود 30% ویزیت‌ها
INSERT INTO @AllServicesToInsert (VisitID, ServiceID, Quantity, ExecutionDateTime, IsAdmissionService)
SELECT v.VisitID, s.ServiceID, 1, v.VisitDateTime, 0
FROM Visits v
CROSS JOIN (
    SELECT ServiceID, ROW_NUMBER() OVER(ORDER BY ServiceID) as rn 
    FROM Services WHERE ServiceCategory IN ('آزمایشگاه', 'تصویربرداری')
) s
WHERE (v.VisitID + s.rn) % 3 = 0;

-- 3. خدمات بستری برای ویزیت‌هایی که پذیرش شده‌اند
INSERT INTO @AllServicesToInsert (VisitID, ServiceID, Quantity, ExecutionDateTime, IsAdmissionService)
SELECT a.VisitID, (SELECT ServiceID FROM Services WHERE ServiceName = 'یک شب بستری'), 
       DATEDIFF(DAY, a.AdmissionDateTime, a.DischargeDateTime), a.AdmissionDateTime, 1
FROM @AdmissionsOutput a
WHERE DATEDIFF(DAY, a.AdmissionDateTime, a.DischargeDateTime) > 0;

-- مرحله ب: خدمات جمع‌آوری شده را در جدول VisitServices درج کرده و ID های جدید را ذخیره می‌کنیم
DECLARE @GeneratedVisitServices TABLE (VisitServiceID INT, VisitID INT);
INSERT INTO VisitServices (VisitID, ServiceID, Quantity, ExecutionDateTime, IsAdmissionService)
OUTPUT inserted.VisitServiceID, inserted.VisitID INTO @GeneratedVisitServices
SELECT VisitID, ServiceID, Quantity, ExecutionDateTime, IsAdmissionService
FROM @AllServicesToInsert;

-- مرحله ج: صورتحساب‌ها را برای تمام ویزیت‌ها ایجاد می‌کنیم
DECLARE @InvoicesOutput TABLE (InvoiceID INT, VisitID INT);
INSERT INTO Invoices (VisitID, IssueDateTime, PatientInsuranceID)
OUTPUT inserted.InvoiceID, inserted.VisitID INTO @InvoicesOutput
SELECT v.VisitID, v.VisitDateTime, pi.PatientInsuranceID
FROM Visits v
CROSS APPLY (SELECT TOP 1 PatientInsuranceID FROM PatientInsurances WHERE PatientID = v.PatientID) AS pi;

-- مرحله د: اقلام صورتحساب را با اتصال خدمات به صورتحساب‌ها ایجاد می‌کنیم
INSERT INTO InvoiceItems(InvoiceID, VisitServiceID, UnitPrice, Quantity)
SELECT
    i.InvoiceID,
    gvs.VisitServiceID,
    s.BaseCost,
    vs.Quantity
FROM @InvoicesOutput i
JOIN @GeneratedVisitServices gvs ON i.VisitID = gvs.VisitID
JOIN VisitServices vs ON gvs.VisitServiceID = vs.VisitServiceID
JOIN Services s ON vs.ServiceID = s.ServiceID;

--********************************************************************************
-- بخش ۳: تولید پرداخت‌ها
--********************************************************************************
PRINT '3. در حال تولید Payments...';

-- برای 95% صورتحساب‌ها پرداخت ثبت می‌کنیم
INSERT INTO Payments (InvoiceID, PaymentDateTime, Amount, PaymentMethod, PayerType)
SELECT
    i.InvoiceID,
    DATEADD(HOUR, 1 + (ABS(CHECKSUM(NEWID())) % 150), inv.IssueDateTime), -- تاریخ پرداخت 1 تا 150 ساعت بعد از صدور
    ISNULL(Total.Amount, 50000),
    CHOOSE(1 + (i.InvoiceID % 3), 'Cash', 'Credit Card', 'Bank Transfer'),
    'Patient'
FROM @InvoicesOutput i
JOIN Invoices inv ON i.InvoiceID = inv.InvoiceID
LEFT JOIN (
    SELECT InvoiceID, SUM(UnitPrice * Quantity) AS Amount
    FROM InvoiceItems
    GROUP BY InvoiceID
) AS Total ON i.InvoiceID = Total.InvoiceID
WHERE i.InvoiceID % 20 <> 0; -- انتخاب 95% رکوردها


GO
PRINT 'تولید تمام داده‌های وابسته با موفقیت به پایان رسید.';
GO
SET NOCOUNT OFF;
GO


SELECT * FROM Patients;
SELECT * FROM PatientContacts;
SELECT * FROM Provinces;
SELECT * FROM Cities;
SELECT * FROM PatientAddresses;

SELECT * FROM Allergies;
SELECT * FROM PatientAllergies;
SELECT * FROM Diseases;
SELECT * FROM PatientMedicalHistory;
SELECT * FROM Drugs;
SELECT * FROM Services;

SELECT * FROM Departments;
SELECT * FROM Staff;
SELECT * FROM Doctors;
SELECT * FROM Specializations;

SELECT top 100 * FROM Visits;
SELECT top 100 * FROM Vitals;
SELECT top 100 * FROM Admissions;
SELECT top 100 * FROM Prescriptions;
SELECT top 100 * FROM PrescriptionItems;
SELECT top 100 * FROM VisitServices;

SELECT * FROM InsuranceCompanies;
SELECT * FROM PatientInsurances;
SELECT top 100 * FROM Invoices;
SELECT top 100 * FROM InvoiceItems;
SELECT top 100 * FROM Payments;

SELECT count(*) FROM Visits;
SELECT count(*) FROM Vitals;
SELECT count(*) FROM Admissions;
SELECT count(*) FROM Prescriptions;
SELECT count(*) FROM PrescriptionItems;
SELECT count(*) FROM VisitServices;
SELECT count(*) FROM Invoices;
SELECT count(*) FROM InvoiceItems;
SELECT count(*) FROM Payments;