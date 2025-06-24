-- This script creates the core tables for our application.

-- 1. The Policies table: Our master, canonical data store.
CREATE TABLE Policies (
    Policy_ID NVARCHAR(50) NOT NULL PRIMARY KEY, Product_Code NVARCHAR(50) NOT NULL,
    Valuation_Date DATE NOT NULL, Issue_Date DATE NOT NULL, Issue_Age INT NOT NULL,
    Gender CHAR(1) NOT NULL, Policy_Status_Code INT NOT NULL, Account_Value DECIMAL(18, 2) NOT NULL,
    Surrender_Charge_Schedule NVARCHAR(MAX), Guaranteed_Crediting_Rate DECIMAL(5, 4),
    Index_Strategy_Code NVARCHAR(50), Sub_Account_Allocation NVARCHAR(MAX),
    Rider_Codes NVARCHAR(255), GLWB_Benefit_Base DECIMAL(18, 2),
    GLWB_Withdrawal_Rate DECIMAL(5, 4), Load_Timestamp DATETIME DEFAULT GETDATE()
);

-- 2. The CalculationJobs table: A log to track every run.
CREATE TABLE CalculationJobs (
    JobID INT IDENTITY(1,1) PRIMARY KEY, Product_Code NVARCHAR(50) NOT NULL,
    AssumptionSetID NVARCHAR(50), Job_Status NVARCHAR(20) NOT NULL,
    Requested_Timestamp DATETIME DEFAULT GETDATE(), Completed_Timestamp DATETIME, UserID NVARCHAR(100) NOT NULL
);

-- 3. The Results table: Stores the final output of each calculation.
CREATE TABLE Results (
    ResultID INT IDENTITY(1,1) PRIMARY KEY,
    JobID INT FOREIGN KEY REFERENCES CalculationJobs(JobID),
    Result_Type NVARCHAR(50) NOT NULL, -- e.g., 'Deterministic_Reserve'
    Result_Value DECIMAL(18, 2) NOT NULL
);


-- New table to store the granular, monthly economic scenarios.
CREATE TABLE EconomicScenarios (
    ScenarioID INT NOT NULL,
    Month INT NOT NULL, -- This is the projection month (1 to 360)
    
    -- Yield Curve Rates for different terms (in years)
    Rate_0_25_yr DECIMAL(9, 8), -- The '0.25' column
    Rate_0_5_yr DECIMAL(9, 8),  -- The '0.5' column
    Rate_1_yr DECIMAL(9, 8),
    Rate_2_yr DECIMAL(9, 8),
    Rate_3_yr DECIMAL(9, 8),
    Rate_5_yr DECIMAL(9, 8),
    Rate_7_yr DECIMAL(9, 8),
    Rate_10_yr DECIMAL(9, 8),   -- This will be our primary discount rate
    Rate_20_yr DECIMAL(9, 8),
    Rate_30_yr DECIMAL(9, 8),
    
    Load_Timestamp DATETIME DEFAULT GETDATE(),

    -- Create a composite primary key to ensure each scenario-month is unique
    PRIMARY KEY (ScenarioID, Month)
);