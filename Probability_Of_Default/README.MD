
# Risk Score Prediction Project

**Objective:** Develop a machine learning model to predict loan risk (RiskScore) based on applicant financial and demographic data.

**Project File:** `Riskscore.ipynb` (contains the complete project, including data processing, model training, and evaluation).

**Data Features:**

* **Personal/Demographic:** ApplicationDate, Age, MaritalStatus, NumberOfDependents.
* **Employment/Education:** EmploymentStatus, EducationLevel, Experience, JobTenure.
* **Income/Assets:** AnnualIncome, MonthlyIncome, SavingsAccountBalance, CheckingAccountBalance, TotalAssets, NetWorth.
* **Financial Obligations/Debt:** LoanAmount, LoanDuration, MonthlyLoanPayment, TotalLiabilities, MonthlyDebtPayments, CreditCardUtilizationRate, NumberOfOpenCreditLines, NumberOfCreditInquiries, DebtToIncomeRatio, TotalDebtToIncomeRatio, UtilityBillsPaymentHistory, BankruptcyHistory, PreviousLoanDefaults.
* **Credit Profile:** CreditScore, PaymentHistory, LengthOfCreditHistory.
* **Loan Details:** LoanPurpose, BaseInterestRate, InterestRate.
* **Risk/Approval:** LoanApproved (target), RiskScore (predicted).

**Process Overview (within `Riskscore.ipynb`):**

1.  **Data Preparation:**
    * Separate features (X) from labels (y).
    * Encode categorical features.
    * Perform a train-test split.
    * Scale numerical features for Linear Regression.
2.  **Linear Regression Model:**
    * Train a Linear Regression model.
    * Evaluate performance using:
        * R² score
        * Mean Absolute Error (MAE)
        * Mean Squared Error (MSE)
        * Root Mean Squared Error (RMSE)
3.  **Feature Importance:**
    * Perform feature importance analysis using Random Forest.
4.  **Model Improvement:**
    * Hyperparameter tuning.
    * Calculate Probability of Default.
    * Feature Scaling.
5.  **Visualization and Analysis:**
    * Visualize high-risk clients.
    * Calculate ROC AUC Score.

**Key Factors Influencing Risk Score (identified via feature importance):**

* DebtToIncomeRatio
* CreditScore
* MonthlyDebtPayments
* NetWorth
* SavingsAccountBalance
* PaymentHistory
* BankruptcyHistory
* LoanAmount
* NumberOfCreditInquiries
* CreditCardUtilizationRate

**Experiments:**

* TensorFlow and other machine learning experiments are located in separate files.
* `Riskscore.ipynb` contains the final and best performing model.

**Usage:**

* Run `Riskscore.ipynb` to reproduce the model, predictions, and analysis.
