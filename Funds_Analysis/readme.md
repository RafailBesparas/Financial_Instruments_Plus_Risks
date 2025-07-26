# Funding Risk Analysis – EDA Project

This project explores refinancing risk in financial instruments using a dataset of institutional funding deals. The goal is to uncover patterns that influence whether a funding instrument is likely to require refinancing.

## 📊 Key Objectives
- Understand the structure of the funding dataset
- Explore relationships between interest spread, maturity, and funding amount
- Test hypotheses related to refinancing risk
- Create visual insights to support treasury and risk decision-making

## 🧪 Techniques Used
- Data cleaning and feature engineering (buckets, z-scores)
- Statistical tests (t-test, chi-squared)
- Visualization with seaborn and matplotlib
- Grouped analysis by funding source, institution, and currency

## 📁 Files Included
- `EDA_Funding_Risks.ipynb` – Jupyter notebook with full EDA process
- `Feature_Engineered_Funding_Data.csv` – Cleaned dataset with derived features
- `Funding_Risk_EDA_Report.md` – Structured report summarizing findings

## 📌 Summary of Findings
- No single variable significantly predicted refinancing need
- Multivariate modeling is recommended for deeper insights
- Segmenting funding by source, size, and currency helps identify risk clusters
