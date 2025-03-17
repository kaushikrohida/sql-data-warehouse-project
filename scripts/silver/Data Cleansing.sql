-- Checking for nulls or duplicates in primary key
-- Expectation: No Result
-- Result: Having null and duplicate values

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Checking for unwanted spaces
-- Expectation: No Result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- Do the transformation and insert the data into bronze.crm_cust_info table

-- Quality check for silver.crm_cust_info table

-- Do the same steps for the remaining tables and insert it into silver layer tables




