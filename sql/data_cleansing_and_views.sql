/* ==============================================================================
Script Name: 01_sattva_foods_transformations.sql
Project:     Sattva Foods - Primary & Secondary Sales Analytics
Author:      Swastik
Date:        April 1, 2026

Description: 
-This script handles the foundational data cleaning, transformation, and schema definition for the Sattva Foods Power BI dashboard. 
-It processes raw messy data injected with real-world inconsistencies and creates the final structured Views (and CTAS tables) required.
============================================================================== */

-- date dimension table CTAS creation
CREATE TABLE dim_date AS
WITH date_series AS (
SELECT
	generate_series(
        '2023-01-01'::DATE,
        '2026-12-31'::DATE,
        '1 day'::INTERVAL
    )::DATE AS full_date
)
SELECT
	full_date AS date_key,
	DATE_TRUNC('month', full_date)::DATE AS month_start_date,
	EXTRACT(YEAR FROM full_date)::INT AS YEAR,
	EXTRACT(MONTH FROM full_date)::INT AS MONTH,
	-- Indian FY: Apr-Mar. FY start year = calendar year if month >= 4, else year - 1
    CASE
		WHEN EXTRACT(MONTH FROM full_date) >= 4
        THEN CONCAT('FY', EXTRACT(YEAR FROM full_date)::INT, '-',
                    RIGHT((EXTRACT(YEAR FROM full_date)::INT + 1)::TEXT, 2))
		ELSE CONCAT('FY', (EXTRACT(YEAR FROM full_date)::INT - 1)::TEXT, '-',
                    RIGHT(EXTRACT(YEAR FROM full_date)::INT::TEXT, 2))
	END AS fiscal_year,
	-- Indian FY quarter: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
    CASE
		WHEN EXTRACT(MONTH FROM full_date) IN (4, 5, 6) THEN 'Q1'
		WHEN EXTRACT(MONTH FROM full_date) IN (7, 8, 9) THEN 'Q2'
		WHEN EXTRACT(MONTH FROM full_date) IN (10, 11, 12) THEN 'Q3'
		WHEN EXTRACT(MONTH FROM full_date) IN (1, 2, 3) THEN 'Q4'
	END AS fiscal_quarter,
	-- Format: Apr-2023
	TO_CHAR(full_date, 'Mon-YYYY') AS month_name,
	EXTRACT(ISODOW FROM full_date)::INT AS day_of_week,
	TRIM(TO_CHAR(full_date, 'Day')) AS day_name,
	-- Working day: Mon(1) through Sat(6). Sunday(7) = not working
    CASE
		WHEN EXTRACT(ISODOW FROM full_date) = 7 THEN FALSE
		ELSE TRUE
	END AS is_working_day,
	-- Weekend flag retained for reference
    CASE
		WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7) THEN TRUE
		ELSE FALSE
	END AS is_weekend
FROM
	date_series;

-- Row count
SELECT
	COUNT(*)
FROM
	dim_date;
-- 1461

-- Fiscal year boundary check
SELECT
	DISTINCT fiscal_year
FROM
	dim_date
ORDER BY
	fiscal_year;
-- FY2022-23, FY2023-24, FY2024-25, FY2025-26, FY2026-27

-- Fiscal quarter logic check
SELECT
	date_key,
	fiscal_year,
	fiscal_quarter
FROM
	dim_date
WHERE
	date_key IN ('2023-04-01', '2023-07-01', '2023-10-01', '2024-01-01');
-- Q1, Q2, Q3, Q4 respectively

-- Month name format check
SELECT
	DISTINCT month_start_date,
	month_name
FROM
	dim_date
ORDER BY
	month_start_date;
-- Apr-2023, May-2023 ... Dec-2026 

-- Working day check
SELECT
	date_key,
	day_name,
	is_working_day
FROM
	dim_date
WHERE
	EXTRACT(ISODOW FROM date_key) = 7
LIMIT 5;
-- all is_working_day = false

-- Null check
SELECT
	COUNT(*)
FROM
	dim_date
WHERE
	date_key IS NULL
	OR fiscal_year IS NULL
	OR fiscal_quarter IS NULL
	OR month_start_date IS NULL;
-- 0

-- dim_date view creation
CREATE OR REPLACE
VIEW vw_dim_date AS
SELECT
	*
FROM
	dim_date;

-- row count check
SELECT
	COUNT(*)
FROM
	vw_dim_date;
-- 1461
------------------------------------------------------------------------------------------------------------------------------
-- sku dimension table CTAS creation
CREATE TABLE dim_sku AS
SELECT
	TRIM(sku_id) AS sku_id,
	TRIM(product_name) AS product_name,
	TRIM(category) AS category,
	TRIM(pack_size) AS pack_size,
	CAST(cost_price AS NUMERIC(10, 2)) AS cost_price,
	CAST(distributor_price AS NUMERIC(10, 2)) AS distributor_price,
	CAST(retailer_price AS NUMERIC(10, 2)) AS retailer_price,
	CAST(mrp AS NUMERIC(10, 2)) AS mrp,
	TRIM(launch_fy) AS launch_fy,
	CAST(units_per_carton AS INT) AS units_per_carton
FROM
	raw_dim_sku;

-- The Primary Key Uniqueness Check
SELECT
	sku_id,
	COUNT(*)
FROM
	dim_sku
GROUP BY
	sku_id
HAVING
	COUNT(*) > 1;

-- The Row Count Check
SELECT
	COUNT(*)
	-- 18
FROM
	dim_sku;

-- The Pricing Logic Check
SELECT
	*
FROM
	dim_sku
WHERE
	cost_price >= distributor_price
	OR distributor_price >= retailer_price
	OR retailer_price >= mrp;

-- null check on critical columns
SELECT COUNT(*) FROM dim_sku
WHERE sku_id IS NULL
   OR product_name IS NULL
   OR cost_price IS NULL
   OR distributor_price IS NULL
   OR retailer_price IS NULL
   OR mrp IS NULL
   OR launch_fy IS NULL
   OR units_per_carton IS NULL;
-- 0

-- launch_fy distinct values check
SELECT
	DISTINCT launch_fy
FROM
	dim_sku
ORDER BY
	launch_fy;
-- FY2023-24
-- FY2024-25

-- pricing logic check
SELECT
	*
FROM
	dim_sku
WHERE
	cost_price >= distributor_price
	OR distributor_price >= retailer_price
	OR retailer_price >= mrp;
-- 0 rows

-- sku dimension view creation
CREATE OR REPLACE
VIEW vw_dim_sku AS
SELECT
	*
FROM
	dim_sku;

-- vw_dim_sku row count check
SELECT
	COUNT(*)
FROM
	vw_dim_sku;
-- 18

------------------------------------------------------------------------------------------------------------------------------

-- distributor dimension table CTAS creation
CREATE TABLE dim_distributor AS
SELECT
	TRIM(distributor_id) AS distributor_id,
	TRIM(distributor_name) AS distributor_name,
	TRIM(territory_id) AS territory_id,
	TRIM(distributor_tier) AS distributor_tier,
	TRIM(launch_fy) AS launch_fy
FROM
	raw_dim_distributor;

-- The Primary Key Uniqueness Check
SELECT
	distributor_id,
	COUNT(*)
FROM
	dim_distributor
GROUP BY
	distributor_id
HAVING
	COUNT(*) > 1;

-- The Row Count Check
SELECT
	COUNT(*)
	-- 36
FROM
	dim_distributor;

-- The Data Quality (Tier) Check
SELECT
	distributor_tier,
	-- A	B	C
	COUNT(*)
	-- 19	5	12	
FROM
	dim_distributor
GROUP BY
	distributor_tier;

-- null check on critical columns
SELECT
	COUNT(*)
FROM
	dim_distributor
WHERE
	distributor_id IS NULL
	OR territory_id IS NULL
	OR distributor_tier IS NULL
	OR launch_fy IS NULL;
-- 0

-- distributor dimension view creation
CREATE OR REPLACE
VIEW vw_dim_distributor AS
SELECT
	*
FROM
	dim_distributor;

-- vw_dim_distributor row count check
SELECT
	COUNT(*)
FROM
	vw_dim_distributor;
-- 36

------------------------------------------------------------------------------------------------------------------------------

-- territory dimension table CTAS creation
CREATE TABLE dim_territory AS
SELECT
	TRIM(territory_id) AS territory_id,
	TRIM(territory_name) AS territory_name,
	TRIM(district) AS district,
	TRIM(state) AS state,
	TRIM(ZONE) AS ZONE,
	TRIM(channel) AS channel,
	TRIM(sales_rep_name) AS sales_rep_name,
	TRIM(launch_fy) AS launch_fy,
	TRIM(town_tier) AS town_tier
FROM
	raw_dim_territory;

-- The Primary Key Uniqueness Check
SELECT
	territory_id,
	COUNT(*)
FROM
	dim_territory
GROUP BY
	territory_id
HAVING
	COUNT(*) > 1;

-- The Row Count Check
SELECT
	COUNT(*)
	--29
FROM
	dim_territory;

-- The Data Quality (Geography) Check
SELECT
	state,
	-- AS	OD	WB	
	ZONE,
	-- NE	EZ	EZ
	COUNT(*)
	-- 4	6	19
FROM
	dim_territory
GROUP BY
	state,
	ZONE;

-- null check on critical columns
SELECT
	COUNT(*)
FROM
	dim_territory
WHERE
	territory_id IS NULL
	OR state IS NULL
	OR ZONE IS NULL
	OR launch_fy IS NULL;
-- 0

-- territory dimension view creation
CREATE OR REPLACE
VIEW vw_dim_territory AS
SELECT
	*
FROM
	dim_territory;

-- vw_dim_territory row count check
SELECT
	COUNT(*)
FROM
	vw_dim_territory;
-- 29
------------------------------------------------------------------------------------------------------------------------------

-- retailer dimension table CTAS creation
CREATE TABLE dim_retailer AS
SELECT
	TRIM(retailer_id) AS retailer_id,
	TRIM(retailer_name) AS retailer_name,
	TRIM(distributor_id) AS distributor_id,
	TRIM(territory_id) AS territory_id,
	TRIM(outlet_type) AS outlet_type,
	TRIM(town_name) AS town_name,
	TRIM(pincode::TEXT) AS pincode,
	TRIM(onboard_date)::DATE AS onboard_date,
	TRIM(status) AS status,
	NULLIF(TRIM(deactivation_date), '')::DATE AS deactivation_date
FROM
	raw_dim_retailer;

-- primary key uniqueness check
SELECT
	retailer_id,
	COUNT(*)
FROM
	dim_retailer
GROUP BY
	retailer_id
HAVING
	COUNT(*) > 1;

-- row count check
SELECT
	COUNT(*)
	-- 2775
FROM
	dim_retailer;

-- null check on primary key
SELECT
	COUNT(*)
	-- 0
FROM
	dim_retailer
WHERE
	retailer_id IS NULL;

-- status category check
SELECT
	status,
	-- Active	-- Inactive
	COUNT(*)
	-- 2678		-- 97
FROM
	dim_retailer
GROUP BY
	status;

-- deactivation logic integrity check
SELECT
	COUNT(*)
	-- 0
FROM
	dim_retailer
WHERE
	(status = 'Active'
		AND deactivation_date IS NOT NULL)
	OR (status = 'Inactive'
		AND deactivation_date IS NULL);

-- foreign key integrity check against dim_distributor
SELECT
	COUNT(*)
	-- 0
FROM
	dim_retailer r
LEFT JOIN dim_distributor d ON
	r.distributor_id = d.distributor_id
WHERE
	d.distributor_id IS NULL;

-- foreign key integrity check against dim_territory
SELECT
	COUNT(*)
	--0
FROM
	dim_retailer r
LEFT JOIN dim_territory t ON
	r.territory_id = t.territory_id
WHERE
	t.territory_id IS NULL;

-- onboard date boundary check
SELECT
	MIN(onboard_date),
	MAX(onboard_date)
FROM
	dim_retailer;
-- 2023-04-15    2026-03-31

-- retailer dimension view creation
CREATE OR REPLACE
VIEW vw_dim_retailer AS
SELECT
	*
FROM
	dim_retailer;

-- vw_dim_retailer row count check
SELECT
	COUNT(*)
FROM
	vw_dim_retailer;
-- 2,775
------------------------------------------------------------------------------------------------------------------------------

-- fact target table CTAS creation
CREATE TABLE fact_target AS
SELECT
	TRIM(target_id) AS target_id,
	TRIM(territory_id) AS territory_id,
	TRIM(month_start_date)::DATE AS month_start_date,
	TRIM(month_name) AS month_name,
	TRIM(fiscal_year) AS fiscal_year,
	TRIM(fiscal_quarter) AS fiscal_quarter,
	CAST(new_store_target AS INT) AS new_store_target,
	CAST(revenue_target_inr AS INT) AS revenue_target_inr
FROM
	raw_fact_target;

-- primary key uniqueness check
SELECT
	target_id,
	COUNT(*)
FROM
	fact_target
GROUP BY
	target_id
HAVING
	COUNT(*) > 1;

-- row count check
SELECT
	COUNT(*)
FROM
	fact_target;
-- 684

-- null check on primary key
SELECT
	COUNT(*)
FROM
	fact_target
WHERE
	target_id IS NULL;
-- 0

-- foreign key integrity check against dim_territory
SELECT
	COUNT(*)
FROM
	fact_target f
LEFT JOIN dim_territory t ON
	f.territory_id = t.territory_id
WHERE
	t.territory_id IS NULL;
-- 0

-- revenue sanity check: no negative or zero revenue targets should exist
SELECT
	COUNT(*)
FROM
	fact_target
WHERE
	revenue_target_inr <= 0;
-- 0

-- fy-level revenue cross-check
SELECT
	fiscal_year,
	SUM(revenue_target_inr) AS total_revenue_target
FROM
	fact_target
GROUP BY
	fiscal_year
ORDER BY
	fiscal_year;
-- FY2023-24: 11940000  FY2024-25: 26832000  FY2025-26: 55587000

-- date boundary check
SELECT
	MIN(month_start_date),
	MAX(month_start_date)
FROM
	fact_target;
-- 2023-04-01    2026-03-01

-- total new store target cross-check
SELECT
	SUM(new_store_target)
FROM
	fact_target;
-- 3651

-- fact target view creation
CREATE OR REPLACE
VIEW vw_fact_target AS
SELECT
	*
FROM
	fact_target;

-- vw_fact_target row count check
SELECT
	COUNT(*)
FROM
	vw_fact_target;
-- 684
------------------------------------------------------------------------------------------------------------------------------

-- fact_primary_sales CTAS creation
CREATE TABLE fact_primary_sales AS
SELECT
	TRIM(primary_sale_id) AS primary_sale_id,
	TRIM(invoice_id) AS invoice_id,
	TRIM(invoice_date)::DATE AS invoice_date,
	TRIM(grn_date)::DATE AS grn_date,
	TRIM(distributor_id) AS distributor_id,
	TRIM(sku_id) AS sku_id,
	CAST(dispatched_qty AS INT) AS dispatched_qty,
	CAST(received_qty AS INT) AS received_qty,
	CAST(invoice_value_inr AS INT) AS invoice_value_inr
FROM
	raw_fact_primary_sales;

-- primary key uniqueness check
SELECT
	primary_sale_id,
	COUNT(*)
FROM
	fact_primary_sales
GROUP BY
	primary_sale_id
HAVING
	COUNT(*) > 1;

-- natural composite key uniqueness check
SELECT
	invoice_id,
	sku_id,
	COUNT(*)
FROM
	fact_primary_sales
GROUP BY
	invoice_id,
	sku_id
HAVING
	COUNT(*) > 1;

-- row count check
SELECT
	COUNT(*)
FROM
	fact_primary_sales;
-- 20775

-- null check on critical columns
SELECT
	COUNT(*)
FROM
	fact_primary_sales
WHERE
	primary_sale_id IS NULL
	OR invoice_id IS NULL
	OR invoice_date IS NULL
	OR grn_date IS NULL
	OR distributor_id IS NULL
	OR sku_id IS NULL;
-- 0

-- grn date must never be before invoice date
SELECT
	COUNT(*)
FROM
	fact_primary_sales
WHERE
	grn_date < invoice_date;
-- 0

-- dispatched qty must always be positive
SELECT
	COUNT(*)
FROM
	fact_primary_sales
WHERE
	dispatched_qty <= 0;
-- 0

-- received qty must never exceed dispatched qty
SELECT
	COUNT(*)
FROM
	fact_primary_sales
WHERE
	received_qty > dispatched_qty;
-- 0

-- invoice value must always be positive
SELECT
	COUNT(*)
FROM
	fact_primary_sales
WHERE
	invoice_value_inr <= 0;
-- 0

-- date boundary check
SELECT
	MIN(invoice_date),
	MAX(invoice_date)
FROM
	fact_primary_sales;
-- 2023-04-01    2026-03-31

-- fy-level revenue cross-check
SELECT
	CASE
		WHEN EXTRACT(MONTH FROM invoice_date) >= 4
        THEN CONCAT('FY', EXTRACT(YEAR FROM invoice_date)::INT, '-',
             RIGHT((EXTRACT(YEAR FROM invoice_date)::INT + 1)::TEXT, 2))
		ELSE CONCAT('FY', (EXTRACT(YEAR FROM invoice_date)::INT - 1)::TEXT, '-',
             RIGHT(EXTRACT(YEAR FROM invoice_date)::INT::TEXT, 2))
	END AS fiscal_year,
	SUM(invoice_value_inr) AS total_invoice_value
FROM
	fact_primary_sales
GROUP BY
	fiscal_year
ORDER BY
	fiscal_year;
-- FY2023-24: 9468000    FY2024-25: 22729320    FY2025-26: 50530788

-- foreign key integrity check against dim_distributor
SELECT
	COUNT(*)
FROM
	fact_primary_sales f
LEFT JOIN dim_distributor d ON
	f.distributor_id = d.distributor_id
WHERE
	d.distributor_id IS NULL;
-- 0

-- foreign key integrity check against dim_sku
SELECT
	COUNT(*)
FROM
	fact_primary_sales f
LEFT JOIN dim_sku s ON
	f.sku_id = s.sku_id
WHERE
	s.sku_id IS NULL;
-- 0

-- invoice value integrity check: invoice_value_inr must equal dispatched_qty x distributor_price
SELECT
	COUNT(*)
FROM
	fact_primary_sales f
JOIN dim_sku s ON
	f.sku_id = s.sku_id
WHERE
	f.invoice_value_inr <> f.dispatched_qty * s.distributor_price;
-- 0

-- fact_primary_sales view creation
CREATE OR REPLACE
VIEW vw_fact_primary_sales AS
SELECT
	*
FROM
	fact_primary_sales;

-- vw_fact_primary_sales row count check
SELECT
	COUNT(*)
FROM
	vw_fact_primary_sales;
-- 20775

------------------------------------------------------------------------------------------------------------------------------

-- fact_secondary_sales CTAS creation
CREATE TABLE fact_secondary_sales AS
WITH
NORMALIZED AS (
SELECT
	TRIM(distributor_bill_no) AS distributor_bill_no,
	CAST(line_item_no AS INT) AS line_item_no,
	CASE
		WHEN TRIM(bill_date) ~ '^\d{2}/\d{2}/\d{4}$'
                THEN TO_DATE(TRIM(bill_date), 'DD/MM/YYYY')
		WHEN TRIM(bill_date) ~ '^\d{2}-\d{2}-\d{4}$'
                THEN TO_DATE(TRIM(bill_date), 'MM-DD-YYYY')
		WHEN TRIM(bill_date) ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TRIM(bill_date)::DATE
		ELSE NULL
	END AS bill_date,
	UPPER(TRIM(retailer_id)) AS retailer_id,
	TRIM(distributor_id) AS distributor_id,
	UPPER(TRIM(sku_id)) AS sku_id,
	CAST(ordered_qty AS INT) AS ordered_qty,
	CAST(delivered_qty AS INT) AS delivered_qty,
	CAST(returned_qty AS INT) AS returned_qty,
	CAST(bill_value_inr AS INT) AS bill_value_inr
FROM
	raw_fact_secondary_sales
),
date_filtered AS (
SELECT
	*
FROM
	NORMALIZED
WHERE
	bill_date BETWEEN '2023-04-01' AND '2026-03-31'
),
deduped AS (
SELECT
	*,
	ROW_NUMBER() OVER (
            PARTITION BY distributor_id,
	distributor_bill_no,
	line_item_no
ORDER BY
	delivered_qty DESC
        ) AS rn
FROM
	date_filtered
),
cleaned AS (
SELECT
	distributor_bill_no,
	line_item_no,
	bill_date,
	retailer_id,
	distributor_id,
	sku_id,
	ordered_qty,
	CASE
		WHEN delivered_qty < 0 THEN 0
		ELSE delivered_qty
	END AS delivered_qty,
	CASE
		WHEN delivered_qty < 0 THEN ABS(delivered_qty)
		ELSE returned_qty
	END AS returned_qty,
	bill_value_inr
FROM
	deduped
WHERE
	rn = 1
)

SELECT
	*
FROM
	cleaned;

-- row count check
SELECT
	COUNT(*)
FROM
	fact_secondary_sales;
-- 411914

-- composite primary key uniqueness check
SELECT
	distributor_id,
	distributor_bill_no,
	line_item_no,
	COUNT(*)
FROM
	fact_secondary_sales
GROUP BY
	distributor_id,
	distributor_bill_no,
	line_item_no
HAVING
	COUNT(*) > 1;

-- null check on critical columns
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	distributor_bill_no IS NULL
	OR line_item_no IS NULL
	OR bill_date IS NULL
	OR retailer_id IS NULL
	OR distributor_id IS NULL
	OR sku_id IS NULL;
-- 0

-- date boundary check: no dates outside the operational window after ctas filtering
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	bill_date < '2023-04-01'
	OR bill_date > '2026-03-31';
-- 0

-- sunday bill_date check: error 9 rows removed by date filter, none should remain
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	EXTRACT(DOW FROM bill_date) = 0;
-- 0

-- delivered_qty must never exceed ordered_qty
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	delivered_qty > ordered_qty;
-- 0

-- negative delivered_qty check
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	delivered_qty < 0;
-- 0

-- foreign key integrity check against dim_distributor
SELECT
	COUNT(*)
FROM
	fact_secondary_sales f
LEFT JOIN dim_distributor d ON
	f.distributor_id = d.distributor_id
WHERE
	d.distributor_id IS NULL;
-- 0

-- foreign key integrity check against dim_sku
SELECT
	COUNT(*)
FROM
	fact_secondary_sales f
LEFT JOIN dim_sku s ON
	f.sku_id = s.sku_id
WHERE
	s.sku_id IS NULL;
-- 2060

-- foreign key integrity check against dim_retailer excluding cash-sale rows
SELECT
    COUNT(*)
FROM
    fact_secondary_sales f
LEFT JOIN dim_retailer r ON
    f.retailer_id = r.retailer_id
WHERE
    r.retailer_id IS NULL
    AND f.retailer_id <> 'CASH-SALE';
-- 0

-- inactive retailer billed after deactivation_date
SELECT
	COUNT(*)
FROM
	fact_secondary_sales f
JOIN dim_retailer r ON
	f.retailer_id = r.retailer_id
WHERE
	r.status = 'Inactive'
	AND r.deactivation_date IS NOT NULL
	AND f.bill_date > r.deactivation_date;
-- 0

-- bill_date before retailer onboard_date
SELECT
	COUNT(*)
FROM
	fact_secondary_sales f
JOIN dim_retailer r ON
	f.retailer_id = r.retailer_id
WHERE
	f.bill_date < r.onboard_date::DATE;
-- 0

-- cash-sale row count 
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	retailer_id = 'CASH-SALE';
-- 1236

-- broken bill math detection
SELECT
	COUNT(*)
FROM
	fact_secondary_sales f
JOIN dim_sku s ON
	f.sku_id = s.sku_id
WHERE
	ABS(f.bill_value_inr - f.delivered_qty * s.retailer_price) > 1
	AND f.bill_value_inr > 0;
-- 1976

-- impossible returns: returned_qty exceeds delivered_qty 
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	returned_qty > delivered_qty;
-- 922

-- free goods rows: delivered_qty >= 1 with bill_value_inr = 0
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	bill_value_inr = 0
	AND delivered_qty >= 1
	AND sku_id IN (
	SELECT
		sku_id
	FROM
		dim_sku);
-- ~823

-- orphaned sku rows: sku_id not present in dim_sku 
SELECT
	COUNT(*)
FROM
	fact_secondary_sales
WHERE
	sku_id NOT IN (
	SELECT
		sku_id
	FROM
		dim_sku);
-- 2060

-- fy-level revenue cross-check on gross bill_value_inr (flagged error rows included)
SELECT
	CASE
		WHEN EXTRACT(MONTH FROM bill_date) >= 4
            THEN CONCAT('FY', EXTRACT(YEAR FROM bill_date)::INT, '-',
                 RIGHT((EXTRACT(YEAR FROM bill_date)::INT + 1)::TEXT, 2))
		ELSE CONCAT('FY', (EXTRACT(YEAR FROM bill_date)::INT - 1)::TEXT, '-',
                 RIGHT(EXTRACT(YEAR FROM bill_date)::INT::TEXT, 2))
	END AS fiscal_year,
	SUM(bill_value_inr) AS total_bill_value
FROM
	fact_secondary_sales
GROUP BY
	fiscal_year
ORDER BY
	fiscal_year;
-- FY2023-24: 8912558    FY2024-25: 22002616    FY2025-26: 47185516

-- fact_secondary_sales view creation
CREATE OR REPLACE
VIEW vw_fact_secondary_sales AS
SELECT
	*
FROM
	fact_secondary_sales;

-- vw_fact_secondary_sales row count check
SELECT
	COUNT(*)
FROM
	vw_fact_secondary_sales;
-- 411914
------------------------------------------------------------------------------------------------------------------------------
