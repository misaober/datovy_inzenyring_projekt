--VARIANTA INVOICE - SNOWFLAKE
--L3 Contract
create or replace view `active-cove-455414-g4.L3_snowflake.L3_contract` AS 
SELECT
contract_id
, branch_id
, contract_valid_from
, contract_valid_to
, prolongation_date
, registration_end_reason
, contract_status
, flag_prolongation
, case
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, day)/365 < 0.5 THEN "less than half year"
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, day)/365 < 1.01 THEN "1 year"
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, day)/365 < 2.01 THEN "2 years"
    ELSE "more than 2 years"
    END AS contract_duration
, extract(year from contract_valid_from) as start_year_of_contract
FROM `active-cove-455414-g4.L2.L2_contract`
where contract_valid_from > contract_valid_to AND contract_valid_from is not null and contract_valid_to is not null
;

-- L3 branch
CREATE OR REPLACE VIEW `active-cove-455414-g4.L3_snowflake.L3_branch` AS 
SELECT
branch_id
, branch_name

FROM `active-cove-455414-g4.L2.L2_branch`
;

-- L3 product + product purchase
CREATE OR REPLACE VIEW `active-cove-455414-g4.L3_snowflake.L3_product` AS
SELECT 
L2_product_purchase.product_purchase_id --PK
, L2_product.product_id
, L2_product.product_name
, L2_product.product_type
, L2_product_purchase.product_valid_from
, L2_product_purchase.product_valid_to
, L2_product_purchase.unit
, L2_product_purchase.flag_unlimited_product

FROM `active-cove-455414-g4.L2.L2_product_purchase` L2_product_purchase 
LEFT JOIN `active-cove-455414-g4.L2.L2_product` L2_product ON
L2_product_purchase.product_id = L2_product.product_id
;


-- L3 invoice
CREATE OR REPLACE VIEW `active-cove-455414-g4.L3_snowflake.L3_invoice` AS SELECT
L2_invoice.invoice_id
, L2_invoice.contract_id
, L2_invoice.paid_date
, L2_invoice.amount_w_vat
, L2_invoice.return_w_vat
, L2_product_purchase.product_id
, (L2_invoice.amount_w_vat - L2_invoice.return_w_vat) AS total_paid_per_invoice 

From `active-cove-455414-g4.L2.L2_invoice` L2_invoice LEFT JOIN 
`active-cove-455414-g4.L2.L2_product_purchase` L2_product_purchase ON
L2_invoice.contract_id = L2_product_purchase.contract_id
;

