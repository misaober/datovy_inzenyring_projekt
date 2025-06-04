--L2 Contract
create or replace view `active-cove-455414-g4.L2.L2_contract` as
Select
contract_id,
branch_id,
contract_valid_from,
contract_valid_to,
registred_date,
signed_date,
activation_process_date,
prolongation_date,
registration_end_reason,
flag_prolongation,
flag_send_email,
contract_status
from `active-cove-455414-g4.L1.L1_contract` 
WHERE registred_date IS NOT NULL;

--l2_invoice
CREATE OR REPLACE VIEW `active-cove-455414-g4.L2.L2_invoice` AS
SELECT
invoice.invoice_id, 
invoice.contract_id,
invoice.date_issue,
invoice.due_date,
invoice.paid_date, 
invoice.start_date, 
invoice.end_date, 
invoice.amount_w_vat, 
invoice.return_w_vat, 
CASE
    WHEN invoice.amount_w_vat <= 0 THEN 0
    WHEN invoice.amount_w_vat > 0 THEN amount_w_vat / 1.2
    END AS amount_wo_vat_usd, 
invoice.insert_date, 
invoice.update_date, 
ROW_NUMBER() OVER (PARTITION BY invoice.contract_id order by invoice.date_issue asc) AS invoice_order,
FROM `active-cove-455414-g4.L1.L1_invoice` invoice
inner join `active-cove-455414-g4.L1.L1_contract` contract on invoice.contract_id = contract.contract_id
where invoice.invoice_type = "invoice" and flag_invoice_issued
;

--L02_product_purchase
CREATE OR REPLACE VIEW `active-cove-455414-g4.L2.L2_product_purchase` AS
SELECT
 product_purchase_id
 ,contract_id
 ,product_id
 ,create_date
 ,product_valid_from
 ,product_valid_to
 ,price_wo_vat
 ,IF(price_wo_vat <= 0, 0,  price_wo_vat * 1.20 ) AS price_w_vat
 ,unit
 ,date_update
 ,product_name
 ,product_type
 ,IF(product_valid_from = '2035-12-31', TRUE, FALSE) AS flag_unlimited_product
FROM `active-cove-455414-g4.L1.L1_product_purchase`
WHERE product_status NOT IN ('canceled', 'canceled registration', 'disconnected')
 AND product_status IS NOT NULL
 AND product_category IN ('product', 'rent')
;

-- L2_product
CREATE OR REPLACE VIEW `active-cove-455414-g4.L2.L2_product` AS
SELECT
product_id, 
product_name,
product_type,
product_category
FROM `active-cove-455414-g4.L1.L1_product`
where product_category IN ("product", "rent")
;

--L02 Branch
create or replace view `active-cove-455414-g4.L2.L2_branch` as select
branch_id,
branch_name
from `active-cove-455414-g4.L1.L1_branch`
where branch_name != "unknown"
;