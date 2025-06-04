-- L1_status
CREATE OR REPLACE VIEW `active-cove-455414-g4.L1.L1_status` AS
SELECT
  CAST(id_status AS INT64) AS product_status_id --PK
  , LOWER(status_name) AS product_status_name
  , PARSE_DATE('%m/%d/%Y', date_update) AS product_status_update_date
FROM `active-cove-455414-g4.L0_google_sheets.status`
WHERE id_status IS NOT NULL
  and status_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_status_id) = 1;


--L1_invoice
CREATE OR REPLACE VIEW `active-cove-455414-g4.L1.L1_invoice` AS
SELECT
id_invoice AS invoice_id, --PK
id_invoice_old AS invoice_previous_id,
invoice_id_contract as contract_id, --FK
status AS invoice_status_id, 
-- Invoice status. Invoice status < 100 have been issued. >= 100 - not issued
IF(status < 100, TRUE, FALSE) AS flag_invoice_issued,
id_branch AS branch_id,--FK

DATE(date, "Europe/Prague") as date_issue,
  DATE(scadent, "Europe/Prague") as due_date,
  DATE(date_paid, "Europe/Prague") as paid_date,
  DATE(start_date, "Europe/Prague") as start_date,
  DATE(end_date, "Europe/Prague") as end_date,
  DATE(date_insert, "Europe/Prague") as insert_date,
  DATE(date_update, "Europe/Prague") as update_date,
value AS amount_w_vat,
payed AS amount_payed,
flag_paid_currier as flag_paid_currier,
invoice_type AS invoice_type_id, -- Invoice_type: 1 - invoice, 3 - credit_note, 2 - return, 4 - other
CASE
  when invoice_type = 1 THEN "invoice"
  when invoice_type = 2 THEN "return"
  when invoice_type = 3 THEN "credit_note"
  when invoice_type = 4 THEN "other"
END AS invoice_type,
number as invoice_number,
value_storno AS return_w_vat
FROM `active-cove-455414-g4.L0_accounting_system.invoice` 
;

--L1_invoice_load
CREATE OR REPLACE VIEW `active-cove-455414-g4.L1.L1_invoice_load` AS
SELECT
id_load as invoice_load_id, --PK
id_contract as contract_id, --FK
id_package as package_id, --FK
id_invoice as invoice_id, --FK
id_package_template as product_id, --FK
notlei as price_wo_vat_usd,
tva as vat_rate,
value as price_w_vat_usd,
payed as paid_w_vat_usd,
case
 when um IN ('mesia','m?síce','m?si?1ce','měsice','mesiace','měsíce','mesice') then  'month'
 when um = "kus" then "item"
 when um = "den" then 'day'
 when um = "min" then 'minute'
 when um = '0' then null
 else um
 end AS unit, 
quantity,
DATE(TIMESTAMP(start_date), "Europe/Prague") as start_date,
DATE(TIMESTAMP(end_date), "Europe/Prague") as end_date,
DATE(TIMESTAMP(date_insert), "Europe/Prague") as date_insert,
DATE(TIMESTAMP(date_update), "Europe/Prague") as date_update,
FROM `active-cove-455414-g4.L0_accounting_system.invoice_load` 
;

--L1 branch
CREATE OR REPLACE VIEW `active-cove-455414-g4.L1.L1_branch` AS
SELECT
CAST(id_branch as INT) as branch_id, --PK
branch_name,
PARSE_DATE('%m/%d/%Y', date_update) AS product_status_update_date

FROM `active-cove-455414-g4.L0_google_sheets.branch` 

where
id_branch != "NULL"
;

--L1 product
CREATE OR REPLACE VIEW `active-cove-455414-g4.L1.L1_product` AS
SELECT
CAST(id_product as INT) as product_id, --PK
name AS product_name,
type AS product_type,
category AS product_category,
CAST (is_vat_applicable as BOOL) AS is_vat_applicable,
PARSE_DATE('%m/%d/%Y', date_update) AS product_status_update_date

FROM `active-cove-455414-g4.L0_google_sheets.product` 

where
id_product is not null

QUALIFY ROW_NUMBER() OVER(PARTITION BY product_id) = 1
;

--L1_product_purchase
create or replace view `active-cove-455414-g4.L1.L1_product_purchase` AS
select
  packages.id_package as product_purchase_id, --PK
  packages.id_contract as contract_id, --FK
  packages.id_package_template as product_id, --FK
  date(packages.date_insert, "Europe/Prague") as create_date,
  PARSE_DATE('%m/%d/%Y', packages.start_date) as product_valid_from,
  PARSE_DATE('%m/%d/%Y', packages.end_date) as product_valid_to,
  packages.fee as price_wo_vat,
  date(packages.date_update, "Europe/Prague") as date_update,
  case
    when packages.measure_unit IN ('mesia','m?síce','m?si?1ce','měsice','mesiace','měsíce','mesice') then  'month'
    when packages.measure_unit = "kus" then "item"
    when packages.measure_unit = "den" then 'day'
    when packages.measure_unit = "min" then 'minute'
    when packages.measure_unit = '0' then null
    else packages.measure_unit
  end AS unit, 
  packages.package_status as product_status_id, --FK
  status.product_status_name as product_status,
  product.product_name,
  product.product_type,
  product.product_category
from `active-cove-455414-g4.L0_crm.product_purchase` packages 
left join `active-cove-455414-g4.L1.L1_status` status
on packages.package_status = status.product_status_id
left join `active-cove-455414-g4.L1.L1_product` product 
on packages.id_package_template = product.product_id;

-- L1 --contract
create or replace view `active-cove-455414-g4.L1.L1_contract` as
Select
id_contract as contract_id, --PK
id_branch as branch_id, --FK
TIMESTAMP(date_contract_valid_from) as contract_valid_from,
TIMESTAMP(date_contract_valid_to) as contract_valid_to,
date_registered as registred_date,
date_signed as signed_date,
TIMESTAMP(activation_process_date) AS activation_process_date,
TIMESTAMP(prolongation_date)  as prolongation_date,
registration_end_reason,
flag_prolongation,
flag_send_inv_email  as flag_send_email,
contract_status as contract_status
from `active-cove-455414-g4.L0_crm.contract` ;
