-- ====================================================================
-- Checking 'bronze.crm_cust_info'
-- ====================================================================

-- checks for nulls or duplicates in primary key
select 
	cst_id,
	count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- check for unwanted space:ll leading and trailing spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)
