/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
*/

create or alter procedure silver.load_silver as
begin
	declare @batch_start_time datetime, @batch_end_time datetime, @start_time datetime, @end_time datetime;
	begin try
		set @batch_start_time = getdate();

		print '================================================';
        print 'Loading Silver Layer';
        print '================================================';

		print '---------------------------------------------------------------------------------------------------------';
		print '                                    Loading CRM Tables                                                   ';
		print '----------------------------------------------------------------------------------------------------------';

		-------------------------------- Loading silver.crm_cust_info -------------------------------------------------
		set @start_time = getdate();

		print  '>> Truncating Table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;

		print '>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info (
					cst_id, 
					cst_key, 
					cst_firstname, 
					cst_lastname, 
					cst_marital_status, 
					cst_gndr,
					cst_create_date
				)
		select
			cst_id,
			cst_key,
	
			-- removing unwanted spaces
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,

			-- data standardization for cst_marital_status column
			case when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'
			end cst_marital_status,

			-- data standardization for cst_gndr column
			case when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				else 'n/a'
			end cst_gndr,

			cst_create_date
		from (
			select
				*,
				row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
		)t -- to fix the duplicate rows: Select the most recent record per customer

		where flag_last = 1

		set @end_time = getdate();
		print 'Load Duration' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>> -------------';


		-------------------------------- Loading silver.crm_prd_info -------------------------------------------------
		set @start_time = getdate();

		print  '>> Truncating Table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;

		print '>> Inserting Data Into: silver.crm_prd_info';
		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select prd_id,
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id, -- why? explained in notion (table1: crm_pdr_info <-> table2: erp_px_cat_g1v2)
		substring(prd_key, 7, LEN(prd_key)) AS prd_key,        -- (table1: crm_pdr_info <-> table2: crm_sales_details)
		prd_nm,
		isnull(prd_cost, 0) AS prd_cost, -- if the value in column is null, replace it with 0

		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'n/a'
		end as prd_line, -- Map product line codes to descriptive values

		/*
		case 
			when upper(trim(prd_line)) = 'M' then 'Mountain'
			when upper(trim(prd_line)) = 'R' then 'Road'
			when upper(trim(prd_line)) = 'S' then 'Other Sales'
			when upper(trim(prd_line)) = 'T' then 'Touring'
			else 'n/a'
		end as prd_line,
		*/
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
		from bronze.crm_prd_info

		set @end_time = getdate();
		print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>> -------------';



		-------------------------------- Loading silver.crm_sales_details -------------------------------------------------
		 set @start_time = getdate();

		print  '>> Truncating Table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;

		print '>> Inserting Data Into: silver.crm_sales_details';
		insert silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt = 0 or len(sls_order_dt) != 8 then null -- initially sls_order_dt is interger data type: eg, 20101229 in table 
				else cast(cast(sls_order_dt as varchar) as date) -- in sql server, to convert integer to date, first cast it to varchar, then date, now: 2010-12-29
			end as sls_order_dt,

			-- do the same for sls_ship_dt column
			case 
				when sls_ship_dt  = 0 or len(sls_ship_dt ) != 8 then null -- initially sls_order_dt is interger data type: eg, 20101229 in table 
				else cast(cast(sls_ship_dt  as varchar) as date) -- in sql server, to convert integer to date, first cast it to varchar, then date, now: 2010-12-29
			end as sls_ship_dt ,

			-- do the same for sls_due_dt column
			case 
				when sls_due_dt  = 0 or len(sls_due_dt ) != 8 then null -- initially sls_order_dt is interger data type: eg, 20101229 in table 
				else cast(cast(sls_due_dt  as varchar) as date) -- in sql server, to convert integer to date, first cast it to varchar, then date, now: 2010-12-29
			end as sls_due_dt,

			--- if sales is null, zero, or negative, it must be recalculated using the formula: Sales = Quantity × Price
			case
				when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity*abs(sls_price)
				else sls_sales
			end as sls_sales,

			sls_quantity,

			case
				when sls_price is null or sls_price <= 0
					then sls_sales / nullif(sls_quantity, 0) -- to avoid divide-by-zero error, use nullif, put null if value is 0
				else sls_price
			end as sls_price
  
		from bronze.crm_sales_details;

		set @end_time = getdate();
		print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>> -------------';
		
		print '------------------------------------------------------------------------------------------------';
		print '                                 Loading ERP Tables                                             ';
		print '------------------------------------------------------------------------------------------------';

		-------------------------------- Loading silver.erp_cust_az12 -------------------------------------------------
		set @start_time = getdate();

		print  '>> Truncating Table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;

		print '>> Inserting Data Into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		select 
			-- Remove 'NAS' prefix if present
			case 
				when cid like 'NAS%' then substring(cid, 4 , len(cid)) -- (table1: erp_cust_az12 <-> table2: crm_cust_info)
				else cid
			end as cid,
	
			-- Set future birthdates to NULL
			case 
				when bdate > getdate() then null
				else bdate
			end as bdate,

			 -- Normalize gender values and handle unknown cases
			 case
				when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
				when upper(trim(gen)) in ('M', 'MALE') then 'Male'
				else 'n/a'
			end as gen
			 /* when upper(trim(gen)) = 'F' or upper(trim(gen)) = 'FEMALE' then 'Female' */

		from bronze.erp_cust_az12;

		set @end_time = getdate();
		print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>> -------------';

		-------------------------------- Loading silver.erp_loc_a101 -------------------------------------------------
		set @start_time = getdate();

		print  '>> Truncating Table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;

		print '>> Inserting Data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101 (
			cid,
			cntry
		)
		select 
			replace(cid, '-', '') as cid,
			case 
				when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('US', 'USA') then 'United State'
				when trim(cntry) = '' or cntry is null then 'n/a'  -- IN (...) compares values, null is not a value, it represents “unknown”, that's why can't use 'in' here
				else trim(cntry)
		end as cntry
		from bronze.erp_loc_a101;

		set @end_time = getdate();
		print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>> -------------';


		-------------------------------- Loading silver.erp_px_cat_g1v2 -------------------------------------------------
		set @start_time = getdate();

		print  '>> Truncating Table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;

		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;

		set @end_time = getdate();
		print 'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>> ----------';

		set @batch_end_time = GETDATE();
		print '=========================================================================================================='
		print '                                Loading Silver Layer is Completed                                        ';
        print 'Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '============================================================================================================'
	end try
	begin catch
		print '=================================================================================================='
		print '                          ERROR OCCURED DURING LOADING BRONZE LAYER                               '
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		print 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		print '==================================================================================================='
	end catch
end


exec silver.load_silver
