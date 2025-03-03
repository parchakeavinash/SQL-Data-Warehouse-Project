/*
=====================================================================================

Store Procedure: Load Silverr Layer ( Bronze > silver)

=====================================================================================
Script Purpose: 
      This Store procedure performs teh ETL ( Extract, Transform, Load ) process to 
      populated the 'silver' schema tables from the 'bronze' schema.
Action performed:
      - Truncates silver tables.
      - Inserts transformed and cleansed data from Bronze into Silver Tables.

Usage Examples:
  EXEC Silver.load_silver;
*/

CREATE OR ALTER PROCEDURE Silver.load_silver as 
Begin
	print('Truncating  table :  silver.crm_cust_info');
	Truncate table silver.crm_cust_info
	print('inserting  data intoL  silver.crm_cust_info');
	insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_first_name,
		cst_last_name,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

	select 
		cst_id,
		cst_key,
		trim(cst_first_name) as cst_first_name,
		trim(cst_last_name) as cst_last_name,
		case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			when upper(trim(cst_marital_status)) = 'M' then 'Married'
			else 'n/a'
		end cst_marital_status,
		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end cst_gndr,
		cst_create_date
	from
	(
		select *,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rnk
		from 
		bronze.crm_cust_info
		where cst_id is not null) t
	where rnk = 1;

	print('Truncating  table :  silver.crm_prd_info');
	Truncate table silver.crm_prd_info
	print('inserting  data intoL  silver.crm_prd_info');
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select 
	prd_id,
	replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	SUBSTRING(prd_key, 7 ,len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	case when upper(trim(prd_line)) = 'M' then 'Mountain'
		 when upper(trim(prd_line)) = 'S' then 'Other Sales'
		 when upper(trim(prd_line)) = 'R' then 'Road'
		 when upper(trim(prd_line)) = 'T' then 'Touring'
		 else 'n/a'
	end prd_line,
	cast(prd_start_dt as date) as prd_start_dt,
	cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) as Date) as prd_end_dt
	from bronze.crm_prd_info;


	print('Truncating  table :  silver.crm_sales_details');
	Truncate table silver.crm_sales_details
	print('inserting  data intoL  silver.crm_sales_details');
	insert into silver.crm_sales_details(
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
	case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
		else cast(cast(sls_order_dt as varchar) as Date)
	end sls_order_dt,
	case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
		else cast(cast(sls_ship_dt as varchar) as Date)
	end sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
		else cast(cast(sls_due_dt as varchar) as Date)
	end sls_due_dt,
	case when sls_sales is null or sls_sales <=0 or sls_sales  != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
		else sls_sales
	end sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity,0)
		else sls_price
	end as sls_price
	from bronze.crm_sales_details;


	print('Truncating  table :  silver.erp_cust_az12');
	Truncate table silver.erp_cust_az12
	print('inserting  data intoL  silver.erp_cust_az12');
	Insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	select 
	SUBSTRING(cid,4,len(cid)) as cid,
	case when bdate > getdate()  then  null
	else bdate
	end as bdate,
	case when upper(trim(gen)) in ('F','Female') then 'Female'
		 when upper(trim(gen)) in ('M','Male') then 'Male'
		 else 'n/a'
	end as gen
	from bronze.erp_cust_az12;


	print('Truncating  table :  silver.erp_loc_a101');
	Truncate table silver.erp_loc_a101
	print('inserting  data intoL  silver.erp_loc_a101');
	insert into silver.erp_loc_a101
	(
	CID,
	CNTR
	)
	SELECT 
	replace(CID, '-','') as cid,
	case when trim(cntr)  = 'DE' then 'Germany'
		  when trim(cntr)  in ('US','USA') then 'United States'
		   when trim(cntr)  = '' OR CNTR IS NULL then 'n/a'
		  else trim(cntr)
	end CNTR
	FROM bronze.erp_loc_a101;



	print('Truncating  table :  silver.erp_px_cat_g1v2');
	Truncate table silver.erp_px_cat_g1v2
	print('inserting  data intoL  silver.erp_px_cat_g1v2');
	insert into silver.erp_px_cat_g1v2
	(	
	id,
	cat,
	subcat,
	maintenance
	)
	select 
	id,
	cat,
	SUBCAT,
	MAINTENANCE
	from bronze.erp_px_cat_g1v2;
END


