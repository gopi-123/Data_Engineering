## Assignment 5: ADF Pipeline 2
 
	 Azurelib Academy
	 
	 1. Create a pipeline name 'Ingestion_Customer_SQLDB_ADLS' which will copy all the records from
	 Customer table to ADLS account as CSV File.
	 2. Create a pipeline name 'Ingestion_Customer_SQLDB_ADLS_Folder' which will copy all the records
	 from Address table to ADLS account inside the 'Address' folder as CSV File.
	 3. Create a Pipeline name 'Ingestion_Customer_JOIN_ADLS' which will copy the all the customer
	 names along with their address into the csv. (Hint Join Customer+ Customer Address table)
	 4. Create a pipeline name 'Ingestion_Product_To_JSON' which will copy all the product records as
	 JSON only if total number of records >10.
 
 
	 ###  Tough Question
	 5. Create a pipeline name 'Ingestion_Product_Addres_To_JSON' which will copy all the product
	 records as JSON only if total number of records >10. After that check total record count in address
	 table if they are greater than 100 then copy the adress table data as CSV in ADLS.
	 
 
 ## Assignment 6: ADF Pipeline 3
	Azurelib Academy
 
	 ### Easy Questions:
	 1. Create a pipeline name 'Without_Foreach_Example' to copy the Customer & Customer address
	 table data into ADLS without using foreach activity.
	 2. Create a pipeline name 'Foreach_Example' to copy the Customer & Customer address table data
	 into ADLS using the one copy activity.
	 
	 ### Tricky Questions:
	 3. Create a pipeline name 'Foreach_Example_2' which solves the following business use case.
	 Customer data is very important for our business. Hence whenever we have more than 100 records in
	 the customer table,
	 wecopythe customer data to another table customer_copy within the sql db.
	 However whenever we do this copy, we first truncate the table 'customer_copy' and then copy the
	 data from 'customer' table
	 
	 #### Hint : Use ADF to solve this problem.
	 
	 4. Business wants to move the data from 3 different tables (Customer, Product, CustomerAddress) to
	 ADLS location in CSV format.
	However they want the make a pipeline in such a manner that All these copy happens through an
	 individual pipeline, which is called by
	 one commonpipeline.
	 5. In the above case problem is that every time the pipeline runs it will overwrite the data. Ensure that
	 all the data goes into
	 Folder like (Customer/Year/Month/Day) to avoid any overwriting. Please rewrite the pipeline.
	 6. I am attaching a file, load this file into your ADLS location. This file contains a threshold value.
	 Create a pipeline
	 in such a manner that, it will first get the threshold value from this file and then check if the record
	 count in the customer
	 table is more than it or not. If yes then copy the customer data from SQL db to ADLS location in JSON
	 format.