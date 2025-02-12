
### Assignment 3: Azure Data Factory Account Creation

Create your first ADF account where adf account name : mission100+yourname+yourdob and put it inside the resource group name azurelib.
Assignment 3: Azure Data Factory Account Creation


### Assignment 4: Azure Data Factory Pipeline 1: 

Go to the ADF studio and create your first pipeline name: Copy_ProductTable_To_CSV. 

In the newly created SQL DB there is one built in SalesLt.Product table.

Copy the Product table data to CSV file in 'landing/CSV' folder in ADLS using the ADF.

###  Assignment 4:  Pipeline 2: 

Create another pipeline name: Copy_Customer_To_JSON In the newly created SQL DB there is one built in SalesLt.Customer table.

Copy the Customer table data to JSON file in 'landing/JSON' folder in ADLS using the ADF.

Hint: During the dataset creation step instead of choosing the file type CSV choose JSON

###  Assignment 4: Pipeline 3: 

Create another pipeline name: Copy_Customer_JSON_To_Folder. In the last pipeline 1, you have created the Customer file in ADLS 'landing/CSV' folder. Now this time try to move this file to another ADLS folder 'landing/CSV2' using the ADF pipeline.

Hint: This time our source is ADLS 'landing/CSV' folder and destination is also of ADLS type but different folder 'landing/CSV2' (You can create one more dataset)

###  Assignment 4: Tough Question: 

###  Assignment 4: Pipeline 4: 

Create another pipeline name: Copy_Customer_To_CSV_Pipe In the newly created SQL DB there is one built in SalesLt.Customer table.

Copy the Customer table data to CSV file in 'landing/CSV_Pipe' folder in ADLS using the ADF. But ensure that the delimiter of this file should be (|) instead of (,)



Hint: After you create your dataset for CSV, open the dataset there you should see the option to change the delimiter.