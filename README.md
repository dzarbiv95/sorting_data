# sort_lists

This project implements data sort process in three versions.
* `sort_task` fetch the data and sort it.
* `merge_task` we have limitation on the size of the data in one sort. we sort chunks and merge them to a long list.
* `parallel_task` we must improve the process performance. we implement the `merge_task` in multi-tasking process.

The project contains 7 python files:
* `connection_db.py` - the model that accompanies the database connection.
* `setup.py` - the script that set up the basic data for the project and any table names. The setup step.
* `sort_task` - the script that implements the sorting task. The first step.
* `merge_task` - the script that implements the merging task. The second step.
* `test_unit` - unit test for the merging method.
* `parallel_task` - the script that implements the parallel task. The third step. 
* `flask_server` - a script that implements a flask server that provide an API server for the sorted data.

## Set up the project:

For this project I chose the sqllite3 database, a simple local database. The database stores its data in a local file.
The name of the DB in this project is `dataDB` and it stored in the file named `dataDB.db`.
In setup, I create the database and its tables and generate the basic data for the project.
The basic data table for the project named `ADS_DATA`.

The data for this table generated in the `setup.py` script.
In this script the table filled 20000 data rows that include the follow fields:
* ``IDX`` - index based on the row number.
* ``ID`` - unique id for any data row.
* ``DATA`` - the data. random word (ad) for the sorting tests.

## step1 - sort_task

The first step implement in `sort_task.py` script.
The script include Fetch all data from the `ADS_DATA` table, sort this data by the `DATA` column
and insert the sorted data to new table named `RESULTS1`.
Finally, insert the time that take from the start of the process to the end to the statistics table named `RESULTS`.

## step2 - merge_task

Now, we have a limitation we can't sort more 2000 record at once. 
The second step implements in `merge_task.py` script.
The script implements the sort by fetch in loop chunks of 2000 data rows from the `ADS_DATA` table.
After fetch the data chunk sort this data and merge the data with the other chunks to one long sorted data list.
The long sorted data insert to new table named `RESULTS2`.
Finally, the time that take from the start of the process to the end inserted to the statistics table named `RESULTS`.
For the merge method built unit test in the `test_unit.py` file.

## step3 - parallel_task

Now, we must improve the process performance.
The third step implements in `parallel_task.py` script.
In this step we implement the merging task from step 2 by multi threading task.