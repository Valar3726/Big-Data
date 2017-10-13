Cus = LOAD '/user/hadoop/input/Customers.csv' USING PigStorage(',') AS (ID, Name, Age, CountryCode, Salary);
Tran = LOAD '/user/hadoop/input/Transactions.csv' USING PigStorage(',') AS (TransID, CustID, TransTotal, TransNumItems, TransDesc);
Tran_1 = GROUP Tran BY CustID;
Tran_2 = FOREACH Tran_1 GENERATE group as CustID, COUNT(Tran.TransID) as NumTrans, SUM(Tran.TransTotal) as TotalSum, MIN(Tran.TransNumItems) as MinItems;
Cus_1 = FOREACH Cus GENERATE ID, Name, Salary;
Result = JOIN Cus_1 BY ID, Tran_2 By CustID;
STORE Result INTO 'output2.csv' USING PigStorage(',');
