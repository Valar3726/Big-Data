Cus = LOAD '/user/hadoop/input/Customers.csv' USING PigStorage(',') AS (ID, Name, Age, CountryCode, Salary);
Cus_1 = FOREACH Cus GENERATE ID, CountryCode;
Cus_2 = GROUP Cus_1 BY CountryCode;
Cus_3 = FOREACH Cus_2 GENERATE group as CountryCode, COUNT(Cus_1) as Number;
Cus_4 = FILTER Cus_3 BY Number < 2000 or Number > 5000;
STORE Cus_4 INTO 'pig_output3' USING PigStorage(',');
