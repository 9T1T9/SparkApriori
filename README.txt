README
Required Running Environment:
Ubuntu
Spark 2.2.0

Download Apriori.py and test transaction file retail.txt 
In the terminal, go to directory where the project is stored.
Input the following command: spark-submit Apriori.py retail.txt.

Then supports of frequent itemsets are ouput.

retail.txt can be replaced by any other transaction files.

Because pyspark is combined into the program, the program can be easily adjusted to run on the distributed environment.  
