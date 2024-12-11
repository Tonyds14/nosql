# nosql
Installing Apache Cassandra on Windows in 2021.docx - for the application and version used and installation guide.
java coding exercise for nosql



Starting up cassandra db and testing connectivity
1. Start Cassandra DB
a. go to "C:\apache-cassandra-3.11.16\bin" - type in "cmd" in file explorer bar 
	-> to open cmd prompt mapped to "C:\apache-cassandra-3.11.16\bin"
b. type-in "Cassandra" then press enter.
c. type-in "cqlsh" then press enter
Note of the message below: 
"Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.11.16 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help."

Cluster Name: "Test Cluster"

Indicates a successful connection to the Cassandra cluster.
<Cluster Name> is the name of your Cassandra cluster as defined in the cassandra.yaml configuration file.

if checking Cassandra connectivity on different cmd session/new window, type in "cqlsh 127.0.0.1 9042"



Sample Run:

config.properties settings

![image](https://github.com/user-attachments/assets/356394de-69aa-435e-a0b4-eadb96ed23dc)


Input excel

![image](https://github.com/user-attachments/assets/2fe4cb96-d39f-4b0b-9434-0cc49628467e)


Console Output:

![image](https://github.com/user-attachments/assets/c0c6c308-1b19-4722-856c-f41582869531)

Output log file

![image](https://github.com/user-attachments/assets/dad5ceaf-108a-4929-a8fd-dee877ba7c30)

validation thru cmd, cqlsh

![image](https://github.com/user-attachments/assets/85fb7689-4e20-4c5c-93d7-8c56d5e53e54)


