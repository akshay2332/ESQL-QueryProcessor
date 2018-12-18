# Dbms-ESQL
ESQL Query Processor for multi-feature queries on single database table.

# Installation guide :

Pre - Requisite
Maven installation should be on the system. Set variable M2_HOME and PATH for configuring the maven path. 

Steps to be performed are as follows
1) Perform the following action in the project directory in the command prompt.
mvn clean

2) After ending perform the following step in this project directory in the command prompt.
mvn package or mvn install

The above steps can be performed in a single step as follows
mvn clean install or mvn clean package

3) A jar will be generated in the target folder in the project directory with name QueryOptimizer-jar-with-dependencies.jar.Copy the jar to the user directory.

4) The JAR requires a property file for database information with the name databaseconfig.properties.

5) Mention the query properties file name in property name "QUERY_FILE_NAME" in databaseconfig.properties. Place the file "query.properties", file in the user directory.

6) Execute the following command to generate the query evaluation plan in current user directory.
java -jar QueryOptimizer-jar-with-dependencies.jar

7) The .java file for optimized plan will be generated at the following location.
${user.directory}/edu/stevens/dbms/queryengine/QueryOptimizer.java

8) This java file can be compiled with the following command.
javac edu/stevens/dbms/queryengine/QueryOptimizer.java

9) The database driver jar can be placed at the current user directory and file can be executed with the following command from current user directory.
# On Unix
java -cp .:postgresql-9.4-1206-jdbc42.jar edu.stevens.dbms.queryengine.QueryOptimizer

# On Windows
java -cp .:postgresql-9.4-1206-jdbc42.jar edu.stevens.dbms.queryengine.QueryOptimizer

