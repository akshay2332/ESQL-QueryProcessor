package edu.stevens.dbms.generator;

import com.sun.codemodel.*;
import edu.stevens.dbms.utility.ReversePolishNotationCalculator;

import java.io.*;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;


public class QueryProcessor {


    private String DATABASE_QUERY_TABLE = "DATABASE_QUERY_TABLE";
    private String DATABASE_QUERY = "SELECT * FROM";
    private String DATABASE_CONNECTION_URL = "DATABASE_CONNECTION_URL";
    private String DATABASE_CONNECTION_PORT = "DATABASE_CONNECTION_PORT";
    private String DATABASE_NAME = "DATABASE_NAME";
    private String DATABASE_USERNAME = "DATABASE_USERNAME";
    private String DATABASE_PASSWORD = "DATABASE_PASSWORD";
    private String DATABASE_DRIVER = "DATABASE_DRIVER";
    private String QUERY_FILE_NAME = "QUERY_FILE_NAME";
    private String whereClause = null;
    private String[] selectAttributes = null;
    private String[] aggregateFunctionList = null;
    private String[] groupingVariablesConditions = null;
    private String[] groupingAttributes = null;
    private String[] groupingVariablesName = null;
    private String havingClause = null;
    private int groupingAttributesCount = 0;
    private int groupingVariables = 0;
    private Map<String, String> attributesMethod = new HashMap<>();
    private JCodeModel jCodeModel;
    private JDefinedClass jDefinedClass = null;
    private List<String> averageAggregateList;
    private boolean averageAggregateFlag = false;
    private ReversePolishNotationCalculator reversePolishNotationCalculator;
    private StringBuffer partitionConditionForEMF = null;
    private static final String PACKAGE_NAME = "edu.stevens.dbms.queryengine";
    private static final String DB_CONFIG_FILE_NAME = "databaseconfig.properties";


    public QueryProcessor() {
        this.jCodeModel = new JCodeModel();
        averageAggregateList = new ArrayList<>();
    }

    private void getTablesMetaData() {

        try {
            Class.forName(this.DATABASE_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        Connection connection;
        this.DATABASE_CONNECTION_URL = this.DATABASE_CONNECTION_URL.concat(":").concat(this.DATABASE_CONNECTION_PORT).concat("/").concat(this.DATABASE_NAME);
        try {
            connection = DriverManager.getConnection(this.DATABASE_CONNECTION_URL, this.DATABASE_USERNAME, this.DATABASE_PASSWORD);
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet columnDetails = databaseMetaData.getColumns(null, null, this.DATABASE_QUERY_TABLE, null);
            /*
            *
            *   -7	BIT
                -6	TINYINT
                -5	BIGINT
                -4	LONGVARBINARY
                -3	VARBINARY
                -2	BINARY
                -1	LONGVARCHAR
                0	NULL
                1	CHAR
                2	NUMERIC
                3	DECIMAL
                4	INTEGER
                5	SMALLINT
                6	FLOAT
                7	REAL
                8	DOUBLE
                12	VARCHAR
                91	DATE
                92	TIME
                93	TIMESTAMP
            *
            *
            * */

            while (columnDetails.next()) {
                String columnName = columnDetails.getString("COLUMN_NAME");
                int columnDataType = columnDetails.getInt("DATA_TYPE");

                switch (columnDataType) {
                    case -7: {
                        this.attributesMethod.put(columnName, "getBoolean");
                        break;
                    }
                    case -1: {
                        this.attributesMethod.put(columnName, "getString");
                        break;
                    }
                    case 1: {
                        this.attributesMethod.put(columnName, "getString");
                        break;
                    }
                    case 12: {
                        this.attributesMethod.put(columnName, "getString");
                        break;
                    }
                    case 91: {
                        this.attributesMethod.put(columnName, "getDate");
                        break;
                    }
                    default: {
                        this.attributesMethod.put(columnName, "getDouble");
                        break;
                    }
                }
            }
            reversePolishNotationCalculator = new ReversePolishNotationCalculator(jCodeModel, attributesMethod);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }


    }


    public static void main(String args[]) {

        QueryProcessor queryProcessor = new QueryProcessor();
        queryProcessor.readDatabaseProperties();
        queryProcessor.getTablesMetaData();
        //queryProcessor.readFileContent();
        queryProcessor.readMFFileContent();

        JPackage jPackage = queryProcessor.jCodeModel._package(PACKAGE_NAME);

        try {

            /*
             *   Creating the class with the name QueryOptimizer
             *   class created in the package edu.stevens.dbms.generated
             * */
            queryProcessor.jDefinedClass = jPackage._class("QueryOptimizer");
            queryProcessor.jDefinedClass.javadoc().add("Query Processing engine.");

            JExpression connectionURLValue = JExpr.lit(queryProcessor.DATABASE_CONNECTION_URL);
            JFieldVar connectionURlVariable = queryProcessor.createVariable(JMod.PRIVATE | JMod.FINAL | JMod.STATIC, String.class, "connectionURL", connectionURLValue);
            JFieldVar averageAggregateFlag = queryProcessor.createVariable(JMod.PRIVATE, boolean.class, "averageAggregateFlag", JExpr.lit(false));
            JFieldVar averageAggregateList = queryProcessor.createVariable(JMod.PRIVATE, List.class, "averageAggregateList", JExpr._null());
            averageAggregateList.type(queryProcessor.jCodeModel.ref(List.class).narrow(String.class));

            JFieldVar totalScan = queryProcessor.createVariable(JMod.PRIVATE, int.class, "totalScan", JExpr.lit((1 + queryProcessor.groupingVariables)));


            /*
             *   Creating the mf table
             *   Map<String,Map<String,String>
             *   key is attributes separated by ~
             *   eg cust~prod~city
             * */

            JType mfTableType;
            JType retrieveMfTye;

            JFieldVar mfTable = queryProcessor.createVariable(JMod.PRIVATE, Map.class, "mfTable", null);
            retrieveMfTye = queryProcessor.jCodeModel.ref(Map.class).narrow(queryProcessor.jCodeModel.ref(String.class), queryProcessor.jCodeModel.ref(String.class));
            mfTableType = queryProcessor.jCodeModel.ref(Map.class).narrow(queryProcessor.jCodeModel.ref(String.class)).narrow(retrieveMfTye);
            mfTable.type(mfTableType);

            JBlock constructorBody = queryProcessor.jDefinedClass.constructor(JMod.PUBLIC).body();
            constructorBody.assign(mfTable, JExpr._new(queryProcessor.jCodeModel.ref(HashMap.class).narrow(queryProcessor.jCodeModel.ref(String.class)).narrow(retrieveMfTye)));
            constructorBody.assign(averageAggregateList, JExpr._new(queryProcessor.jCodeModel.ref(ArrayList.class).narrow(String.class)));

            // Add get method
            JMethod getter = queryProcessor.createMethod(JMod.PUBLIC, connectionURlVariable.type(), "getConnectionURL");
            getter.body()._return(connectionURlVariable);
            getter.javadoc().add("Returns the ConnectionURL.");
            getter.javadoc().addReturn().add(connectionURlVariable.name());


            /*
             *   connect method to check the connection to database
             * */
            JMethod connectMethod = queryProcessor.createMethod(JMod.PRIVATE, queryProcessor.jCodeModel.VOID, "connect");
            connectMethod.javadoc().add("Testing the connection of database");
            JBlock jBlock = connectMethod.body();

            JTryBlock jTryBlock = jBlock._try();
            jTryBlock.body().add(queryProcessor.accessStaticMethod(Class.class, "forName").arg(queryProcessor.DATABASE_DRIVER));

            JCatchBlock jCatchBlock = jTryBlock._catch(queryProcessor.jCodeModel.ref(Exception.class));

            JVar exceptionVariable = jCatchBlock.param("exception");
            jCatchBlock.body().add(queryProcessor.accessNonStaticMethod(exceptionVariable, "printStackTrace"));



            /*
             *   display mf table according to having clause and selection statement
             * */
            JMethod displayMfTable = queryProcessor.createMethod(JMod.PRIVATE, queryProcessor.jCodeModel.VOID, "displayMfTable");
            connectMethod.javadoc().add("Testing the connection of database");
            JBlock displayMfTableBlock = displayMfTable.body();

            for (String sel : queryProcessor.selectAttributes) {
                sel = sel.trim();
                displayMfTableBlock.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "print").arg(
                        queryProcessor.jCodeModel.ref(String.class).staticInvoke("format").arg("%-15s").arg(sel));

                displayMfTableBlock.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "print").arg("\t");
            }
            displayMfTableBlock.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "println").arg("");


            JVar mfTableKeysToDisplay = displayMfTableBlock.decl(queryProcessor.jCodeModel.ref(Set.class).narrow(String.class), "mapKeys",
                    mfTable.invoke("keySet"));
            JVar mfTableColumns = displayMfTableBlock.decl(queryProcessor.jCodeModel.ref(Map.class).narrow(String.class).narrow(String.class),
                    "mfAttrMap", JExpr._null());
            JForEach mfTableForEach = displayMfTableBlock.forEach(queryProcessor.jCodeModel.ref(String.class), "key", mfTableKeysToDisplay);
            JBlock mfTableForEachBlock = mfTableForEach.body();
            mfTableForEachBlock.assign(mfTableColumns, mfTable.invoke("get").arg(mfTableForEach.var()));
            // if having clause true

            if (queryProcessor.havingClause == null || "".equalsIgnoreCase(queryProcessor.havingClause)) {
                queryProcessor.havingClause = "true & true";
            }

            //JBlock mfTableForEachBlock_if = mfTableForEachBlock._if(jExpression)._then();
            JBlock havingClauseMfTableDisplay = mfTableForEachBlock._if(queryProcessor.reversePolishNotationCalculator.performReversePolishNotation(queryProcessor.havingClause))._then();
            // split key

            JVar attributesKeys = havingClauseMfTableDisplay.decl(queryProcessor.jCodeModel.ref(String[].class), "attributesKeys", mfTableForEach.var().invoke("split").arg("~"));

            for (String selection : queryProcessor.selectAttributes) {
                selection = selection.trim();
                boolean flag = false;

                for (int i = 0; i < queryProcessor.groupingAttributesCount; i++) {
                    if (selection.equalsIgnoreCase(queryProcessor.groupingAttributes[i])) {
                        havingClauseMfTableDisplay.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "print").arg(
                                queryProcessor.jCodeModel.ref(String.class).staticInvoke("format").arg("%-15s").arg(attributesKeys.component(JExpr.lit(i))

                                )
                        );
                        havingClauseMfTableDisplay.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "print").arg("\t");
                        flag = true;
                        break;
                    }
                }
                if (!flag) {

                    if (selection.contains("count") || selection.contains("avg") || selection.contains("sum")
                            || selection.contains("max") || selection.contains("min")) {

                        havingClauseMfTableDisplay.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "print").arg(
                                queryProcessor.jCodeModel.ref(String.class).staticInvoke("format").arg("%-15s").arg(
                                        JOp.cond(mfTableColumns.invoke("get").arg(selection).ne(JExpr._null()), mfTableColumns.invoke("get").arg(selection), JExpr.lit("0"))
                                )
                        );
                    } else {

                        havingClauseMfTableDisplay.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "print").arg(
                                queryProcessor.jCodeModel.ref(String.class).staticInvoke("format").arg("%-15s").arg(
                                        JOp.cond(mfTableColumns.invoke("get").arg(selection).ne(JExpr._null()), mfTableColumns.invoke("get").arg(selection), JExpr.lit(""))
                                )
                        );
                    }


                    havingClauseMfTableDisplay.invoke(queryProcessor.jCodeModel.ref(System.class).staticRef("out"), "print").arg("\t");
                }
            }
            havingClauseMfTableDisplay.add(queryProcessor.printScreen());


            /*
             *   method for carrying average
             *
             *
             * */

            JMethod averageMethod = queryProcessor.createMethod(JMod.PRIVATE, queryProcessor.jCodeModel.VOID, "averageMethod");

            JBlock averageMethodBlock = averageMethod.body();

            JVar mapKeys = averageMethodBlock.decl(queryProcessor.jCodeModel.ref(Set.class).narrow(String.class), "mapKeys", mfTable.invoke("keySet"));

            JVar mfAttrMap = averageMethodBlock.decl(queryProcessor.jCodeModel.ref(Map.class).narrow(String.class).narrow(String.class), "mfAttrMap", JExpr._null());


            JForEach case_0_avgAggrForeach = averageMethodBlock.forEach(queryProcessor.jCodeModel.ref(String.class), "key", mapKeys);

            JBlock case_0_avgAggForEachBody = case_0_avgAggrForeach.body();
            case_0_avgAggForEachBody.assign(mfAttrMap, mfTable.invoke("get").arg(case_0_avgAggrForeach.var()));

            JForEach case_0_avgAggForEachBodyForEach = case_0_avgAggForEachBody.forEach(queryProcessor.jCodeModel.ref(String.class), "avgKeys", averageAggregateList);

            JBlock case_0_avgAggForEachBodyForEachBlock = case_0_avgAggForEachBodyForEach.body();
            JVar avgDtlsArray = case_0_avgAggForEachBodyForEachBlock.decl(queryProcessor.jCodeModel.ref(String[].class), "avgDtlsArray", case_0_avgAggForEachBodyForEach.var().invoke("split").arg("_"));

            JVar sumAvgValue = case_0_avgAggForEachBodyForEachBlock.decl(queryProcessor.jCodeModel.ref(String.class), "sumAvgValue", JExpr._null());

            case_0_avgAggForEachBodyForEachBlock.assign(sumAvgValue, JExpr.lit("sum_").plus(avgDtlsArray.component(JExpr.lit(1))).plus(JExpr.lit("_")).plus(avgDtlsArray.component(JExpr.lit(2))));

            case_0_avgAggForEachBodyForEachBlock.assign(sumAvgValue, mfAttrMap.invoke("get").arg(sumAvgValue));

            JVar countAvgValue = case_0_avgAggForEachBodyForEachBlock.decl(queryProcessor.jCodeModel.ref(String.class), "countAvgValue", JExpr._null());
            case_0_avgAggForEachBodyForEachBlock.assign(countAvgValue, JExpr.lit("count_").plus(avgDtlsArray.component(JExpr.lit(1))).plus(JExpr.lit("_")).plus(avgDtlsArray.component(JExpr.lit(2))));
            case_0_avgAggForEachBodyForEachBlock.assign(countAvgValue, mfAttrMap.invoke("get").arg(countAvgValue));

            JVar averageOfColumn = case_0_avgAggForEachBodyForEachBlock.decl(queryProcessor.jCodeModel.DOUBLE, "averageOfColumn", JExpr.lit(0));

            JBlock case_0_avgAggForEachBodyForEachBlock_if = case_0_avgAggForEachBodyForEachBlock._if(sumAvgValue.ne(JExpr._null()).cand(sumAvgValue.invoke("isEmpty").not()).
                    cand(countAvgValue.ne(JExpr._null()).cand(countAvgValue.invoke("isEmpty").not())))._then();

            case_0_avgAggForEachBodyForEachBlock_if.assign(averageOfColumn,
                    queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg(sumAvgValue).
                            div(queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg(countAvgValue)));

            // 3 decimal rounding
            case_0_avgAggForEachBodyForEachBlock_if.add(mfAttrMap.invoke("put").arg(case_0_avgAggForEachBodyForEach.var()).arg(

                    queryProcessor.accessStaticMethod(String.class, "valueOf").arg(
                            JExpr._new(queryProcessor.jCodeModel.ref(DecimalFormat.class)).arg("#.00#").invoke("format").arg(averageOfColumn))
            ));

            case_0_avgAggForEachBody.add(mfTable.invoke("put").arg(case_0_avgAggrForeach.var()).arg(mfAttrMap));

            // method ends here

            /*
             *   method for performing aggregation of grouping variables for mf table map
             *
             * */
            JMethod performAggregation = queryProcessor.createMethod(JMod.PRIVATE, queryProcessor.jCodeModel.ref(Map.class).
                            narrow(queryProcessor.jCodeModel.ref(String.class), queryProcessor.jCodeModel.ref(String.class)),
                    "performAggregationGroupingVariables");
            performAggregation._throws(SQLException.class);

            JVar resultSetMethodVar = performAggregation.param(queryProcessor.jCodeModel.ref(ResultSet.class), "resultSet");

            JVar aggregationList = performAggregation.param(queryProcessor.jCodeModel.ref(List.class).
                    narrow(queryProcessor.jCodeModel.ref(String.class)), "aggregationList");

            JVar mfAggregationMap = performAggregation.param(queryProcessor.jCodeModel.ref(Map.class).
                    narrow(queryProcessor.jCodeModel.ref(String.class), queryProcessor.jCodeModel.ref(String.class)), "mfAggregationMap");

            JBlock performAggregationBody = performAggregation.body();

            performAggregationBody._if(mfAggregationMap.eq(JExpr._null()))._then().assign(mfAggregationMap,
                    JExpr._new(queryProcessor.jCodeModel.ref(HashMap.class).narrow(
                            queryProcessor.jCodeModel.ref(String.class), queryProcessor.jCodeModel.ref(String.class)
                    )));

            JConditional ifLoopPerformAggregation = performAggregationBody._if(aggregationList.eq(JExpr._null()).cor(aggregationList.invoke("size").eq(JExpr.lit(0))));
            ifLoopPerformAggregation._then()._return(mfAggregationMap);

            JBlock elseIfBlockPerformAggregation = ifLoopPerformAggregation._else();

            JVar aggregateFunctionDesc = elseIfBlockPerformAggregation.decl(queryProcessor.jCodeModel.ref(String[].class),
                    "aggregateFunctionDesc", JExpr._null());

            JVar aggregatationFunction = elseIfBlockPerformAggregation.decl(queryProcessor.jCodeModel.ref(String.class),
                    "aggregateFunction", JExpr._null());

            JVar aggregateColumn = elseIfBlockPerformAggregation.decl(queryProcessor.jCodeModel.ref(String.class),
                    "aggregateColumn", JExpr._null());


            elseIfBlockPerformAggregation.directStatement("/*   Code for carrying out the actual aggregation function */");

            JForEach forEachLoopElseIfPerformAggregation = elseIfBlockPerformAggregation.forEach(queryProcessor.jCodeModel.ref(String.class), "aggregate", aggregationList);

            JBlock forBlockElseIfPerformAggregation = forEachLoopElseIfPerformAggregation.body();

            forBlockElseIfPerformAggregation.assign(forEachLoopElseIfPerformAggregation.var(), forEachLoopElseIfPerformAggregation.var().invoke("trim"));
            forBlockElseIfPerformAggregation.assign(aggregateFunctionDesc, forEachLoopElseIfPerformAggregation.var().invoke("split").arg("_").arg(JExpr.lit(-1)));

            forBlockElseIfPerformAggregation.assign(aggregatationFunction, aggregateFunctionDesc.component(JExpr.lit(0)));
            forBlockElseIfPerformAggregation.assign(aggregateColumn, aggregateFunctionDesc.component(JExpr.lit(1)));

            JSwitch jSwitchforBlockElseIfPerformAggregation = forBlockElseIfPerformAggregation._switch(aggregatationFunction);

            JBlock case_sum_Body = jSwitchforBlockElseIfPerformAggregation._case(JExpr.lit("sum")).body();
            JVar sum = case_sum_Body.decl(queryProcessor.jCodeModel.ref(String.class), "sum");
            case_sum_Body.assign(sum,

                    queryProcessor.accessStaticMethod(String.class, "valueOf").arg(resultSetMethodVar.invoke("getDouble").arg(aggregateColumn)));

            JVar sumFromMap = case_sum_Body.decl(queryProcessor.jCodeModel.ref(String.class), "sumFromMap", mfAggregationMap.invoke("get").arg(forEachLoopElseIfPerformAggregation.var()));

            JConditional caseSumIf = case_sum_Body._if(sumFromMap.eq(JExpr._null()));

            JBlock caseSumIfBlock = caseSumIf._then();
            caseSumIfBlock.add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).arg(sum));

            JBlock caseSumElseBlock = caseSumIf._else();

            JVar doubleSum = caseSumElseBlock.decl(queryProcessor.jCodeModel.DOUBLE, "doubleSum",
                    queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg(sumFromMap));

            caseSumElseBlock.assignPlus(doubleSum, queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg(sum));

            caseSumElseBlock.add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).arg(
                    queryProcessor.accessStaticMethod(String.class, "valueOf").arg(doubleSum)));
            case_sum_Body._break();


            JBlock case_max_Body = jSwitchforBlockElseIfPerformAggregation._case(JExpr.lit("max")).body();
            JVar maxFromMap = case_max_Body.decl(queryProcessor.jCodeModel.ref(String.class), "maxFromMap",
                    mfAggregationMap.invoke("get").arg(forEachLoopElseIfPerformAggregation.var()));
            JVar maxFromDb = case_max_Body.decl(queryProcessor.jCodeModel.DOUBLE, "maxFromDb",
                    resultSetMethodVar.invoke("getDouble").arg(aggregateColumn));
            JConditional caseMaxIf = case_max_Body._if(maxFromMap.eq(JExpr._null()));
            JBlock caseMaxIfBlock = caseMaxIf._then();
            caseMaxIfBlock.add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).
                    arg(queryProcessor.accessStaticMethod(String.class, "valueOf").arg(maxFromDb)));
            JBlock caseMaxElseBlock = caseMaxIf._else();
            JConditional caseMaxElseIf = caseMaxElseBlock._if(maxFromDb.gt(queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg(maxFromMap)));
            caseMaxElseIf._then().add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).arg(
                    queryProcessor.accessStaticMethod(String.class, "valueOf").arg(maxFromDb)));
            case_max_Body._break();


            JBlock case_min_Body = jSwitchforBlockElseIfPerformAggregation._case(JExpr.lit("min")).body();
            JVar minFromMap = case_min_Body.decl(queryProcessor.jCodeModel.ref(String.class), "minFromMap",
                    mfAggregationMap.invoke("get").arg(forEachLoopElseIfPerformAggregation.var()));
            JVar minFromDb = case_min_Body.decl(queryProcessor.jCodeModel.DOUBLE, "minFromDb",
                    resultSetMethodVar.invoke("getDouble").arg(aggregateColumn));
            JConditional caseMinIf = case_min_Body._if(minFromMap.eq(JExpr._null()));
            JBlock caseMinIfBlock = caseMinIf._then();
            caseMinIfBlock.add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).
                    arg(queryProcessor.accessStaticMethod(String.class, "valueOf").arg(minFromDb)));
            JBlock caseMinElseBlock = caseMinIf._else();
            JConditional caseMinElseIf = caseMinElseBlock._if(minFromDb.lt(queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg(minFromMap)));
            caseMinElseIf._then().add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).arg(
                    queryProcessor.accessStaticMethod(String.class, "valueOf").arg(minFromDb)));
            case_min_Body._break();


            JBlock case_count_Body = jSwitchforBlockElseIfPerformAggregation._case(JExpr.lit("count")).body();

            JVar countFromMap = case_count_Body.decl(queryProcessor.jCodeModel.ref(String.class), "countFromMap",
                    mfAggregationMap.invoke("get").arg(forEachLoopElseIfPerformAggregation.var()));

            JConditional caseCountIf = case_count_Body._if(countFromMap.eq(JExpr._null()));
            caseCountIf._then().add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).arg(JExpr.lit("1")));
            JBlock caseCountElseBlock = caseCountIf._else();

            caseCountElseBlock.add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).arg(
                    queryProcessor.accessStaticMethod(String.class, "valueOf").arg(queryProcessor.accessStaticMethod(Integer.class, "parseInt").arg(countFromMap).plus(JExpr.lit(1)))));
            case_count_Body._break();


            JBlock avgCaseBlock = jSwitchforBlockElseIfPerformAggregation._case(JExpr.lit("avg")).body();
            avgCaseBlock._break();


            JBlock defaultCaseBlock = jSwitchforBlockElseIfPerformAggregation._default().body();
            defaultCaseBlock.decl(queryProcessor.jCodeModel.ref(String[].class), "aggrAray", forEachLoopElseIfPerformAggregation.var().invoke("split").arg("_").arg(JExpr.lit(-1)));
            defaultCaseBlock.add(mfAggregationMap.invoke("put").arg(forEachLoopElseIfPerformAggregation.var()).arg(

                    resultSetMethodVar.invoke("getString").arg(JExpr.ref("aggrAray").component(JExpr.lit(0)))
            ));
            defaultCaseBlock._break();

            performAggregationBody._return(mfAggregationMap);


            /*
             *   execute the query
             *
             * */
            JMethod retrieve = queryProcessor.createMethod(JMod.PRIVATE, queryProcessor.jCodeModel.VOID, "retrieve");

            /*
             *   retrieve method body starts here
             *
             * */
            JBlock retrieveMethodBlock = retrieve.body();
            JVar queryString = retrieveMethodBlock.decl(queryProcessor.jCodeModel.ref(String.class), "queryString", JExpr.lit(queryProcessor.DATABASE_QUERY.concat(" ").concat(queryProcessor.DATABASE_QUERY_TABLE)));

            // adding try block
            JTryBlock retrieveTryBlock = retrieveMethodBlock._try();

            // body of try block
            JBlock retrieveBlockInsideTry = retrieveTryBlock.body();

            JVar connection = retrieveBlockInsideTry.decl(queryProcessor.jCodeModel.ref(Connection.class), "connection", JExpr._null());
            retrieveBlockInsideTry.assign(connection, queryProcessor.accessStaticMethod(DriverManager.class, "getConnection").
                    arg(connectionURlVariable).arg(JExpr.lit(queryProcessor.DATABASE_USERNAME)).arg(JExpr.lit(queryProcessor.DATABASE_PASSWORD)));

            JVar statement = retrieveBlockInsideTry.decl(queryProcessor.jCodeModel.ref(Statement.class), "statement", JExpr._null());
            retrieveBlockInsideTry.assign(statement, queryProcessor.accessNonStaticMethod(connection, "createStatement"));

            JVar resultSet = retrieveBlockInsideTry.decl(queryProcessor.jCodeModel.ref(ResultSet.class), "resultSet", JExpr._null());

            JVar resultPresent = retrieveBlockInsideTry.decl(queryProcessor.jCodeModel.BOOLEAN, "resultPresent", JExpr.lit(false));

            JVar mfTempMap = retrieveBlockInsideTry.decl(queryProcessor.jCodeModel.ref(Map.class), "mfAttrMap", JExpr._null());
            mfTempMap.type(retrieveMfTye);
            JVar keyOfMFTable = retrieveBlockInsideTry.decl(queryProcessor.jCodeModel.ref(String.class), "keyOfMFTable");

            /*
             *   for loop for scan
             *
             * */
            JVar scan = retrieveBlockInsideTry.decl(queryProcessor.jCodeModel.INT, "scan");
            JForLoop scanloop = retrieveBlockInsideTry._for();
            scanloop.init(scan, JExpr.lit(0));
            scanloop.test(scan.lt(totalScan));
            scanloop.update(scan.incr());

            JBlock scanLoopBlock = scanloop.body();
            scanLoopBlock.assign(averageAggregateFlag, JExpr.lit(false));
            scanLoopBlock.assign(resultSet, queryProcessor.accessNonStaticMethod(statement, "executeQuery").arg(queryString));
            scanLoopBlock.assign(resultPresent, queryProcessor.accessNonStaticMethod(resultSet, "next"));
            scanLoopBlock.assign(mfTempMap, JExpr._null());

            JSwitch scanLoopSwitch = scanLoopBlock._switch(scan);
            JCase case_0 = scanLoopSwitch._case(JExpr.lit(0));
            JBlock case_0_Body = case_0.body();


            /*
             * Find the aggregate functions for case 0
             * */
            case_0_Body.directStatement("/*Find the aggregare functions for case 0*/");

            /*
             *   Defining the condition to be handled for case 0 in while loop
             *
             * */

            JVar aggregateFunctionForGV0 = case_0_Body.decl(queryProcessor.jCodeModel.ref(List.class).narrow(queryProcessor.jCodeModel.ref(String.class)),
                    "aggregateFunctionForGV0", JExpr._null());
            List<String> aggregateFunctionList0 = queryProcessor.fetchAggregateFunctionList("O");
            if (aggregateFunctionList0 == null || aggregateFunctionList0.size() == 0) {
                case_0_Body.assign(aggregateFunctionForGV0, JExpr._null());
            } else {
                case_0_Body.assign(aggregateFunctionForGV0, queryProcessor.accessStaticMethod(Arrays.class, "asList").arg(queryProcessor.addValueToList(aggregateFunctionList0)));
            }

            if (queryProcessor.averageAggregateFlag) {
                case_0_Body.assign(averageAggregateFlag, JExpr.lit(true));
                case_0_Body.assign(averageAggregateList, queryProcessor.accessStaticMethod(Arrays.class, "asList").
                        arg(queryProcessor.addValueToList(queryProcessor.averageAggregateList)));
                queryProcessor.averageAggregateList.clear();
            } else {
                case_0_Body.assign(averageAggregateFlag, JExpr.lit(false));

            }
            /*
             *   While loop for iterating over the result
             *
             * */

            JBlock whileLoopBlock = case_0_Body._while(resultPresent).body();

            if (queryProcessor.whereClause != null && !queryProcessor.whereClause.isEmpty()) {
                JBlock whileLoopBlockif = whileLoopBlock._if(queryProcessor.reversePolishNotationCalculator.performReversePolishNotation(queryProcessor.whereClause))._then();

                whileLoopBlockif.assign(keyOfMFTable, queryProcessor.formKeyGroupingAttributes(queryProcessor.groupingAttributes));
                whileLoopBlockif.assign(mfTempMap, mfTable.invoke("get").arg(keyOfMFTable));
                JBlock ifLoopBlock_case_0 = whileLoopBlockif._if(mfTempMap.eq(JExpr._null()))._then();
                ifLoopBlock_case_0.assign(mfTempMap, JExpr._new(queryProcessor.jCodeModel.ref(HashMap.class).narrow(queryProcessor.jCodeModel.ref(String.class), queryProcessor.jCodeModel.ref(String.class))));

                whileLoopBlockif.assign(mfTempMap, JExpr._this().invoke(performAggregation).arg(resultSet).arg(aggregateFunctionForGV0).arg(mfTempMap));

                for (String ga : queryProcessor.groupingAttributes) {
                    whileLoopBlockif.add(mfTempMap.invoke("put").arg(ga).arg(resultSet.invoke("getString").arg(ga)));
                }

                whileLoopBlockif.add(mfTable.invoke("put").arg(keyOfMFTable).arg(mfTempMap));
            } else {
                whileLoopBlock.assign(keyOfMFTable, queryProcessor.formKeyGroupingAttributes(queryProcessor.groupingAttributes));
                whileLoopBlock.assign(mfTempMap, mfTable.invoke("get").arg(keyOfMFTable));
                JBlock ifLoopBlock_case_0 = whileLoopBlock._if(mfTempMap.eq(JExpr._null()))._then();
                ifLoopBlock_case_0.assign(mfTempMap, JExpr._new(queryProcessor.jCodeModel.ref(HashMap.class).narrow(queryProcessor.jCodeModel.ref(String.class), queryProcessor.jCodeModel.ref(String.class))));
                whileLoopBlock.assign(mfTempMap, JExpr._this().invoke(performAggregation).arg(resultSet).arg(aggregateFunctionForGV0).arg(mfTempMap));
                for (String ga : queryProcessor.groupingAttributes) {
                    whileLoopBlock.add(mfTempMap.invoke("put").arg(ga).arg(resultSet.invoke("getString").arg(ga)));
                }

                whileLoopBlock.add(mfTable.invoke("put").arg(keyOfMFTable).arg(mfTempMap));
            }
            whileLoopBlock.assign(resultPresent, queryProcessor.accessNonStaticMethod(resultSet, "next"));


            /*
             *   While Loop ends here
             *
             * */

            JBlock case_0_AverageAggrBlock = case_0_Body._if(averageAggregateFlag)._then();
            case_0_AverageAggrBlock.add(JExpr._this().invoke(averageMethod));
            case_0_Body.assign(averageAggregateList, JExpr._null());
            case_0_Body._break();

            for (int i = 0; i < queryProcessor.groupingVariables; i++) {
                queryProcessor.averageAggregateFlag = false;

                List<String> aggregateFunctionListI = queryProcessor.fetchAggregateFunctionList(queryProcessor.groupingVariablesName[i]);

                List<String> variableAttributeList = queryProcessor.groupingVariablesDependencyChecking(i);

                if ((variableAttributeList.isEmpty()
                        || (variableAttributeList.size() == queryProcessor.groupingAttributes.length))
                        && ((queryProcessor.partitionConditionForEMF == null)
                        || (queryProcessor.partitionConditionForEMF.length() == 0))) {
                    /*
                     *   dependent on all grouping attributes
                     *   and no dependency on previous mf values
                     * */

                    /*
                     *   While loop for iterating over the result
                     *
                     * */

                    JCase jCase_i = scanLoopSwitch._case(JExpr.lit((i + 1)));
                    JBlock case_i_body = jCase_i.body();


                    JVar aggregateFunctionForGVI = case_i_body.decl(queryProcessor.jCodeModel.ref(List.class).narrow(queryProcessor.jCodeModel.ref(String.class)),
                            "aggregateFunctionForGV".concat(queryProcessor.groupingVariablesName[i]), JExpr._null());


                    if (aggregateFunctionListI == null || aggregateFunctionListI.size() == 0) {
                        case_i_body.assign(aggregateFunctionForGVI, JExpr._null());
                    } else {
                        case_i_body.assign(aggregateFunctionForGVI,
                                queryProcessor.accessStaticMethod(Arrays.class, "asList").arg(queryProcessor.addValueToList(aggregateFunctionListI)));
                    }


                    if (queryProcessor.averageAggregateFlag) {
                        case_i_body.assign(averageAggregateFlag, JExpr.lit(true));
                        case_i_body.assign(averageAggregateList, queryProcessor.accessStaticMethod(Arrays.class, "asList").
                                arg(queryProcessor.addValueToList(queryProcessor.averageAggregateList)));
                        queryProcessor.averageAggregateList.clear();
                    } else {
                        case_i_body.assign(averageAggregateFlag, JExpr.lit(false));
                    }


                    JBlock while_i_LoopBlock = case_i_body._while(resultPresent).body();


                    while_i_LoopBlock.assign(keyOfMFTable, queryProcessor.formKeyGroupingAttributes(queryProcessor.groupingAttributes));

                    JExpression variableCondition = queryProcessor.formGroupingVariableSelectionCondition(i);
                    JBlock case_i_body_if = while_i_LoopBlock._if(variableCondition)._then();
                    case_i_body_if.assign(mfTempMap, mfTable.invoke("get").arg(keyOfMFTable));
                    case_i_body_if.assign(mfTempMap, JExpr._this().invoke(performAggregation).arg(resultSet).arg(aggregateFunctionForGVI).arg(mfTempMap));
                    case_i_body_if.add(mfTable.invoke("put").arg(keyOfMFTable).arg(mfTempMap));

                    while_i_LoopBlock.assign(resultPresent, queryProcessor.accessNonStaticMethod(resultSet, "next"));

                    JBlock case_i_AverageAggrBlock = case_i_body._if(averageAggregateFlag)._then();
                    case_i_AverageAggrBlock.add(JExpr._this().invoke(averageMethod));
                    case_i_body.assign(averageAggregateList, JExpr._null());

                    case_i_body._break();
                } else {
                    JCase jCase_i = scanLoopSwitch._case(JExpr.lit((i + 1)));
                    JBlock case_i_body = jCase_i.body();


                    JVar aggregateFunctionForGVI = case_i_body.decl(queryProcessor.jCodeModel.ref(List.class).narrow(queryProcessor.jCodeModel.ref(String.class)),
                            "aggregateFunctionForGV".concat(queryProcessor.groupingVariablesName[i]), JExpr._null());


                    if (aggregateFunctionListI == null || aggregateFunctionListI.size() == 0) {
                        case_i_body.assign(aggregateFunctionForGVI, JExpr._null());
                    } else {
                        case_i_body.assign(aggregateFunctionForGVI,
                                queryProcessor.accessStaticMethod(Arrays.class, "asList").arg(queryProcessor.addValueToList(aggregateFunctionListI)));
                    }


                    if (queryProcessor.averageAggregateFlag) {
                        case_i_body.assign(averageAggregateFlag, JExpr.lit(true));
                        case_i_body.assign(averageAggregateList, queryProcessor.accessStaticMethod(Arrays.class, "asList").
                                arg(queryProcessor.addValueToList(queryProcessor.averageAggregateList)));
                        queryProcessor.averageAggregateList.clear();
                    } else {
                        case_i_body.assign(averageAggregateFlag, JExpr.lit(false));
                    }

                    JVar partialMfTable = case_i_body.decl(queryProcessor.jCodeModel.ref(Map.class), "partialMfTable");
                    partialMfTable.type(mfTableType);
                    case_i_body.assign(partialMfTable, JExpr._new(queryProcessor.jCodeModel.ref(HashMap.class).narrow(queryProcessor.jCodeModel.ref(String.class)).narrow(retrieveMfTye)));
                    case_i_body.assign(mfTempMap, JExpr._new(queryProcessor.jCodeModel.ref(HashMap.class).narrow(queryProcessor.jCodeModel.ref(String.class), queryProcessor.jCodeModel.ref(String.class))));
                    String array[] = variableAttributeList.toArray(new String[variableAttributeList.size()]);

                    JBlock while_i_LoopBlock = case_i_body._while(resultPresent).body();
                    while_i_LoopBlock.assign(keyOfMFTable, queryProcessor.formKeyGroupingAttributes(array));

                    JExpression variableCondition = queryProcessor.formGroupingVariableSelectionCondition(i);
                    JBlock case_i_body_if = while_i_LoopBlock._if(variableCondition)._then();

                    // dependency on MF table
                    // for each row in MF table check the current row of database

                    if (queryProcessor.partitionConditionForEMF != null && !(queryProcessor.partitionConditionForEMF.length() == 0)) {

                        JVar partialKeyToSearch = case_i_body_if.decl(queryProcessor.jCodeModel.ref(String.class), "partialKeyToSearch", JExpr.lit(""));
                        mapKeys = case_i_body_if.decl(queryProcessor.jCodeModel.ref(Set.class).narrow(String.class), "mapKeys", mfTable.invoke("keySet"));

                        JForEach case_i_avgAggrForeach = case_i_body_if.forEach(queryProcessor.jCodeModel.ref(String.class), "key", mapKeys);
                        JBlock case_i_avgAggForEachBody = case_i_avgAggrForeach.body();
                        case_i_avgAggForEachBody.assign(mfAttrMap, mfTable.invoke("get").arg(case_0_avgAggrForeach.var()));
                        case_i_avgAggForEachBody.assign(partialKeyToSearch, JExpr.lit(""));


                        JVar keysArray = case_i_avgAggForEachBody.decl(queryProcessor.jCodeModel.ref(String[].class), "keysArray", case_0_avgAggrForeach.var().invoke("split").arg("~").arg(JExpr.lit(-1)));

                        for (int k = 0; k < array.length; k++) {
                            int j;
                            for (j = 0; j < queryProcessor.groupingAttributes.length; j++) {
                                if (queryProcessor.groupingAttributes[j].equalsIgnoreCase(array[k])) {
                                    break;
                                }
                            }

                            if (k != (array.length - 1)) {
                                case_i_avgAggForEachBody.add((JStatement) partialKeyToSearch.assignPlus(keysArray.component(JExpr.lit(j))));
                                case_i_avgAggForEachBody.add((JStatement) partialKeyToSearch.assignPlus(JExpr.lit("~")));
                            } else {
                                case_i_avgAggForEachBody.add((JStatement) partialKeyToSearch.assignPlus(keysArray.component(JExpr.lit(j))));
                            }
                        }

                        JBlock matchingPartialKeyBlock = case_i_avgAggForEachBody._if(
                                mfAttrMap.ne(JExpr._null()).cand(partialKeyToSearch.invoke("equalsIgnoreCase").arg(keyOfMFTable))
                        )._then();

                        JBlock case_0_avgAggForEachBodyIf = matchingPartialKeyBlock._if(queryProcessor.reversePolishNotationCalculator.performReversePolishNotation(queryProcessor.partitionConditionForEMF.toString().trim()))._then();
                        case_0_avgAggForEachBodyIf.assign(mfTempMap, JExpr._this().invoke(performAggregation).arg(resultSet).arg(aggregateFunctionForGVI).arg(mfTempMap));
                        case_0_avgAggForEachBodyIf.add(mfTable.invoke("put").arg(case_0_avgAggrForeach.var()).arg(mfAttrMap));
                    } else {
                        case_i_body_if.assign(mfTempMap, partialMfTable.invoke("get").arg(keyOfMFTable));
                        case_i_body_if.assign(mfTempMap, JExpr._this().invoke(performAggregation).arg(resultSet).arg(aggregateFunctionForGVI).arg(mfTempMap));
                        case_i_body_if.add(partialMfTable.invoke("put").arg(keyOfMFTable).arg(mfTempMap));
                    }

                    while_i_LoopBlock.assign(resultPresent, queryProcessor.accessNonStaticMethod(resultSet, "next"));


                    if (queryProcessor.partitionConditionForEMF == null || queryProcessor.partitionConditionForEMF.length() == 0) {

                        JVar partialKeyToSearch = case_i_body.decl(queryProcessor.jCodeModel.ref(String.class), "partialKeyToSearch", JExpr.lit(""));
                        mapKeys = case_i_body.decl(queryProcessor.jCodeModel.ref(Set.class).narrow(String.class), "mapKeys", mfTable.invoke("keySet"));
                        case_0_avgAggrForeach = case_i_body.forEach(queryProcessor.jCodeModel.ref(String.class), "key", mapKeys);
                        case_0_avgAggForEachBody = case_0_avgAggrForeach.body();
                        case_0_avgAggForEachBody.assign(mfAttrMap, mfTable.invoke("get").arg(case_0_avgAggrForeach.var()));
                        case_0_avgAggForEachBody.assign(partialKeyToSearch, JExpr.lit(""));


                        JVar keysArray = case_0_avgAggForEachBody.decl(queryProcessor.jCodeModel.ref(String[].class), "keysArray", case_0_avgAggrForeach.var().invoke("split").arg("~").arg(JExpr.lit(-1)));

                        for (int k = 0; k < array.length; k++) {
                            if (k != (array.length - 1)) {
                                case_0_avgAggForEachBody.add((JStatement) partialKeyToSearch.assignPlus(keysArray.component(JExpr.lit(k))));
                                case_0_avgAggForEachBody.add((JStatement) partialKeyToSearch.assignPlus(JExpr.lit("~")));
                            } else {
                                case_0_avgAggForEachBody.add((JStatement) partialKeyToSearch.assignPlus(keysArray.component(JExpr.lit(k))));
                            }
                        }

                        case_0_avgAggForEachBody.add(mfAttrMap.invoke("putAll").arg(partialMfTable.invoke("get").arg(partialKeyToSearch)));
                        case_0_avgAggForEachBody.add(mfTable.invoke("put").arg(case_0_avgAggrForeach.var()).arg(mfAttrMap));
                    }


                    queryProcessor.partitionConditionForEMF = null;

                    JBlock case_i_AverageAggrBlock = case_i_body._if(averageAggregateFlag)._then();
                    case_i_AverageAggrBlock.add(JExpr._this().invoke(averageMethod));
                    case_i_body.assign(averageAggregateList, JExpr._null());

                    case_i_body._break();
                }
            }


            /*
             *  for loop ends here
             *
             * */

            // body of catch block of earlier try
            JCatchBlock retrieveCatchBlock = retrieveTryBlock._catch(queryProcessor.jCodeModel.ref(SQLException.class));
            exceptionVariable = retrieveCatchBlock.param("exception");
            retrieveCatchBlock.body().add(queryProcessor.accessNonStaticMethod(exceptionVariable, "printStackTrace"));


            /*
             *   creating main method
             *
             * */
            JMethod mainExecution = queryProcessor.createMethod(JMod.PUBLIC | JMod.STATIC, queryProcessor.jCodeModel.VOID, "main");
            mainExecution.param(String[].class, "args");
            JBlock mainExecutionBlock = mainExecution.body();

            /*
             *   creating object of the same class
             *
             * */
            JVar queryOptimizerClassObj = mainExecutionBlock.decl(queryProcessor.jDefinedClass, "queryOptimizer", JExpr._new(queryProcessor.jDefinedClass));

            /*
             *   calling the function with the object
             *
             * */
            mainExecutionBlock.invoke(queryOptimizerClassObj, connectMethod);
            mainExecutionBlock.invoke(queryOptimizerClassObj, retrieve);
            mainExecutionBlock.invoke(queryOptimizerClassObj, displayMfTable);

        } catch (JClassAlreadyExistsException e) {
            e.printStackTrace();
        }


        // Generate the code
        try {
            String useDir = System.getProperty("user.dir");
            useDir = useDir+"/src/main/java";
            System.out.println("File created at location = " + useDir + " in the package " + PACKAGE_NAME);
            queryProcessor.jCodeModel.build(new File(useDir));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public JInvocation accessStaticMethod(Class staticClass, String methodName) {
        JBlock staticBlock = new JBlock();
        return staticBlock.staticInvoke(jCodeModel.ref(staticClass), methodName);
    }

    private JInvocation accessNonStaticMethod(JExpression objectOnMethodInvoke, String methodName) {
        JBlock nonStaticBlock = new JBlock();
        return nonStaticBlock.invoke(objectOnMethodInvoke, methodName);
    }


    private JStatement printScreen() {
        JBlock jBlock = new JBlock();

        return jBlock.invoke(jCodeModel.ref(System.class).staticRef("out"), "println").arg("");
    }

    private JMethod createMethod(int modifierType, JType jType, String name) {
        return jDefinedClass.method(modifierType, jType, name);
    }


    private JFieldVar createVariable(int modifierType, Class classType, String nameOfVariable, JExpression variableValue) {
        return jDefinedClass.field(modifierType, classType, nameOfVariable, variableValue);
    }

    private JArray addValueToList(List<String> array) {

        JArray jArray = JExpr.newArray(jCodeModel.ref(String.class));

        for (String element : array
                ) {
            jArray.add(JExpr.lit(element));
        }

        return jArray;
    }

    private List<String> fetchAggregateFunctionList(String groupingVariableIdentity) {
        List<String> aggregateFunction = new ArrayList<>();
        if ((groupingVariableIdentity != null))
            if ((!Objects.equals(groupingVariableIdentity, ""))) {
                for (String element : aggregateFunctionList) {
                    element = element.trim();
                    if (element.contains(groupingVariableIdentity)) {
                        aggregateFunction.add(element);
                        if (element.contains("avg")) {
                            averageAggregateFlag = true;
                            averageAggregateList.add(element);
                            aggregateFunction.add(element.replace("avg", "count"));
                        }
                    }
                }

                for (String selection : selectAttributes) {
                    selection = selection.trim();
                    if (selection.contains(groupingVariableIdentity) && !aggregateFunction.contains(selection.trim())) {
                        aggregateFunction.add(selection);
                    }
                }


            }
        return aggregateFunction;
    }


    private void readDatabaseProperties() {

        Properties databaseProperties = new Properties();
        InputStream input;

        try {

            input = new FileInputStream(QueryProcessor.DB_CONFIG_FILE_NAME);

            // loading a properties file
            databaseProperties.load(input);
            DATABASE_QUERY_TABLE = databaseProperties.getProperty(DATABASE_QUERY_TABLE);
            DATABASE_CONNECTION_URL = databaseProperties.getProperty(DATABASE_CONNECTION_URL);
            DATABASE_CONNECTION_PORT = databaseProperties.getProperty(DATABASE_CONNECTION_PORT);
            DATABASE_NAME = databaseProperties.getProperty(DATABASE_NAME);
            DATABASE_USERNAME = databaseProperties.getProperty(DATABASE_USERNAME);
            DATABASE_PASSWORD = databaseProperties.getProperty(DATABASE_PASSWORD);
            DATABASE_DRIVER = databaseProperties.getProperty(DATABASE_DRIVER);
            QUERY_FILE_NAME = databaseProperties.getProperty(QUERY_FILE_NAME);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readMFFileContent() {
        Properties mfQueryProperties = new Properties();
        InputStream inputStream;

        try {
            inputStream = new FileInputStream(QUERY_FILE_NAME);
            // loading a properties file
            mfQueryProperties.load(inputStream);
            selectAttributes = mfQueryProperties.getProperty("Selection_Attribute").split(",", -1);
            groupingVariables = Integer.parseInt(mfQueryProperties.getProperty("Number_of_Grouping_Variables"));
            groupingVariablesName = new String[groupingVariables];
            groupingVariablesConditions = new String[groupingVariables];

            for (int i = 0; i < groupingVariables; i++) {
                groupingVariablesName[i] = String.valueOf((char) ('A' + i));

                groupingVariablesConditions[i] = mfQueryProperties.getProperty("SELECT_CONDITION_VECT_".concat(groupingVariablesName[i]));
                if (groupingVariablesConditions[i] == null || groupingVariablesConditions[i].isEmpty()) {
                    groupingVariablesConditions[i] = "true | true";
                }
            }

            groupingAttributes = mfQueryProperties.getProperty("Grouping_Attributes").split(",");
            groupingAttributesCount = groupingAttributes.length;
            aggregateFunctionList = mfQueryProperties.getProperty("F-VECT").split(",", -1);
            havingClause = mfQueryProperties.getProperty("HAVING_CONDITION");
            whereClause = mfQueryProperties.getProperty("WHERE_CLAUSE");
            if ("null".equalsIgnoreCase(whereClause)) {
                whereClause = null;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private JExpression formKeyGroupingAttributes(String[] keyArray) {
        int currentPointerGAArray;
        JExpression keyOfGroupingAttributes = null;
        int lengthOfCurrentGV = keyArray.length;
        for (currentPointerGAArray = 0; (currentPointerGAArray < lengthOfCurrentGV); currentPointerGAArray++) {
            if (keyOfGroupingAttributes == null) {
                keyOfGroupingAttributes = JExpr.ref("resultSet").invoke("getString").arg(keyArray[currentPointerGAArray]);
            } else {
                keyOfGroupingAttributes = keyOfGroupingAttributes.invoke("concat").arg(JExpr.ref("resultSet").invoke("getString").arg(keyArray[currentPointerGAArray]));
            }
            if (currentPointerGAArray != (lengthOfCurrentGV - 1)) {
                keyOfGroupingAttributes = keyOfGroupingAttributes.invoke("concat").arg("~");
            }
        }
        return keyOfGroupingAttributes;
    }


    private List<String> groupingVariablesDependencyChecking(int groupingVariable) {
        List<String> attributeDependencyOfVariable = new ArrayList<>();

        String groupingVariableCondition = groupingVariablesConditions[groupingVariable];

        System.out.println("grouping variable | " + groupingVariablesName[groupingVariable] + " | condition | " + groupingVariableCondition);


        for (String attributes : groupingAttributes) {
            String toVerify = this.groupingVariablesName[groupingVariable].concat("~").concat(attributes).concat(" = ").concat(attributes);
            if (groupingVariableCondition.contains(toVerify)) {
                attributeDependencyOfVariable.add(attributes);
                groupingVariableCondition = groupingVariableCondition.replace(toVerify,
                        "true");
            }
        }

        if ("true".equalsIgnoreCase(groupingVariableCondition.trim())) {
            groupingVariableCondition += " & true";
        }

        if ((attributeDependencyOfVariable.size() != groupingAttributes.length
                && attributeDependencyOfVariable.size() != 0)
                || groupingVariableCondition.contains("MF")) {


            while (groupingVariableCondition.contains("MF")) {

                if (groupingVariableCondition.contains("MF")) {
                    int subStringStartIndex = groupingVariableCondition.indexOf("MF");

                    int endSubStringIndex = groupingVariableCondition.indexOf("&", subStringStartIndex);
                    String toReplace;
                    if (endSubStringIndex == -1) {
                        toReplace = groupingVariableCondition.substring(subStringStartIndex, groupingVariableCondition.length());
                    } else {
                        toReplace = groupingVariableCondition.substring(subStringStartIndex, endSubStringIndex - 1);
                    }
                    groupingVariableCondition = groupingVariableCondition.replace(toReplace, "true");
                    if (partitionConditionForEMF == null || partitionConditionForEMF.length() == 0) {
                        partitionConditionForEMF = new StringBuffer(toReplace);
                    } else {
                        partitionConditionForEMF = partitionConditionForEMF.append(" & ").append(toReplace);
                    }

                }
            }

        }
        groupingVariablesConditions[groupingVariable] = groupingVariableCondition;

        return attributeDependencyOfVariable;

    }


    private JExpression formGroupingVariableSelectionCondition(int groupingVariable) {
        String condition = groupingVariablesConditions[groupingVariable];

        if (whereClause != null && !whereClause.isEmpty()) {
            condition = condition.trim().concat(" & ").concat(whereClause.replace("O", groupingVariablesName[groupingVariable]));
        }
        return reversePolishNotationCalculator.performReversePolishNotation(condition);


    }

}
