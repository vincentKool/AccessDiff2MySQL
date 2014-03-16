package msaccessdiffandsqlscripter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Vincent Kool
 */
public class AccesDB {

    private Connection DBCon;
    private String dbLocation;

    public void connectToDB(String dblocation) throws SQLException {
        if(!dblocation.isEmpty()) dbLocation = dblocation;
        try {
            // Classical JDBC-ODBC driver
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
            // Database connection string consists of driver with
            // string "dblocation" and set the database as readonly
            String db = "JDBC:ODBC:Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=";
            db += dblocation;
            db += ";READONLY=true;";

            DBCon = DriverManager.getConnection(db);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MSAccessDiffAndSQLScripter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void closeDB() throws SQLException {
        DBCon.close();
    }

    public ArrayList<String> getTables() throws Exception {
//        if (DBCon ==  null){
//            throw new Exception("DBCon is null in getTables");
//        }
        Statement stmt = createStatement();
        ArrayList<String> tables = new ArrayList<>();
        // Specify the type of object; in this case we want tables
        String[] types = {"TABLE"};
        ResultSet checkTable = DBCon.getMetaData().getTables(null, null, "%", types);
        String tableName = "";

        while (checkTable.next()) {
            tableName = checkTable.getString(3);
            tables.add(tableName);
        }
        
        stmt.close();
        return tables;
    }
    
    public ArrayList<String> getColumnsOfTable(String table) throws SQLException{
//        if(DBCon ==  null)      throw new SQLException("DBCon is null in getColumnsOfTable");
//        if(DBCon.isClosed())    connectToDB(dbLocation);
        
        ArrayList<String> returnArray = new ArrayList<>();
        Statement stmt = createStatement();
        String query = "SELECT * FROM "+table;
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        for(int i = 1; i < rsmd.getColumnCount(); i++){
            String name = rsmd.getColumnName(i);
            returnArray.add(name);
        }  
        return returnArray;
    }
    
    public String[] getIndexInfo(String table) throws SQLException{
//        if(DBCon ==  null)      throw new SQLException("DBCon is null in getColumnsOfTable");
//        if(DBCon.isClosed())    connectToDB(dbLocation);
        
        ArrayList<String> keylist = new ArrayList<>();
        
        DatabaseMetaData dmd = getMetaData();
        ResultSet rs = dmd.getIndexInfo(null, null, table, true, true);
        ResultSetMetaData rsmd = rs.getMetaData();
        String indexName = "";
        String columName = "";
        
        while(rs.next()){
            indexName = rs.getString(6);
            columName = rs.getString(9);
            if((indexName != null) && (indexName.equals("PrimaryKey"))){
                keylist.add(columName);
            }
        }
        keylist.trimToSize();
        return keylist.toArray(new String[keylist.size()]);
        
    }

    public HashMap getRecordsWithFields(String table, ArrayList<String> fields) throws NullPointerException, SQLException{
        // Test for invalid field input
        if(fields.isEmpty()) throw new NullPointerException("No fields where specified for table:"+table);

        // Create variables
        HashMap records = new HashMap();
        
        // Get KeyColums
        String[] keyColumns = getIndexInfo(table);
        Statement statement = createStatement();

        // Create part of query for the fields
        String queryFields = "";
        try {
            // Normal fields
            for (String field : fields) {
                queryFields += "["+ field + "], ";
            }
            queryFields = queryFields.substring(0, queryFields.length() - 2) + " ";
        } catch (StringIndexOutOfBoundsException e) {
            queryFields = "";
        }

        // Create part of the query for the keycolums
        String keyFields = ",";
        if(keyColumns.length > 0){
        try {
            // Key fields
            for (String field : keyColumns) {
                keyFields += "[" + field + "]&";
            }
            keyFields = keyFields.substring(0, keyFields.length() - 1) + " AS IDX";
        } catch (StringIndexOutOfBoundsException e) {
            keyFields = "";
        }
        } else {
            keyFields = "";
        }

        // Combine the query
        String query = "SELECT "+
                queryFields +
                keyFields +
                " FROM "+table ;
        ResultSet resultSet = statement.executeQuery(query);
        
        HashMap record = new HashMap();
        while (resultSet.next()) {
            record.clear();
            for (int i = 1; i <= fields.size(); i++) {
                record.put(fields.get(i-1), resultSet.getString(i));
            }
            records.put(resultSet.getString(fields.size()+1), record.clone());
        }
        
        // Return records
        return records;
    }
    
    private Statement createStatement() throws SQLException {
        if(DBCon ==  null)      throw new SQLException("DBCon is null in createStatement");
        if(DBCon.isClosed())    connectToDB(dbLocation);
        return DBCon.createStatement();
    }
    
    private DatabaseMetaData getMetaData() throws SQLException{
        if(DBCon ==  null)      throw new SQLException("DBCon is null in getMetaData");
        if(DBCon.isClosed())    connectToDB(dbLocation);
        return DBCon.getMetaData();
    }
}
