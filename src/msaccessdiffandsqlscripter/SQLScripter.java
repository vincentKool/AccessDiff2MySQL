package msaccessdiffandsqlscripter;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 *
 * @author Vincent Kool
 */
public class SQLScripter {
PrintWriter printWriter;
    SQLScripter(PrintWriter pw) {
        printWriter = pw;
    }
    
    public void script(String table, HashMap<String, Object> changeMap){
//        String[] tables = changeMap.keySet().toArray(new String[changeMap.size()]);
//        for(String table: tables){
            String[] keyFields      = (String[])  changeMap.get("IDX");
            ArrayList createList    = (ArrayList) changeMap.get("create");
            ArrayList deleteList    = (ArrayList) changeMap.get("delete");
            ArrayList altList       = (ArrayList) changeMap.get("alter");
            
            if(deleteList != null) scriptDelete(table, deleteList, keyFields);
            if(altList    != null) scriptAlter(table, altList, keyFields);
            if(createList != null) scriptCreate(table, createList);
//        }
    }
    
    public void scriptCreate(String table, ArrayList<HashMap> addList){
        HashMap item1 = (HashMap<String, String>) addList.get(0);
        Set<String> keys = item1.keySet();
        String queryStart = "INSERT INTO "+table+" (`";
        String query = "";
        
        for(String key: keys){
            queryStart += key + "`, `";
        }
        queryStart = queryStart.substring(0, queryStart.length() - 3) + ") VALUES (";        
       
        for(HashMap<String, String> item: addList){
            query = queryStart; 
            for(String key: keys){
                query += escapeString(item.get(key)) + ", ";
            }
            query = query.substring(0, query.length() - 2) + ");";        
            printWriter.println(query);
//            System.out.println(query);
        }
        printWriter.flush();
    }

    public void scriptDelete(String table, ArrayList<HashMap> deleteList, String[] keyFields){
//        HashMap item1 = (HashMap<String, String>) deleteList.get(0);
//        Set<String> keys = item1.keySet();
        String queryStart = "DELETE FROM "+table+" WHERE ";
        String query = "";
        
        for(HashMap<String, String> item: deleteList){
            query = queryStart; 
            for(String key: keyFields){
                query += "`"+key+"` = "+escapeString(item.get(key)) + " AND ";
            }
            query = query.substring(0, query.length() - 5) + ";";        
            printWriter.println(query);
//            System.out.println(query);
        }
        printWriter.flush();
    }

    public void scriptAlter(String table, ArrayList<HashMap> altList, String[] keyFields){
        ArrayList<String> kFields = new ArrayList<>();
        for(String f:keyFields){ kFields.add(f);}
        HashMap item1 = (HashMap<String, String>) altList.get(0);
        Set<String> keys = item1.keySet();
        //UPDATE Persons SET Age=36 WHERE FirstName='Peter' AND LastName='Griffin'
        String queryStart = "UPDATE "+table+" SET ";
        String query = "";
        
        for(HashMap<String, String> item: altList){
            query = queryStart; 
            for(String key: keys){
                if(!kFields.contains(key)) query += "`"+key+ "`="+ escapeString(item.get(key))+" AND ";
            }
            query = query.substring(0, query.length() - 4) + " WHERE ";                    
            
            for(String key: keyFields){
                query += "`"+key+"` = '"+item.get(key) + "' AND ";
            }
            query = query.substring(0, query.length() - 5) + ";";        
            printWriter.println(query);
//            System.out.println(query);
        }
        printWriter.flush();
    }

    private String escapeString(String input){
        if(input == null) return "NULL";
        if(input.isEmpty()) return input;
        if(input.length() == 0) return input;
        input = input.replaceAll("'", "\\\'");
        return "'"+ input +"'"; 
    }
    
}
