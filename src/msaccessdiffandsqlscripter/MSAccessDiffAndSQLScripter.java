package msaccessdiffandsqlscripter;

import java.awt.EventQueue;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Vincent Kool
 */
public class MSAccessDiffAndSQLScripter implements Runnable {
    private static AccesDB oldDB;
    private static AccesDB newDB;
    private static ArrayList<String> correspondingTables;
    private static HashMap structureChanges;
    private static HashMap <String, HashMap> contentChanges;
    private static GUI gui;
    private static String oldDBLocation,  newDBLocation;

    MSAccessDiffAndSQLScripter(String oldDBLocation, String newDBLocation, GUI gui) {
        this.gui = gui;
        this.oldDBLocation = oldDBLocation;
        this.newDBLocation = newDBLocation;
    }
    
    public void run()
    {
        // Running code
        // initialize neccesary variables
        structureChanges = new HashMap(30);
        ArrayList<String> correspondingFields = new ArrayList<>();
        int progress = 0;
        try {
            // Connect to the old DB
            oldDB = new AccesDB();
            oldDB.connectToDB(this.oldDBLocation);
            ArrayList<String> oldTables = oldDB.getTables();
            oldDB.closeDB();
           
            // Connect to the new DB
            newDB = new AccesDB();
            newDB.connectToDB(this.newDBLocation);
            ArrayList<String> newTables = newDB.getTables();
            newDB.closeDB();
            
            // Check is the database tables are the same
            // And if that is not the case find the tables that occur in both 
            // databases
            
            HashMap returnOfComparison = compareAL(oldTables, newTables);
            structureChanges.put("tables", returnOfComparison.get("dissimilar"));
            correspondingTables = (ArrayList<String>) returnOfComparison.get("similar");
            
            contentChanges = new HashMap(correspondingTables.size());
            
            // for each table find the differences, additions, changes or deletions 
            for (String table : correspondingTables){
                // Check for interrupt request
                final String t = table;
                final int p = progress;
                if(Thread.currentThread().isInterrupted()) break;
                EventQueue.invokeLater(new Runnable(){
                    public void run(){
                        gui.updateStatusBarAndTextArea((float) (p/(2.0*correspondingTables.size())), "analyze table: "+t);   
                    }
                });
                progress++;
                // Compare table structure
                returnOfComparison = compareAL(oldDB.getColumnsOfTable(table), newDB.getColumnsOfTable(table));
                structureChanges.put(table, returnOfComparison.get("dissimilar"));
                correspondingFields = (ArrayList<String>) returnOfComparison.get("similar");
                
                // Compare data in table (corresponding fields)
                compareTableWithFields(table, correspondingFields);
            }
            
            
            // This is how to get today's date in Java
        Date today = new Date();
     
      
        //formatting date in Java using SimpleDateFormat
        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("_yMMdd_HHmmSS_");
        String date = DATE_FORMAT.format(today);
//        System.out.println("Today in dd-MM-yyyy format : " + date);
//      
//        //Another Example of formatting Date in Java using SimpleDateFormat
//        DATE_FORMAT = new SimpleDateFormat("dd/MM/yy");
//        date = DATE_FORMAT.format(today);
//        System.out.println("Today in dd/MM/yy pattern : " + date);
//      
//        //formatting Date with time information
//        DATE_FORMAT = new SimpleDateFormat("dd-MM-yy:HH:mm:SS");
//        date = DATE_FORMAT.format(today);
//        System.out.println("Today in dd-MM-yy:HH:mm:SS : " + date);
//      
//        //SimpleDateFormat example - Date with timezone information
//        DATE_FORMAT = new SimpleDateFormat("dd-MM-yy:HH:mm:SS Z");
//        date = DATE_FORMAT.format(today);
//        System.out.println("Today in dd-MM-yy:HH:mm:SSZ : " + date);
            
            
            
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("script"+date+".sql")));
            SQLScripter sql = new SQLScripter(pw);
            // Create SQL from data
            for(String table: contentChanges.keySet()){
                final String t = table;
                final int p = progress;
                if(Thread.currentThread().isInterrupted()) break;
                EventQueue.invokeLater(new Runnable(){
                    public void run(){
                        gui.updateStatusBarAndTextArea((float) (p/(2.0*correspondingTables.size())), "create script of: "+t);   
                    }
                });
                progress++;
                sql.script(table, contentChanges.get(table));
//                sql.scriptCreate(table, (ArrayList) contentChanges.get(table).get("create"));
//                sql.scriptDelete(table, (ArrayList) contentChanges.get(table).get("delete"));
            }
            pw.close();
            pw = null;
                EventQueue.invokeAndWait(new Runnable(){
                    public void run(){
                        gui.updateStatusBarAndTextArea((float) 1.0, "Finished !!");   
                    }
                });            
            
            //System.exit(0);
            
            Logger.getLogger("net.weaponsystems.SQLScripter").info("-- einde");
        } catch (Exception ex) {
            Logger.getLogger(MSAccessDiffAndSQLScripter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) {
        String hallo = "hal'lo";
        hallo = hallo.replaceAll("'", "\\\'o");
        System.out.println(hallo);
    }

public static void doSomething(String OldDB, String NewDB){
// initialize neccesary variables
        structureChanges = new HashMap(30);
        ArrayList<String> correspondingFields = new ArrayList<>();
        int progress = 0;
        try {
            // Connect to the old DB
            oldDB = new AccesDB();
            oldDB.connectToDB(OldDB);
            ArrayList<String> oldTables = oldDB.getTables();
            oldDB.closeDB();
           
            // Connect to the new DB
            newDB = new AccesDB();
            newDB.connectToDB(NewDB);
            ArrayList<String> newTables = newDB.getTables();
            newDB.closeDB();
            
            // Check is the database tables are the same
            // And if that is not the case find the tables that occur in both 
            // databases
            
            HashMap returnOfComparison = compareAL(oldTables, newTables);
            structureChanges.put("tables", returnOfComparison.get("dissimilar"));
            correspondingTables = (ArrayList<String>) returnOfComparison.get("similar");
            
            contentChanges = new HashMap(correspondingTables.size());
//            correspondingTables.remove("")
//            correspondingTables.clear();
//            correspondingTables.add("hulp_class");
            
            // for each table find the differences, additions, changes or deletions 
            for (String table : correspondingTables){
                gui.updateStatusBar((float) (progress/(2.0*correspondingTables.size())));
                progress++;
                // Compare table structure
                returnOfComparison = compareAL(oldDB.getColumnsOfTable(table), newDB.getColumnsOfTable(table));
                structureChanges.put(table, returnOfComparison.get("dissimilar"));
                correspondingFields = (ArrayList<String>) returnOfComparison.get("similar");
                
                // Compare data in table (corresponding fields)
                compareTableWithFields(table, correspondingFields);
//                break;
            }
            
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("script.sql")));
            SQLScripter sql = new SQLScripter(pw);
            // Create SQL from data
            for(String table: contentChanges.keySet()){
                gui.updateStatusBar((float) (progress/(2.0*correspondingTables.size())));
                sql.script(table, contentChanges.get(table));
                progress++;
//                sql.scriptCreate(table, (ArrayList) contentChanges.get(table).get("create"));
//                sql.scriptDelete(table, (ArrayList) contentChanges.get(table).get("delete"));
            }
            pw.close();
            pw = null;
            System.exit(0);
            
            Logger.getLogger("net.weaponsystems.SQLScripter").info("-- einde");
        } catch (Exception ex) {
            Logger.getLogger(MSAccessDiffAndSQLScripter.class.getName()).log(Level.SEVERE, null, ex);
        }
}
    




    /**
     * @param args the command line arguments
     */
    public static void main2(String[] args) {
        doSomething("C:\\Users\\Vincent Kool\\Documents\\WeaponsystemsDB\\WSnet_oud.accdb", "C:\\Users\\Vincent Kool\\Documents\\WeaponsystemsDB\\WSnet_nieuw.accdb");
    }
    
    /**
     * compareAL compares two arrayLists for added, deleted and same items
     * 
     * @param oldTables
     * @param newTables
     * @return HashMap.similar -> ArrayList
     *         HashMap.dissmilar-> HashMap.add / removed
     */
    static HashMap compareAL(ArrayList<String> oldTables, ArrayList<String> newTables)
    {
        HashMap returnMap = new HashMap(4); 
        HashMap tableMap  = new HashMap(4);

        ArrayList<String> removedList   = new ArrayList<String>(oldTables);
        ArrayList<String> addedList     = new ArrayList<String>(newTables);
        ArrayList<String> sameList      = new ArrayList<String>(newTables);


        removedList.removeAll( newTables );
        addedList.removeAll( oldTables );
        sameList.removeAll( addedList );
        
        tableMap.put("add", addedList);
        tableMap.put("removed", removedList);
        
        returnMap.put("similar", sameList);
        returnMap.put("dissimilar", tableMap);
        return returnMap;
    }
    
    static void compareTableWithFields(String table, ArrayList<String> fields) throws SQLException{
        HashMap<String, HashMap> oldRecords;
        HashMap<String, HashMap> newRecords;
        HashMap<String, String> oldRecord;
        HashMap<String, String> newRecord;
        HashMap<String, Object> changeMap = new HashMap();
        ArrayList<HashMap> addRecords = new ArrayList<>();
        ArrayList<HashMap> delRecords = new ArrayList<>();
        ArrayList<HashMap> altRecords = new ArrayList<>();
        String log = "";
        
        if(fields.isEmpty()) return;
        
        oldRecords = oldDB.getRecordsWithFields(table, fields);
        newRecords = newDB.getRecordsWithFields(table, fields);

        log = ("-- Table: "+table+" (nr of records:"+oldRecords.size()+"/"+newRecords.size()+") ");

        // Add index fields to the changeMap
        changeMap.put("IDX",oldDB.getIndexInfo(table));
        String[] idxSet = oldRecords.keySet().toArray(new String[oldRecords.size()] );
        
        for(String idx : idxSet){
            oldRecord = oldRecords.get(idx);
            // If idx is not in newrecords then the record must be deleted
            if(!newRecords.containsKey(idx)){
                //  Put in the delete array
                delRecords.add(oldRecord);
            } else {
                newRecord = newRecords.get(idx);
                // For each field compare the records
                for(String field : oldRecord.keySet()){
                    // Check if old or new is null
                    if(  (oldRecord.get(field) == null)^(newRecord.get(field) == null)  ){
                        // Add to modified
                        altRecords.add(newRecord);
                        break;
                    } else if(  (oldRecord.get(field) == null) && (newRecord.get(field) == null)  ){
                        // TODO: nothing modified
                    } else if( !(oldRecord.get(field).equals(newRecord.get(field)))){
                        // Add to modified
                        altRecords.add(newRecord);
                        break;
                    } 
                }
                
                // Remove from newRecords
                newRecords.remove(idx);
            }
            // Remove from oldRecords
            oldRecords.remove(idx);
        }
        addRecords.addAll(newRecords.values());
        if(!addRecords.isEmpty()) changeMap.put("create", addRecords);
        if(!delRecords.isEmpty()) changeMap.put("delete", delRecords);
        if(!altRecords.isEmpty()) changeMap.put("alter", altRecords);
        
        // Store changrecords in global variable (only if there are some changes)
        if(!(addRecords.isEmpty() && delRecords.isEmpty() && altRecords.isEmpty())) contentChanges.put(table, changeMap);
        
        Logger.getLogger("net.weaponsystems.SQLScripter").info(log+" add: "+addRecords.size()+", delete: "+ delRecords.size() +", alter: "+altRecords.size());
        

    }
    
    
    
    
    static void compareTable(String table) throws SQLException, Exception{
        // Compare columns
        ArrayList<String> oldColumns = oldDB.getColumnsOfTable(table);
        ArrayList<String> newColumns = newDB.getColumnsOfTable(table);
        ArrayList<String> correspondingColumns = getCorrespondingArrayList(oldColumns, newColumns);
        
    }
    
    static ArrayList<String> getCorrespondingArrayList(ArrayList<String> table1, ArrayList<String> table2) throws Exception{
        ArrayList<String> returnArray = new ArrayList<>();
        if(table1.isEmpty() || table2.isEmpty()){
            return new ArrayList<String>();
        }
            for (String s : table1) //use for-each loop
            {
                if(table2.contains(s)){
                    returnArray.add(s);
                    //table1.remove(s);
                    //table2.remove(s);
                    //System.out.print(">>");
                }
                System.out.println(s);
            }        
            return returnArray;
    }
    
}
