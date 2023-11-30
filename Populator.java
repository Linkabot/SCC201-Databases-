import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeSet;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.util.Date;




//https://github.com/simoc/csvjdbc
import org.relique.jdbc.csv.CsvDriver;



/**
 * Class which needs to be modified in order to complete some of the later milestones
 */
public class Populator {
    public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-DD";
    
    Hashtable<String, String> dictionary = new Hashtable<String, String>();
    
    //

    public static final boolean debug=false;
    /**
     * Constructor
     */

    public  Populator() {
        
        dictionary.put("a", "INT");
        dictionary.put("a_I", "INT");
        dictionary.put("a_B", "INT");
        dictionary.put("b", "CHAR");
        dictionary.put("c", "INT");
        dictionary.put("d", "VARCHAR");
        dictionary.put("e", "VARCHAR"); 
        dictionary.put("f", "CHAR");
        dictionary.put("g", "INT");
        dictionary.put("h", "CHAR");
        dictionary.put("i", "INT");
        dictionary.put("k", "DATE");       
        dictionary.put("l", "CHAR");
        dictionary.put("p", "INT");
        dictionary.put("r", "CHAR");
        dictionary.put("t", "VARCHAR");
        dictionary.put("q", "INT");
        dictionary.put("u", "VARCHAR");
        dictionary.put("j", "VARCHAR");
    }

    
    /** 
     * @param args standard main args
     * @throws Exception it could all go horribly wrong
     */
    public static void main(String[] args) throws Exception {
    
        // calls to methods which will complete the database setup and data population
        Populator p = new Populator();
        p.setup("solution.sql");
        //p.milestone5("R", "R", "G");
        //p.updateAllAges();
        //p.updateCensus();
        //p.changeHomePlanet(89, "Earth");
        //p.populationOf("Earth");
        //p.listSuperHeroesContainingPower("covid");
        //p.milestone4();
        //p.milestone1("S", "S");


    }

    /**
     * A single text file containing all the INSERT..INTO statements required for
     * the csv file:
     * e.g.
     * INSERT INTO R(p,l,k) VALUES( 101, 'Eric Noel', '2001-012-25' );
     * NB the text field values are quoted in primes’.
     * 
     * @param csvFileName       name of the csv file containing the data
     * @param tableToInsertInto name of the table to insert data into
     * @return a string composed of the DML statements needed to insert the data.
     */
    public String milestone1(String csvFileName, String tableToInsertInto) {
        String allInserts = "";

        ArrayList<String> headers = new ArrayList<String>();


        try {
            Connection csv = csvConn();
            Connection sqlite = sqliteConn();
            /*
            #############################################################
            */
            Statement stmt = csv.createStatement();
            Statement stmt2 = sqlite.createStatement();

            // Select the ID and NAME columns from sample.csv
            ResultSet results = stmt.executeQuery("SELECT * FROM " + csvFileName );
            ResultSet results2 = stmt2.executeQuery("SELECT * FROM " + tableToInsertInto );
            // ##########################################################
            ResultSetMetaData meta = null;

	    	int columnCount = 0;

            while (results.next()){

                /*
                Gets the headers and puts it into a string
                ####################################################
                */
                
                if (meta == null)
                { 
                    //Gets data from both csv file and table inserting into. 
                    //Needed to compare attributes later so when i have to put data into 
                    //my a_I or a_B, it'll default to inserting into a_B
                    for(int u = 0; u < 2; u++){
                        String columnName = "";
                
                        if(u == 0){
                            
                            meta = results.getMetaData();
                        }
                        else if (u == 1) {
                            
                            meta = results2.getMetaData();
                        }
                        
                        columnCount = meta.getColumnCount();
                        for (int i = 0; i <= columnCount; i++)
                        {
                            
                            if (i > 0){
                                if(!columnName.contains(meta.getColumnName(i))){
                                    columnName += meta.getColumnName(i) + ",";
                                }
                            }
                        }

                        //takes the finally comma away from the attributes string. 
                        columnName = columnName.substring(0, columnName.length() - 1);
                        headers.add(columnName);
                    }
                    
                //split the string into individual letters. 
                //This will be used to compare the 2 sets of attributes. 
                String [] first = headers.get(0).split(",");
                String [] second = headers.get(1).split(",");

               //This is to get the final attributes that the cvs will insert into. 
                String thirdHeader = "";
                
                //This is the comparision. 
                //if relation attributes comes about, it'll accept a_B as an attribute
                //for data to be inserted into. 
                //The CSV will usually have a instead of a_B 
                for (String i : first){
                    for(String j : second){

                        if ( (i.equals(j)) || (j.contains(i)) && (j.contains("_B")) ){
                            thirdHeader += j + ",";
                        }
                    }
                }

                //This takes the last comma of my final list of attributes.
                if (thirdHeader != ""){
                    thirdHeader = thirdHeader.substring(0, thirdHeader.length() - 1);
                }
          

                headers.add(thirdHeader); // Headers used for Primary Table
             
                }
                /*
                ####################################################
                */

                //must set the data back to the csv data
                meta = results.getMetaData();
                columnCount = meta.getColumnCount();

                String insertStatment = "";
                //Time to get the data and convert it to its given type.
                //Then insert that data into a string that will be used for my insert statements. 
                for (int i = 1; i <= columnCount; i++)
                {
                    
                    String name = "";
                    String nameType = "";
                    //used to know what type the attribute is. 
                    name = meta.getColumnName(i);
                    nameType = dictionary.get(name);
                    // Convert String into a date Then insert into the insert statement string
                    if ((nameType == "DATE")  && (!(results.getString(i).contains("\\N"))))
                    {
                        //must take away quotes to begin with if the csv has quotes around the dates
                        String temp = "";
                        if (results.getString(i).toString().startsWith("'")){
                            temp = results.getString(i).replaceAll("'", "");
                        }
                        else 
                        temp = results.getString(i).trim();
                        
                        String date = temp;
                        Date newDate = new SimpleDateFormat(DEFAULT_DATE_FORMAT).parse(date);
                        java.sql.Date sqlStartDate = new java.sql.Date(newDate.getTime());
                        //inserts the data into an ongoing InsertString. Also puts quotes if needed
                        insertStatment = insertStatment + "'" + sqlStartDate + "'" + ",";

                    }
                    // Converting String into an INTeger then insert into the insert statement string
                    else if ((nameType == "INT") && (!(results.getString(i).contains("\\N"))))
                    {    
                        //must trim the number as if there is a space it'll break         
                        String integer = results.getString(i).trim();
                        Integer number = Integer.parseInt(integer);

                        //inserts the data into an ongoing InsertString. Also puts quotes if needed
                        insertStatment = insertStatment + number + ",";

                    }

                    // For Strings then insert into the insert statement string
                    else if ((nameType == "VARCHAR") && (!(results.getString(i).contains("\\N"))) || (nameType == "CHAR") && (!(results.getString(i).contains("\\N"))) )
                    {
                        
                        String string = results.getString(i);
                        //inserts the data into an ongoing InsertString. Also puts quotes if needed
                        insertStatment = insertStatment + "'" + string + "'" + ",";

                    }
                    //converting null statements into appropriate string then insert into insert statement string
                    else if(results.getString(i).contains("\\N")){
                        String n = results.getString(i);
                        n = "NULL";

                        //inserts the data into an ongoing InsertString. Also puts quotes if needed
                        insertStatment = insertStatment + n + ",";
                    }
                }
                // put the insert statement string into my query 
                insertStatment = insertStatment.substring(0, insertStatment.length() - 1);
                String query = ("INSERT OR IGNORE INTO " + tableToInsertInto + " (" + headers.get(2) + ") VALUES (" + insertStatment + ");");
             
                // collect all the insert statements together with new lines. 
                allInserts = allInserts + "\n" + query;
                if (allInserts.startsWith(" ")){
                    allInserts = allInserts.replaceFirst(" ", "");
                }
            }
            // --------------------------------------------------------------------------    
        } catch(Exception c){
            c.printStackTrace();
        }
        
        // returns all insert statements
        return allInserts;
    }

    /**
     * DO NOT MODIFY
     * 
     * @param csvFileName       name of the csv file containing the data
     * @param tableToInsertInto name of the table to insert data into
     * @param fileNameToSaveTo  name of file to save the DML statements to
     * @param append determine if the contents should be appended to the file if it already exists
     */
    public void milestone2(String csvFileName, String tableToInsertInto, String fileNameToSaveTo,boolean append) {
        String sql = milestone1(csvFileName, tableToInsertInto);
        saveStringToFile(fileNameToSaveTo, sql,append);
    }

    /**
     * 
     * @param csvFileName       name of the csv file containing the data
     * @param tableToInsertInto name of the table to insert data into
     * @throws Exception you need to do something with this!
     */
    public void milestone3(String csvFileName, String tableToInsertInto) throws Exception {
        exec(milestone1(csvFileName, tableToInsertInto));
    }

    /**
     * Complete the code which will read all csv files in the ENTITIES folder and
     * populate the appropriate ENTITIES.
     */
    public void milestone4() {
        ArrayList<String> csvHeaders = new ArrayList<String>();
        ArrayList<String> sqliteHeaders = new ArrayList<String>();

        try{
            //connect to csv files and sqlites
            Connection csv = csvConn();
            Connection sqlite = sqliteConn();
            
            DatabaseMetaData md = csv.getMetaData();
            //connect to Entities and look through every csv file. 
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                //needed to reset the comparison that takes place between the csv and table attributes
                csvHeaders.clear();
                Statement stmt = csv.createStatement();
                // for each csv file, select all from it to get data. 
                ResultSet results = stmt.executeQuery("SELECT * FROM " + rs.getString(3) );
                
                ResultSetMetaData meta = null;

                int columnCount = 0;
                // get the attributes from the csv files 
                if (meta == null)
                {
                    //get metadata from csv file
                    meta = results.getMetaData();
                    //get column count
                    columnCount = meta.getColumnCount();
                    
                                
                    for (int i = 0; i <= columnCount; i++)
                    {
                        if (i > 0){
                            //add to an arraylist to compare with table attributes later
                            csvHeaders.add(meta.getColumnName(i));
                        }
                    }
                    // to compare later to find identical attributes

                    DatabaseMetaData uf = sqlite.getMetaData();
                    //find all tables that were created from solution.sql
                    ResultSet tables = uf.getTables(null, null, null, new String[]{"TABLE"});

                    //go through each table
                    while (tables.next()) {
                        sqliteHeaders.clear();
                        Statement stmt2 = sqlite.createStatement();
                        //select everything from the table
                        ResultSet results2 = stmt2.executeQuery("SELECT * FROM " + tables.getString(3) );

                        meta = null;

                        columnCount = 0;
                        
                        //get the attributes for each table. 
                        if (meta == null)
                        {
                            //get metadata from table
                            meta = results2.getMetaData();
                            //get column count 
                            columnCount = meta.getColumnCount();
                                                                  
                            for (int i = 0; i <= columnCount; i++)
                            {
                                if (i > 0){
                                    //add to an arraylist to compare with the csv file
                                    //I take the "_B" and "_I" because for milestone 4 its just comparing the cvs and table. 
                                    //milestone4 calls milestone 3 which calls milestone 1 which will then deal with the relation attributes. 
                                    if( (meta.getColumnName(i).contains("_B")) || (meta.getColumnName(i).contains("_I")) ){
                                        String []str = meta.getColumnName(i).split("_");
                                        String part = str[0];
                                        sqliteHeaders.add(part);
                                    }
                                    else{
                                        sqliteHeaders.add(meta.getColumnName(i));
                                    }  
                                }
                            }
                            
                            // compare the too array list with containsAll
                            // If they match, call milestone3 with the csv file and table. 
                            // Milestone 3 calls milestone 1 
                            if( (sqliteHeaders.containsAll(csvHeaders)) ){
                                milestone3(rs.getString(3),tables.getString(3));
                            }                                                    
                        }                       
                    }                    
                }
            }
        }
        catch(Exception c){
            c.printStackTrace();
        }
    }

    /**
     * Method to take a csv file where the first row is a head of field names and 
     * all other rows are data to be inserted into the named table
     * e.g.\\
     * DATA FILE LANGUAGE.CSV to be inserted into K:
     * 
     * u
     * 'English'
     * 'French'
     * 'Plutonian'
     * 
     * results in:
     * 
     * BEGIN TRANSACTION;
     * -- NB might already exist --
     * INSERT OR IGNORE INTO K(u) VALUES( 'English' );
     * INSERT OR IGNORE INTO K(u) VALUES( 'French' );
     * INSERT OR IGNORE INTO K(u) VALUES( 'Plutonian' );
     * COMMIT;
     * @param csvFileName name of the csvFile containing the data NB fileName does not have to match the entity name
     * @param primaryTableToInsertInto name of the table to insert into
     * @return a sql string containing appropriate insert into statements
     */
    public String insertCSVData(String csvFileName, String primaryTableToInsertInto) {
        String result = milestone1(csvFileName, primaryTableToInsertInto);
        return result;
    }

    /**
     * A single text file containing all the correct INSERT..INTO statements for
     * data which involves TWO entities.
     * e.g.\\
     * DATA FILE HERO.CSV to be inserted into R and G:
     * 
     * p,l,k,h,f
     * 101,'Eric Noel','2001-12-25','Eric Noel', 'Santa Clause'
     * 
     * results in:
     * 
     * PRAGMA foreign_keys=ON;
     * BEGIN TRANSACTION;
     * -- NB might already exist --
     * INSERT OR IGNORE INTO R(p,l,k) VALUES( 101, 'Eric Noel',
     * '2001-012-25' );
     * INSERT INTO G(p,h,f) VALUES (101,'Eric Noel','Santa Clause');
     * COMMIT;
     * 
     * @param csvFileName                name of the csv file containing the data
     * @param primaryTableToInsertInto   name of the table to insert data into
     * @param secondaryTableToInsertInto name of the table to insert data into
     *                                   which has a foreign key dependency on the
     *                                   primaryTableToInsertInto i.e. data can't be
     *                                   put into secondaryTableToInsertInto ifTABLES: 
     *                                   there is no related item in the
     *                                   primaryTableToInsertInto
     * @return a string composed of the DML statements needed to insert the data.
     */
    public String milestone5(String csvFileName, String primaryTableToInsertInto, String secondaryTableToInsertInto) {
        String everyInsert = "PRAGMA foreign_keys=ON; \nBEGIN TRANSACTION;\n"; 
        String allInserts = "";
        String allInserts2 = "";
        ArrayList<String> headers = new ArrayList<String>();

        try {
            Connection csv = csvConn();
            Connection sqlite = sqliteConn();

            /*
            #############################################################
            */

            // Create a Statement object to execute the query with.
            // need 3 for the csv file and 2 tables 
    
            Statement stmt = csv.createStatement();
            Statement stmt2 = sqlite.createStatement();
            Statement stmt3 = sqlite.createStatement();

            // Select the ID and NAME columns from sample.csv
            ResultSet results = stmt.executeQuery("SELECT * FROM " + csvFileName );
            ResultSet results2 = stmt2.executeQuery("SELECT * FROM " + primaryTableToInsertInto );
            ResultSet results3 = stmt3.executeQuery("SELECT * FROM " + secondaryTableToInsertInto );

            // ##########################################################
         
            ResultSetMetaData metaCsv = null;


	    	int columnCount = 0;


            while (results.next()){
                // ###############################################################################################
                //Gets the headers for every file and puts it into an arraylist for comparison
                
                if (metaCsv == null)
                {
                    for(int u = 0; u < 3; u++){
                        String columnName = "";
                        
                        if(u == 0){
                            //csv metadata
                            metaCsv = results.getMetaData();
                        }
                        else if (u == 1) {
                            //primary table meta data
                            metaCsv = results2.getMetaData();
                        }
                        else if (u == 2) {
                            //secondary table metadata
                            metaCsv = results3.getMetaData();
                        }
                        //get the columncount from the data
                        columnCount = metaCsv.getColumnCount();
                        
                        for (int i = 0; i <= columnCount; i++)
                        {
                            if (i > 0){
 
                                if(!columnName.contains(metaCsv.getColumnName(i))){
                                    columnName += metaCsv.getColumnName(i) + ",";
                                }
                            }
                        }
                        //take final comma away fromt the string
                        columnName = columnName.substring(0, columnName.length() - 1);
                        headers.add(columnName);
                    }
                // ###############################################################################################

                //split the string to individual letters for attribute comparison
                String [] first = headers.get(0).split(",");
                String [] second = headers.get(1).split(",");
                String [] third = headers.get(2).split(",");

                //These will be the final Strings of attributes that will be insterted 
                String thirdHeader = "";
                String forthHeader = "";

                // ###############################################################################################
                //To compare which attributes each table has and what needs to be put in 
                for (String i : first){
                    for(String j : second){
                        //deals with relation attributes
                        if ( (i.equals(j)) || (j.contains(i)) && (j.contains("_B")) ){
                            thirdHeader += j + ",";
                        }
                    }
                }
            
                for (String i : first){
                    for(String j : third){

                        if ( (i.equals(j)) || (j.contains(i)) && (j.contains("_B")) ){
                            forthHeader += j + ",";
                        }
                    }
                }
                
                // ###############################################################################################
                //take final comma from the attribute strings
                if (thirdHeader != ""){
                    thirdHeader = thirdHeader.substring(0, thirdHeader.length() - 1);
                }
                if (forthHeader != ""){
                    forthHeader = forthHeader.substring(0, forthHeader.length() - 1);                }
                
                headers.add(thirdHeader); // Headers used for Primary Table
                headers.add(forthHeader); // headers used for Secondary Table
                }
                
                // ###############################################################################################
                
                String primaryInsertStatment = "";
                String secondInsertStatment = "";
                metaCsv = results.getMetaData();
                columnCount = metaCsv.getColumnCount();
                
                //converting data to types and putting them into specific insert Strings
                for (int i = 1; i <= columnCount; i++)
                {
                    
                    String name = "";
                    String nameType = "";
                    
                    // use dictionary to know which type each attribute is.
                    name = metaCsv.getColumnName(i);
                    nameType = dictionary.get(name);

                    // ###############################################################################################
                    // Converting Strings into DATE
                    if ((nameType == "DATE")  && (!(results.getString(i).contains("\\N"))))
                    {
                        String temp = "";
                        if (results.getString(i).toString().startsWith("'")){
                            temp = results.getString(i).replace("'", "");
                        }
                        else 
                        temp = results.getString(i).trim();
                        
                        String date = temp;
                        Date newDate = new SimpleDateFormat(DEFAULT_DATE_FORMAT).parse(date);
                        java.sql.Date sqlStartDate = new java.sql.Date(newDate.getTime());
                        
                        //if the arraylist contains the attribute being converted, put it into the insert string
                        if (headers.get(3).contains(name)){
                        primaryInsertStatment = primaryInsertStatment + "'" + sqlStartDate + "'" + ",";
                        }

                        if (headers.get(4).contains(name)){
                            secondInsertStatment = secondInsertStatment + "'" + sqlStartDate + "'" + ",";
                        }
                    }
                    // ###########################################################################################
                    // Converting String into an INTeger
                    else if ((nameType == "INT") && (!(results.getString(i).contains("\\N"))))
                    {
                        String temp = "";
                        if (results.getString(i).toString().startsWith("'")){
                            temp = results.getString(i).replaceAll("'", "");
                        }
                        else 
                        temp = results.getString(i);
                        
                        //must trim otherwise if there is a space the value wont be converted. 
                        String num = temp;
                        String integer = num.trim();
                        Integer number = Integer.parseInt(integer);

                        //if the arraylist contains the attribute being converted, put it into the insert string
                        if (headers.get(3).contains(name)){
                            primaryInsertStatment = primaryInsertStatment + number  + ",";
                        }
    
                        if (headers.get(4).contains(name)){
                            secondInsertStatment = secondInsertStatment + number  + ",";
                        }
                    }
                    // ###############################################################################################
                    // For Strings
                    else if ((nameType == "VARCHAR") && (!(results.getString(i).contains("\\N"))) || (nameType == "CHAR") && (!(results.getString(i).contains("\\N"))) )
                    {
                        String temp = "";
                        if (results.getString(i).toString().contains("'")){
                            temp = results.getString(i).replaceAll("'", "");
                            if (temp.startsWith(" ")){
                                temp = temp.replaceFirst(" ", "");
                            }
                        }

                        else 
                        temp = results.getString(i);
                        
                        String str = temp;
                        String string = str;

                        //if the arraylist contains the attribute being converted, put it into the insert string
                        if (headers.get(3).contains(name)){
                            primaryInsertStatment = primaryInsertStatment + "'" + string + "'" + ",";
                        }
                        
                        if (headers.get(4).contains(name)){
                            secondInsertStatment = secondInsertStatment + "'" + string + "'" + ",";
                        }
                    }
                    // ###############################################################################################
                    // for NULLS
                    else if(results.getString(i).contains("\\N")){
                        String n = results.getString(i);
                        n = "NULL";

                        //if the arraylist contains the attribute being converted, put it into the insert string
                        if (headers.get(3).contains(name)){
                            primaryInsertStatment = primaryInsertStatment + n + ",";
                        }

                        if (headers.get(4).contains(name)){
                            secondInsertStatment = secondInsertStatment + n + ",";
                        }
                    }
                    // ###############################################################################################
                }
                //puts primary insert string into an insert statement string.
                if (primaryInsertStatment != ""){
                    primaryInsertStatment = primaryInsertStatment.substring(0, primaryInsertStatment.length() - 1);
                    String query = ("INSERT OR IGNORE INTO " + primaryTableToInsertInto + " (" + headers.get(3) + ") VALUES (" + primaryInsertStatment + ");");
                    allInserts = allInserts + "\n" + query;
                    if (allInserts.startsWith(" ")){
                        allInserts = allInserts.replaceFirst(" ", "");
                    }
                }
                //puts secondary insert string into an insert statement string.
                if (secondInsertStatment != ""){
                    secondInsertStatment = secondInsertStatment.substring(0, secondInsertStatment.length() - 1);
                    String query2 = ("INSERT INTO " + secondaryTableToInsertInto + " (" + headers.get(4) + ") VALUES (" + secondInsertStatment + ");");
                    allInserts2 = allInserts2 + "\n" + query2;
                    if (allInserts2.startsWith(" ")){
                        allInserts2 = allInserts2.replaceFirst(" ", "");
                    }
                }
            }
               
            //Puts both primary and secondary insert statements into one big String
            everyInsert = everyInsert + allInserts + allInserts2 + "\nCOMMIT;";
            // --------------------------------------------------------------------------    
        } catch(Exception c){
            c.printStackTrace();
        }
        //returns the insert statements
        return everyInsert;
    }

    /**
     * DO NOT MODIFY
     * Insert data from a single csv file which spans two entities
     * This is not easy and is a stretch problem
     * @param csvFileName                name of the csv file containing the data
     * @param primaryTableToInsertInto   name of the table to insert data into
     * @param secondaryTableToInsertInto name of the table to insert data into
     *                                   which has a foreign key dependency on the
     *                                   primaryTableToInsertInto i.e. data can't be
     *                                   put into secondaryTableToInsertInto if
     *                                   there is no related item in the
     *                                   primaryTableToInsertInto
     * @param fileNameToSaveTo           name of file to save the DML statements to
     * @param append determine if the contents should be append to a file if it already exists
     */
    public void instoCSV(String csvFileName, String primaryTableToInsertInto, 
    String secondaryTableToInsertInto,
            String fileNameToSaveTo,boolean append) {
        String sql = milestone5(csvFileName, primaryTableToInsertInto, secondaryTableToInsertInto);
        saveStringToFile(fileNameToSaveTo, sql,append);
    }

    /**
     * A method to update the ages in R.c based on R.k which is a date of birth.
     * See the courswork assignment for an appropriate sql update statement.
     */
    public void updateAllAges(){

        try {
            Connection sqlite = sqliteConn();
            
            Statement stmt = sqlite.createStatement();
            
            //Only need the name and date to update age. 
            //Therefore call specific attributes from correct table.
            ResultSet results = stmt.executeQuery("SELECT k, l FROM R;");
            
            ResultSetMetaData metaCsv = null;

            
            while (results.next()){
                int age = 0;
                String tname = "";
                
                // ###############################################################################################
                
                if (metaCsv == null)
                {
                    //get meta data
                    metaCsv = results.getMetaData();   
                }
                String updateString = "";
                //gets the dates and compares the date to the current date. 
                String temp = "";
                if (results.getString(1).toString().startsWith("'")){
                    temp = results.getString(1).replace("'", "");
                }
                else {
                    temp = results.getString(1);
                }
                //splits the date to get individual year, month and date and i need ints to pass into the comparison
                String [] num = temp.split("-");
                LocalDate today = LocalDate.now();
                LocalDate birthday = LocalDate.of(Integer.parseInt(num[0]), Integer.parseInt(num[1]), Integer.parseInt(num[2]));
                Period po = Period.between(birthday, today);
                //returns the amount of years apart from the 2 dates. 
                age = po.getYears();
            // ###########################################################################################

                //gets the name.
                temp = "";
                if (results.getString(2).toString().contains("'")){
                    temp = results.getString(2).replaceAll("'", "");
                    if (temp.startsWith(" ")){
                        temp = temp.replaceFirst(" ", "");
                    }
                }

                else 
                temp = results.getString(2);
                
                String str = temp;
                String string = str;
                //put quotes around the name otherwise it wont work. 
                tname = "'" + string +"'";
                    
                //put everything into an update string and execute the update statement
                updateString = "UPDATE R SET c = " + age + " WHERE l = " + tname + ";";
                exec(updateString);
            }
        }
        catch(Exception c){
            c.printStackTrace();
        }
    }

    /**
     * A method to update the Plaent census data in F.i 
     * based on I and R which is the relationship(I) of who (R) lives on which planet(F).
     * NB, the census data is the count of people where their age (R.c) is LESS than 100 years on the date of the census.
     * ALSO ignore ages which are less than 0.
     * 
     */
    public void updateCensus(){
        //My census will get the count of people that have an id for a planet.
        //They will then update the census depending on the count. 
        try {
            Connection sqlite = sqliteConn();

            Statement stmt = sqlite.createStatement();
            
            //Get the specific attributes in my select statement. 
            //Also include conditions. 
            ResultSet results = stmt.executeQuery("SELECT F.a, COUNT(R.a_I) FROM R INNER JOIN F ON R.a_I = F.a WHERE R.c < 100 AND R.c > 0 GROUP BY F.a");
            
            ResultSetMetaData metaCsv = null;
            
            while (results.next()){ 
                if (metaCsv == null)
                {
                    metaCsv = results.getMetaData();
                }
                //put the results from the select statement into an update statement to update the cencus. 
                String updateString = "UPDATE F\nSET i = " + results.getInt(2) + "\nWHERE a = " + results.getInt(1) + ";\n";
                //execute statement
                exec(updateString);
            }
        }
        catch(Exception c){
            c.printStackTrace();
        }
    }

    /**
     * Inserts data from a csv file into the set of entities (and related relationships)
     * A super stretch question
     * You will have to work out which entities contain which attributes 
     * and then work out the order in which they should be inserted
     * where relations are involved, you may have needed to 
     * add extra attributes to tables to code 1:M relationships which will have their own names not in the er diagram!
     * This class will therefore have to 'know' about the extra attibutes
     * @param csvFile the path of the csv file. The file will ALWAYS have a header row at the start which indicates the attribute names.
     * @param entities the array of entities which may be needed
     * @param relations the array of relationships which may be needed
     * @return return true if the process was successfull
     */
    public boolean insertCSVData(String csvFile,String[] entities,String[] relations){
        boolean result=true;

        return result;
    }


    /**
     * Method to find all of the super heroes in the database which have a power containing the the value of txt
     * @param txt the text which is contained in a super heroes power
     * @return a list of the hidden identies of the super heroes with a power
     */
    public List<String> listSuperHeroesContainingPower(String txt){
        List<String> result=new ArrayList<>();
        try {
            Connection sqlite = sqliteConn();

            Statement stmt = sqlite.createStatement();
            //select statement to get the correct attributes needed/
            ResultSet results = stmt.executeQuery("SELECT DISTINCT G.h FROM G INNER JOIN S ON G.p = S.p WHERE S.r LIKE '%" + txt + "%';");
            
            ResultSetMetaData metaCsv = null;
            
            while (results.next()){
                if (metaCsv == null)
                {
                    metaCsv = results.getMetaData();
                }
                //add the results into the arraylist.
                result.add(results.getString(1));
            }
        }
        catch(Exception c){
            c.printStackTrace();
        }
        return result;
    }

    public TreeSet<String> entityAttributes(String entityName){
        TreeSet<String> result=new TreeSet<>();
        // TODO
        return result;
    }

    /**
     * Method to create appropriate sql statements to insert data from a csv file
     * @param csvFile path to the csv file.  The file will ALWAYS have a header row at the start which indicates the attribute names.
     * @param entities the array of entities which may be needed
     * @param relations the array of relationships which may be needed
     * @return DML sql string which will insert data into the entities from the csv file
     */
    public String createInsertSQLForCsvFile(String csvFile,String[] entities,String[] relations){
        StringBuilder builder=new StringBuilder();
        // TODO
        return builder.toString();
    }

    /**
     * 
     * @param fileNameToSaveTo file name to save to
     * @param text the text to be saved into the file
     * @param append if true and the file already exists the text will be added to the end of file
     */
    private void saveStringToFile(String fileNameToSaveTo, String text,boolean append) {
        //NB APPEND contents to file
        try (FileWriter fw = new FileWriter(fileNameToSaveTo,append)) {
            fw.append(text);
            fw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Inserts the result of parsing the csv file into the database
     * 
     * @param csvFileName                name of the csv file containing the data
     * @param primaryTableToInsertInto   name of the table to insert data into
     * @param secondaryTableToInsertInto name of the table to insert data into
     *                                   which has a foreign key dependency on the
     *                                   primaryTableToInsertInto i.e. data can't be
     *                                   put into secondaryTableToInsertInto if
     *                                   there is no related item in the
     *                                   primaryTableToInsertInto
     * @throws Exception  it could all go horribly wrong
     */
    public void milestone6(String csvFileName, String primaryTableToInsertInto, String secondaryTableToInsertInto)
            throws Exception {
                //NB assumes valid sql inserts were generated bile milestoneF1
                //do not rely on this in production code (HINT)
                //Can change the rest if you want
        exec(milestone5(csvFileName, primaryTableToInsertInto, secondaryTableToInsertInto));
    }

    /**
     * Here just as an example piece of code
     * 
     * @throws Exception it could all go horribly wrong
     */
    public static void dummyExample() throws Exception {
        Populator populator=new Populator();
        Connection csv = populator.csvConn();
        Connection sqlite = populator.sqliteConn();

        // get a list of tables and views
        System.out.println("List of table names based on CSV files");
        DatabaseMetaData md = csv.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
        while (rs.next()) {
            System.out.println(rs.getString(3));
        }
        // Create a Statement object to execute the query with.
        // A Statement is not thread-safe.
        Statement stmt = csv.createStatement();

        // Select the ID and NAME columns from sample.csv
        ResultSet results = stmt.executeQuery("SELECT p,l FROM R order by p ASC");
        // Dump out the results to a CSV file with the same format
        // using CsvJdbc helper function
        boolean append = true;
        System.out.println("\nData from planets.csv");
        CsvDriver.writeToCsv(results, System.out, append);
        System.out.println("\nData from human table");
        CsvDriver.writeToCsv(sqlite.createStatement().executeQuery("SELECT p,l FROM R order by p ASC"),
                System.out, append);
    }

    /**
     * Create a string containing the csv version of the resultset
     * @param r ResultSet to be used in building the csv contents
     * @return a String representing the attributes and data of the ResultSet
     */
    public String csvIt(ResultSet r) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            final String utf8 = StandardCharsets.UTF_8.name();
            PrintStream ps = new PrintStream(baos, true, utf8);
            CsvDriver.writeToCsv(r, ps, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new String(baos.toByteArray());
    }

    //DO NOT CHANGE
    static Connection csv = null;

    /**
     * ON PAIN of Zylon, do not modify this method
     * 
     * @return a connection to the csv files in this directory
     */
    public Connection csvConn() {
        try {
            if (csv == null || csv.isClosed()) {
                String folder = "./ENTITIES";
                String url = "jdbc:relique:csv:" + folder + "?" +
                        "separator=," + "&" + "fileExtension=.csv";
                csv = DriverManager.getConnection(url);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return csv;
    }

    //DO NOT CHANGE
    static Connection sqlite = null;

    /**
     * ON PAIN of Zylon, do not modify this method
     * 
     * @return a connection to the sqlite database created from your DDL sql script
     * @throws SQLException it could all go horribly wrong
     */
    public  Connection sqliteConn() throws SQLException {
        if (sqlite == null || sqlite.isClosed()) {
            try {
                Class.forName("org.sqlite.JDBC");
                sqlite = DriverManager.getConnection("jdbc:sqlite:LSHs.db");
                //System.out.println("Opened database successfully");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return sqlite;
    }

    

    /**
     * Reads the contents of the solution.sql and executes the SQL commands
     * @throws IOException
     * @throws SQLException
     * @throws Exception it could all go horribly wrong
     */
    public void setup(String fileName) throws SQLException, IOException  {
        String code=readFile(fileName);
        exec(code);
    }



    /**
     * executes one or more sql commands. 
     * The commands are seperated by ;
     * For this assignment, we confirm that no data will contain the ; charachter
     * @param batch the string of commands seperated by ;
     * @throws Exception it could all go horribly wrong
     */
    public void exec(String batch) throws SQLException {
        String[] lines = batch.split(";");
        Connection conn = sqliteConn();
        conn.setAutoCommit(false);
        for (String sql : lines) {
            debug(sql);
            sql=sql.trim();
            try{
            conn.createStatement().execute(sql);
            } catch (Exception e){
                //don't use Log4j!!!!!
                if (debug) {e.printStackTrace();}
            }
        }
        try{
        conn.commit();//this could really throw an exception if the data violates constraints
        } catch (Exception e){
            try{
            conn.rollback();
            } catch (Exception r){
                debug(r.getLocalizedMessage());
            }
            debug(e.getLocalizedMessage());
        }
        try {
        conn.setAutoCommit(true);
   
        } catch (Exception f){
            if (!conn.isClosed()){
                conn.close();
            }
        }

        
    }

    private void debug(String txt) {
        if (debug){
            System.out.println(txt);
        }
    }


    /**
     * Utility function to read a file 
     * @param f name of the file to read
     * @return a String containing the contents of the file
     * @throws IOException it could all go horribly wrong
     */
    public static String readFile(String f) throws IOException {
        return new String(Files.readAllBytes(Paths.get(f)));
    }

    /**
     * pre-condition: You have create a DDL file called solution.sql
     * you have run a command in this directory:
     * sqlite3 LSH.db &lt; solution.sql
     * 
     * OR you have used the setup method above
     */
    public void milestoneA() {
    }

    /**
     * Runs a sql command against the sqlite database
     * @param sql command to be executed
     * @return The csv version of the results returned
     * @throws SQLException it could all go horribly wrong
     */
    public String runSqliteQuery(String sql) throws SQLException {
        return csvIt(sqliteConn().createStatement().executeQuery(sql));
    }

    /**
     * Runs a sql command against the CSV database
     * @param sql command to be executed
     * @return The csv version of the results returned
     * @throws SQLException anything could go wrong
     */
    public String runCSVQuery(String sql) throws SQLException {
        return csvIt(csvConn().createStatement().executeQuery(sql));
    }

    /**
     * Method to find the names of attributes in  a resultset
     * @param rs a result set
     * @return a list of names of attributes using in the resultset
     */
    public List<String> attributeNamesofResultSet(ResultSet rs){
        List<String> result = new ArrayList<>();
        
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int n=rsmd.getColumnCount();
            for (int i=1;i<=n;i++){
                result.add(rsmd.getColumnName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }


    /**
     * 
     * method to change the home world of a person R  NB if the planet does not exist
     * @param i is the p of the person entity (R)
     * @param planetName is the name of the planet that the person moves to (attribute t in entity F)
     * @throws UnknownPlanetException if planet does not exists
     * @throws UnknownPersonException if the person does not exist
     */
    public void changeHomePlanet(int i, String planetName) throws UnknownPlanetException,UnknownPersonException{
        try {
            Connection sqlite = sqliteConn();

            Statement stmt = sqlite.createStatement();
            
            //select statement to get the id of the planet 
            ResultSet results = stmt.executeQuery("SELECT DISTINCT a FROM F WHERE t = '" + planetName + "'");
            
            ResultSetMetaData metaCsv = null;
            
            while (results.next()){
                if (metaCsv == null)
                {
                    metaCsv = results.getMetaData();
                }
                
                //update string to change the planet they live on with the i being the id of the person.
                String updateString = "UPDATE R\nSET a_I = " + results.getInt(1) + "\nWHERE p = " + i + ";\n";
                //execute update string
                exec(updateString);
                
            }
        }
        catch(Exception c){
            c.printStackTrace();
        }
    }
    

    /**
     * find the population of a planet after the last census update
     * @param planetName
     * @return the last census population size
     */
    public long populationOf(String planetName) throws UnknownPlanetException{
        long result=-1;
        try {
            Connection sqlite = sqliteConn();

            Statement stmt = sqlite.createStatement();
            
            //get the census of the plane which is attribute i
            ResultSet results = stmt.executeQuery("SELECT DISTINCT i FROM F WHERE t = '" + planetName + "'");
            
            ResultSetMetaData metaCsv = null;
            
            while (results.next()){
                if (metaCsv == null)
                {
                    metaCsv = results.getMetaData();
                }
                //save the number
                result = results.getInt(1);
            }
        }
        catch(Exception c){
            c.printStackTrace();
        }
        return result;
    }

    public static class UnknownPlanetException extends Exception {    }
    public static class UnknownPersonException extends Exception {    }
}
