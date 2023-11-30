

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for indicating if milestones are being addressed
 */
public class PopulatorTest {

    private Populator instance;
    private Cat cat;

    /**
     * test setup
     * 
     * @throws Exception err
     */
    @Before
    public void setup() {
        deleteDatabaseFile();
        instance = new Populator();
        try {
            instance.setup("solution.sql");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        cat = new Cat();
    }

    @After
    public void close() {
        try {
            instance.sqliteConn().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            instance.csvConn().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * deletes the LSHs.db file
     */
    private void deleteDatabaseFile() {
        File f = new File("LSHs.db");
        if (f.exists()) {
            f.delete();
        }
    }

    /**
     * Functional approach to reading a file!
     * 
     * @param f the filename to load
     * @return the contens of the file 'f'
     * @throws IOException err
     */
    public static String readFile(String f) throws IOException {
        return new String(Files.readAllBytes(Paths.get(f)));
    }

    /**
     * A single text file which contains at the start the CREATE TABLE statements
     * that create the tables for the ENTITIES that your ER Diagram defines. (But
     * without the primary and foreign keys being indicated).
     * 
     * @throws IOException err
     */
    @Test
    public void milestoneA2() throws IOException {
        try {
            String[] tables = new String[] { "F", "L", "K", "S", "A", "R", "G" };
            TreeSet<String> expected = new TreeSet<>(Arrays.asList(tables));
            TreeSet<String> got = cat.tableNames(instance.sqliteConn());
            assertTrue(got.containsAll(expected));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Another test to see if table has been created
     * 
     * @throws IOException err
     */
    @Test
    public void milestoneA1() throws IOException {
        try {
            TreeSet<String> got = cat.tableNames(instance.sqliteConn());
            assertTrue(got.contains("R"));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * A single text file as in (a) above, but the CREATE TABLE statements include
     * indicators of primary keys and the variable types
     * 
     * @throws Exception    err
     * @throws SQLException sqlerr
     **/
    @Test
    public void milestoneB1() throws Exception {
        assertTrue(cat.hasPrimaryKeys(instance.sqliteConn(), "R"));
    }

    /**
     * A single text file as in (a) above, but the CREATE TABLE statements include
     * indicators of primary keys and the variable types
     * 
     * @throws Exception err happens
     **/
    @Test
    public void milestoneB2() throws Exception {
        TreeMap<String, Attribute> atts = cat.tableAttributes(instance.sqliteConn(), "R");
        Attribute a = atts.get("k");
        assertTrue(a != null);
        assertTrue(a.type.toLowerCase().trim().contains("date"));
    }

    /**
     * A single text file as in (b) above, but including foreign keys.
     * 
     * @throws Exception err
     **/
    @Test
    public void milestoneC() throws Exception {
        assertTrue(cat.hasForeingKeys(instance.sqliteConn(), "G"));
    }

    /**
     * A single text file containing all the INSERT..INTO statements required for
     * the csv file R:
     * 
     * @throws Exception err happens
     **/
    @Test
    public void milestoneD() throws Exception {
        String sql = "select * from 'R'";
        // NB using try(ResultSet rs) ... which closes rs automatically
        try (ResultSet rs = instance.sqliteConn().createStatement().executeQuery(sql)) {
            assertTrue(rs.next());
        }
    }

    /**
     * Complete the code in Populator.java which will read all csv files in the
     * ENTITIES folder and populate the appropriate entities.
     **/
    @Test
    public void milestoneE() {
    }

    /**
     * A single text file containing all the correct INSERT..INTO statements for
     * data which involves two entities.
     * 
     * DATA FILE HERO.CSV:\\
     * 
     * p,l,k,h,f\\ 101,'Eric Noel','2001-12-25','Eric Noel', 'Santa
     * Clause'
     * 
     * SQL
     * 
     * PRAGMA foreign_keys=ON; BEGIN TRANSACTION; -- NB might already exist --
     * INSERT OR IGNORE INTO R(p,l,k) VALUES( 101, 'Eric Noel',
     * '2001-012-25' ); -- assuming this does not exist INSERT INTO G(p,h,f)
     * VALUES (101,'Eric Noel','Santa Clause'); COMMIT;
     * 
     * @throws SQLException
     */
    @Test
    public void milestoneF() throws Exception {
        String csvFile = "./ENTITIES/f.csv";
        cat.write(csvFile,
                "p,l,k,h,f\n101,'Eric Noel','2001-12-25','Eric Noel', 'Santa * Clause'");
        instance.insertCSVData(csvFile, new String[] { "R", "G" }, new String[] { "ISA" });
        try (ResultSet rs = instance.sqliteConn().createStatement().executeQuery("select * from R where p=101")) {
            assertTrue(rs.next());
        }
        try (ResultSet rs = instance.sqliteConn().createStatement()
                .executeQuery("select * from G where p=101 and f='Santa * Clause'")) {
            assertTrue(rs.next());
        }
    }

    /**
     * The database also needs indexes in the database to speed up possible queries
     * involving the combination of entities and attribute constraints. Add code to
     * establish what indexes are present and include CREATE INDEX statements in
     * your DDL to create these. You should have a set of statements of the form
     * (what follows is just an example):
     * 
     * CREATE INDEX planets_id ON F (a);
     * 
     * @throws SQLException
     * 
     */
    @Test
    public void milestoneG() throws SQLException {
        long start = System.currentTimeMillis();
        cat.doStuff(instance.sqliteConn());
        long end = System.currentTimeMillis();
        double tt = (end - start) / 1000.0;
        assertTrue(tt < 10.5);
    }

    /**
     * If the database structure is modified after creation it is possible for
     * tables to be returned in an order that would break key constraints – i.e. a
     * table depends on a yet to be created table. CREATE statements need sorting
     * based on foreign keys.
     * 
     * As with (g) but ensure the CREATE TABLE statements are in the ‘correct’
     * order.
     */
    @Test
    public void milestoneH() {
    }

    @Test
    public void checkAgeUpdate() throws Exception {
        String sql = "INSERT OR IGNORE INTO R(p,l,k,c) VALUES( 10101, 'Eric Bell', '1982-02-25',5 );";
        instance.exec(sql);
        instance.updateAllAges();
        try (ResultSet rs = instance.sqliteConn().createStatement().executeQuery("select * from R where p=10101")) {
            assertTrue(rs.next());
            LocalDate dob = new SimpleDateFormat("yyyy-MM-dd")
                    .parse(rs.getString("k"))
                    .toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            Period period = Period.between(dob, LocalDate.now());
            assertEquals(period.getYears(), rs.getInt("c"));
        }
    }

    @Test
    public void checkCensusUpdate()
            throws SQLException, Populator.UnknownPlanetException, Populator.UnknownPersonException {
        instance.updateCensus();
        instance.changeHomePlanet(1, "Pluto");
        instance.updateCensus();
        long e2 = instance.populationOf("Pluto");
        instance.changeHomePlanet(1, "Pluto");
        instance.updateCensus();
        instance.changeHomePlanet(1, "Earth");
        instance.updateCensus();
        long e3 = instance.populationOf("Pluto");

        assertEquals(e3, e2 - 1);

    }

    @Test
    public void checkListSuperHeroPowers() throws SQLException {
        String sql = "INSERT OR IGNORE INTO S(g,p,r) VALUES( 101, 101, 'covid triple boosted' );";
        List<String> x = instance.listSuperHeroesContainingPower("covid");
        assertTrue(x.size() > 0);
        assertTrue(x.size() == 1);
    }

    @Test
    public void hasViews() throws SQLException, Exception {
        TreeSet<String> result = cat.viewNames(instance.sqliteConn());
        assertTrue(result.size() > 0);
    }

    private static class Cat {
        TreeSet<String> tableNames(Connection conn) throws Exception {
            return metadata(conn, "TABLE");
        }

        public void doStuff(Connection conn) {
        }

        TreeSet<String> viewNames(Connection conn) throws Exception {
            return metadata(conn, "VIEW");
        }

        TreeMap<String, Attribute> tableAttributes(Connection conn, String table) throws SQLException {
            DatabaseMetaData metadata = conn.getMetaData();
            TreeMap<String, Attribute> arrayList = new TreeMap<>();
            ResultSet resultSet = metadata.getColumns(null, null, table, null);
            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("TYPE_NAME");
                int size = resultSet.getInt("COLUMN_SIZE");
                arrayList.put(name, new Attribute(name, type, size));
            }
            return arrayList;
        }

        TreeSet<String> metadata(Connection conn, String param) throws Exception {
            TreeSet<String> arrayList = new TreeSet<>();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String[] arrayOfString = { param };
            ResultSet resultSet = databaseMetaData.getTables(null, null, "%", arrayOfString);
            while (resultSet.next())
                arrayList.add(resultSet.getString("TABLE_NAME"));
            return arrayList;
        }

        final boolean isAlive() {
            boolean bool = false;
            Date date = new Date();
            String str = "2022-03-21 06:00:00";
            try {
                Date date1 = (new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse(str);
                bool = date1.before(date);
            } catch (ParseException parseException) {
                parseException.printStackTrace();
            }
            return bool;
        }

        private boolean hasPrimaryKeys(Connection conn, String table) throws Exception {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            boolean result = false;
            try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(conn.getCatalog(), null, table)) {
                result = resultSet.next();
            }
            return result;
        }

        boolean hasForeingKeys(Connection conn, String tableName) throws SQLException {
            DatabaseMetaData dm = conn.getMetaData();
            boolean result = false;
            try (ResultSet rs = dm.getImportedKeys(null, null, tableName)) {
                result = rs.next();
            }
            return result;
        }

        private void write(String string, String string2) {
            try (FileWriter w = new FileWriter(string)) {
                w.append(string2);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    static class Attribute implements Comparable<Attribute> {
        final String name;
        final String type;
        final int size;

        public Attribute(String name, String type, int size) {
            this.name = name;
            this.type = type;
            this.size = size;
        }

        @Override
        public int compareTo(PopulatorTest.Attribute o) {
            return name.compareTo(o.name);
        }
    }

    
}
