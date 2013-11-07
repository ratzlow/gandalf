package net.gandalf.h2;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-31
 */
public class ReadMetaDataTest {
    String ddlSql = "CREATE TABLE person (" +
                    "  id IDENTITY," +
                    "  firstname VARCHAR(50) NOT null," +
                    "  lastname  VARCHAR(20) NOT null," +
                    "  age       NUMBER(10,0) NOT null," +
                    ");";
    @Test
    public void testReadMetaData() throws SQLException, ClassNotFoundException {
        Class.forName(org.h2.Driver.class.getCanonicalName());
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:test", "sa", "");
        Statement stmt = conn.createStatement();

        stmt.execute( ddlSql );
        checkSchemaExists(stmt);

        DatabaseMetaData metaData = conn.getMetaData();
        System.out.println( "DB Product = " + metaData.getDatabaseProductName() );
        ResultSet rs = metaData.getTables(conn.getCatalog(), null, "PERSON", null);
        int colMetas = 0;
        while (rs.next()) {
            colMetas++;
        }
        Assert.assertEquals(1, colMetas);

        conn.close();
    }

    private void checkSchemaExists(Statement stmt) throws SQLException {
        stmt.execute( "INSERT INTO person (firstname, lastname, age) values ('frank', 'ratzlow', 38)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM person");
        int rowNo=0;
        while (rs.next()) {
            rowNo++;
        }
        Assert.assertEquals(1, rowNo);
    }
}
