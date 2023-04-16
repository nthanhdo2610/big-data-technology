package miu.edu.bdt.producer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

//@TestMethodOrder(MethodOrderer.MethodName.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HiveConnectionTests {
    @BeforeAll
    static void beforeAll(){
        System.out.println("BeforeAll=====");
    }

    @AfterAll
    static void afterAll(){
        System.out.println("AfterAll=====");
    }

    @BeforeEach
    void setUp() {
        System.out.println("BeforeEach=====");
    }

    @AfterEach
    void tearDown() {
        System.out.println("AfterEach=====");
    }

    @Test
    @Order(2)
    public void test1() throws SQLException, ClassNotFoundException {
        System.out.println("test1=====");
        Connection con = null;
        try {
//            String conStr = "jdbc:hive2://localhost:8022/default";
//            String conStr = "jdbc:hive2://127.0.0.1:10000/default";
            String conStr = "jdbc:hive2:localhost:10000/default";
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            con = DriverManager.getConnection(conStr, "", "");
            Statement stmt = con.createStatement();
//            stmt.executeQuery("CREATE DATABASE emp");
            System.out.println("Database emp created successfully.");
        } catch (Exception ex) {
            ex.printStackTrace();
            throw  ex;
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @Order(1)
    public void test2(){
        System.out.println("test2=====");

    }
}
