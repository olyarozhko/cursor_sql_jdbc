package com.rozhko;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import java.sql.*;

public class DatabaseOperation {
    public static void main(String[] args) {
        String url = "jdbc:sqlserver://localhost;" + "encrypt=false;";
        String user1 = "us1";
        String password = "1";

        try {
            DriverManager.registerDriver(new SQLServerDriver());
            Connection connection = DriverManager.getConnection(url, user1, password);
            Statement statement = connection.createStatement();

            System.out.print("--- Query 'select * from test.dbo.promotions' ---\n");
            ResultSet resultSet = statement.executeQuery("select * from test.dbo.promotions");
            while (resultSet.next()) {
                System.out.print(resultSet.getDate(4));
                System.out.print("\t" + resultSet.getString(2) + "\n");
            }

            System.out.print("\n--- Query with 'where' ---\n");
            ResultSet resultSetWhere = statement.executeQuery("select * from test.dbo.promotions " +
                    "where discount = 0.25");
            while (resultSetWhere.next()) {
                System.out.print(resultSetWhere.getDate(4));
                System.out.print("\t" + resultSetWhere.getString(2) + "\n");
            }

            System.out.print("\n--- Query 'select * from test.dbo.persons where surname LIKE '%o%'' ---\n");
            ResultSet resultSetWhereLike = statement.executeQuery("select * from test.dbo.persons " +
                    "where surname LIKE '%o%'");
            while (resultSetWhereLike.next()) {
                System.out.print(resultSetWhereLike.getString(2));
                System.out.print("\t" + resultSetWhereLike.getString(3) + "\n");
            }

            System.out.print("""
                                        
                    --- Query 'select TOP 2 * from test.dbo.emails where email like '%.net' or email like '%.ca'' ---
                    """);
            ResultSet resultSetWhereOrLike = statement.executeQuery("select TOP 2 * from test.dbo.emails " +
                    "where email like '%.net' or email like '%.ca'");
            while (resultSetWhereOrLike.next()) {
                System.out.print(resultSetWhereOrLike.getString(2) + "\n");
            }

            System.out.print("\n--- Query JOIN' ---\n");
            ResultSet resultSetJoin = statement.executeQuery("""
                    select p.name, p.surname, e.email, prom.promotion_name
                    from test.dbo.persons p
                    INNER JOIN test.dbo.emails e
                    on p.email_id = e.id
                    INNER JOIN test.dbo.person_promotions pp
                    on p.email_id = pp.email_id
                    INNER JOIN test.dbo.promotions prom
                    on pp.promotion_id = prom.promotion_id\s""");
            while (resultSetJoin.next()) {
                System.out.print(resultSetJoin.getString(1));
                System.out.print("\t\t\t" + resultSetJoin.getString(2));
                System.out.print("\t\t\t" + resultSetJoin.getString(3));
                System.out.print("\t\t\t" + resultSetJoin.getString(4) + "\n");
            }

            resultSetWhere.close();
            resultSet.close();
            resultSetWhereLike.close();
            resultSetWhereOrLike.close();
            resultSetJoin.close();
            statement.close();
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
