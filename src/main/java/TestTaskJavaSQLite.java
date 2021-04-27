import java.sql.*;

/**
 * @Author Dmitry Veremeenko aka StDimensiy
 * Test Work for dbeaver.com
 * Created 27.04.2021
 * v1.0
 */
public class TestTaskJavaSQLite {
    private static Connection connection;
    private static Statement statement;
    private static ResultSet resultSet;

    public static void main(String[] args) {
        connectionInit();
        getReportOnTotalSalaryBySubconto("DEPARTMENT");
        printResult();
        connectionClose();
    }

    public static void connectionInit() {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:src/main/resources/testtascusers.db");
            statement = connection.createStatement();
            primaryLoadData();
        } catch (ClassNotFoundException e) {
            System.out.println("ОШИБКА! Библиотека sqlite.JDBC недоступна / не установлена");
        } catch (SQLException e) {
            System.out.println("ОШИБКА! Подключение к базе данных отсутствует");
        }
    }

    public static void getReportOnTotalSalaryBySubconto(String subconto) {
        // строка запроса к БД с возможностью получать сумму поля SALARY относительно группировки значений любого столбца
        String queryString = "SELECT " + subconto + ", SUM(SALARY) FROM EMPLOYEES GROUP BY " + subconto;
        try {
            resultSet = statement.executeQuery(queryString);
        } catch (SQLException e) {
            e.printStackTrace();
            resultSet = null;
        }
    }

    private static void printResult() {
        if (resultSet != null) {
            try {
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1) + " : " + resultSet.getString(2));
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public static void connectionClose() {
        if (connection == null) return;
        try {
            connection.close();
            statement.close();
            resultSet.close();
        } catch (SQLException e) {
            System.out.println("ОШИБКА! соединения не могут быть закрыты.");
        }
    }

    private static void primaryLoadData() {
        try {
            String primaryLoad = "CREATE TABLE IF NOT EXISTS EMPLOYEES (EMPLOYEE_ID INTEGER NOT NULL UNIQUE," +
                    " FIRST_NAME TEXT NOT NULL," +
                    " LAST_NAME TEXT NOT NULL," +
                    " DEPARTMENT TEXT NOT NULL," +
                    " SALARY INTEGER NOT NULL DEFAULT 0," +
                    " PRIMARY KEY( EMPLOYEE_ID  AUTOINCREMENT))";
            statement.execute(primaryLoad);
            String startQuery = "INSERT INTO EMPLOYEES(FIRST_NAME, LAST_NAME, DEPARTMENT, SALARY) VALUES ";
            statement.executeUpdate(startQuery + "('John', 'Smith', 'Development', 5000)");
            statement.executeUpdate(startQuery + "('Nick', 'Johnson', 'Development', 6000)");
            statement.executeUpdate(startQuery + "('Mary', 'Johnson', 'Sales', 4000)");
            statement.executeUpdate(startQuery + "('Cristopher', 'Robin', 'Sales', 4000)");
            statement.executeUpdate(startQuery + "('Harry', 'Gates', 'Management', 8000)");
        } catch (SQLException throwables) {
            System.out.println("Ошибка в модуле первичной загрузки значений в базу данных. Проверьте правильность запросов");
            throwables.printStackTrace();
        }
    }
}
