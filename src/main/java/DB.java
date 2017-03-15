import java.sql.*;

/**
 * Created by iLeonidze on 17.02.2017.
 */
public class DB {
    private int dbIDLength = 20;
    private int dbConnectionTimeout = 2;
    private Connection connection = null;

    public DB() {
    }

    public boolean isDBConnected() {
        try {
            return connection.isValid(dbConnectionTimeout);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean init() {
        if (isDBConnected()) {
            System.out.print("Соединение с базой уже установлено");
            return true;
        }
        System.out.print("Ищем драйвер БД: ");
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("ОК");
        } catch (ClassNotFoundException e) {
            System.out.println("Не найден");
            return false;
        }
        System.out.print("Подключаемся к базе: ");
        try {
            connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/xe", "ILEONIDZE", "alpine");
            System.out.println("ОК");
        } catch (SQLException e) {
            System.out.print("Ошибка - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
            return false;
        }
        return true;
    }

    public boolean isTableExists(String tableName) {
        try {
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet tables = dbm.getTables(null, null, tableName.toUpperCase(), null);
            if (tables.next()) return true;
        } catch (SQLException e) {
            System.out.print("Возникли проблемы при подключении к БД");
        }
        return false;
    }

    public boolean isDBReady() {
        return idDBStructureHealthy() || regenerateDBStructure();
    }

    public boolean regenerateDBStructure() {
        boolean regenerationErrorOccurred = false;
        System.out.println("Восстановление структуры БД");
        System.out.print("Таблица shops: ");
        if (!isTableExists("SHOPS")) {
            PreparedStatement preparedStatement;
            try {
                preparedStatement = connection.prepareStatement("CREATE TABLE shops(id NUMERIC(20) NOT NULL PRIMARY KEY, name VARCHAR(64), address VARCHAR(128))");
                /*preparedStatement.setInt(1, dbIDLength); // id
                preparedStatement.setInt(2, 64); // name
                preparedStatement.setInt(3, 128); // address*/
                preparedStatement.executeQuery();
                System.out.println("Восстановлена");
            } catch (SQLException e) {
                System.out.println("Не удалось восстановить - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
                e.printStackTrace();
                regenerationErrorOccurred = true;
            }
        } else {
            System.out.println("Не требует восстановления");
        }
        System.out.print("Таблица categories: ");
        if (!isTableExists("categories")) {
            PreparedStatement preparedStatement;
            try {
                preparedStatement = connection.prepareStatement("CREATE TABLE categories(id NUMERIC(20) NOT NULL PRIMARY KEY, parent_cat_id NUMERIC(20) NULL, name VARCHAR(64), FOREIGN KEY (parent_cat_id) REFERENCES categories(id))");
                preparedStatement.executeQuery();
                System.out.println("Восстановлена");
            } catch (SQLException e) {
                System.out.println("Не удалось восстановить - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
                e.printStackTrace();
                regenerationErrorOccurred = true;
            }
        } else {
            System.out.println("Не требует восстановления");
        }
        System.out.print("Таблица goods: ");
        if (!isTableExists("goods")) {
            PreparedStatement preparedStatement;
            try {
                preparedStatement = connection.prepareStatement("CREATE TABLE goods(id NUMERIC(20) NOT NULL PRIMARY KEY, cat_id NUMERIC(20), name VARCHAR(64), price INT DEFAULT 0 NOT NULL, count INT DEFAULT 0 NOT NULL, FOREIGN KEY (cat_id) REFERENCES categories(id))");
                preparedStatement.executeQuery();
                System.out.println("Восстановлена");
            } catch (SQLException e) {
                System.out.println("Не удалось восстановить - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
                e.printStackTrace();
                regenerationErrorOccurred = true;
            }
        } else {
            System.out.println("Не требует восстановления");
        }
        System.out.print("Таблица links: ");
        if (!isTableExists("links")) {
            PreparedStatement preparedStatement;
            try {
                preparedStatement = connection.prepareStatement("CREATE TABLE links(shop_id NUMERIC(20) NOT NULL, cat_id NUMERIC(20) NOT NULL, FOREIGN KEY (shop_id) REFERENCES shops(id), FOREIGN KEY (cat_id) REFERENCES categories(id))");
                preparedStatement.executeQuery();
                System.out.println("Восстановлена");
            } catch (SQLException e) {
                System.out.println("Не удалось восстановить - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
                e.printStackTrace();
                regenerationErrorOccurred = true;
            }
        } else {
            System.out.println("Не требует восстановления");
        }
        return !regenerationErrorOccurred;
    }

    public boolean idDBStructureHealthy() {
        System.out.print("Состояние структуры БД: ");
        if (isTableExists("shops") && isTableExists("categories") && isTableExists("goods") && isTableExists("links")) {
            System.out.println("ОК");
            return true;
        } else {
            System.out.println("Повреждена");
            return false;
        }
    }

    public boolean dropAllTables() {
        return dropTable("links") && dropTable("goods") && dropTable("shops") && dropTable("categories");
    }

    public boolean dropTable(String tableName) {
        if (!isTableExists(tableName)) return true;
        Statement preparedStatement = null;
        System.out.print("Сброс таблицы " + tableName + "... ");
        try {
            preparedStatement = connection.createStatement();
            preparedStatement.executeUpdate("DROP TABLE " + tableName);
            preparedStatement.close();
            System.out.println("Выполнено");
            return true;
        } catch (SQLException e) {
            System.out.println("Ошибка - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
            e.printStackTrace();
        }
        return false;
    }

    public boolean isBatchExecutionSupported(){
        try {
            return connection.getMetaData().supportsBatchUpdates();
        }catch(Exception e){
            System.out.println("Во время проверки поддержки пакетного исполнения произошла ошибка");
            return false;
        }
    }

    public void commit(){
        try {
            connection.commit();
        }catch(Exception e){
            System.out.println("При коммите в БД произошла ошибка");
        }
    }

    public boolean insert(String sql){
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public int countTableRaws(String tableName){
        if (!isTableExists(tableName)) return -1;
        Statement preparedStatement;
        try {
            preparedStatement = connection.createStatement();
            ResultSet rs  = preparedStatement.executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            int size = rs.getInt(1);
            preparedStatement.close();
            return size;
        } catch (SQLException e) {
            System.out.println("Ошибка при получении количества строк из таблицы "+tableName+" - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
            e.printStackTrace();
        }
        return -1;
    }

    public int[] executeRawBatch(String[] raws){
        try {
            Statement st = connection.createStatement();
            for (String raw : raws) {
                st.addBatch(raw);
            }
            int[] result = st.executeBatch();
            st.close();
            return result;
        }catch(SQLException e){
            System.out.println("Ошибка - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void proceedHierarchy(boolean print, int limit){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT lpad(' ', level)||name as Tree FROM categories WHERE ROWNUM < "+limit+" START WITH parent_cat_id is null CONNECT BY PRIOR id = parent_cat_id ORDER SIBLINGS BY name");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    if(print) System.out.print(columnValue);
                }
                if(print) System.out.println("");
            }
            rs.close();
            st.close();
            //return result;
        }catch(SQLException e) {
            System.out.println("Ошибка - код: " + e.getErrorCode() + ", причина: " + e.getLocalizedMessage());
            e.printStackTrace();
            //return null;
        }
    }

    public void updateGoodName(int id, String name){
        String query = "CREATE PROCEDURE updateGoodName(goodID number, goodName varchar(16)) AS BEGIN UPDATE goods SET name=goodName WHERE id=goodID; END;";
    }
}