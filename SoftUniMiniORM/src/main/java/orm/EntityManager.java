package orm;


import annotations.Column;
import annotations.Entity;
import annotations.Id;

import javax.swing.plaf.nimbus.State;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class EntityManager<E> implements DbContext<E> {
    private Connection connection;

    public EntityManager(Connection connection) {
        this.connection = connection;
    }

    public boolean persist(E entity) throws IllegalAccessException, SQLException {
        Field primary = this.getId(entity.getClass());
        primary.setAccessible(true);
        Object value = primary.get(entity);

        if (value == null || (int) value <= 0) {
            return this.doInsert(entity, primary);
        }
        return doUpdate(entity, primary);
    }

    public Iterable<E> find(Class<E> table) {
        return null;
    }

    public Iterable<E> find(Class<E> table, String where) {
        return null;
    }

    public E findFirst(Class<E> table) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {


        return this.findFirst(table,null);
    }

    public E findFirst(Class<E> table, String where) throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Statement statement = connection.createStatement();
        String query = "SELECT * FROM " + this.getTableName(table) +
                " WHERE 1 " + (where != null ? " AND " + where : "") + " LIMIT 1";
        ResultSet resultSet = statement.executeQuery(query);
        E entity = table.newInstance();
        resultSet.next();
        this.fillEntity(table, resultSet, entity);
        return entity;
    }

    private Field getId(Class entity) {
        return Arrays.stream(entity.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Id.class))
                .findFirst()
                .orElseThrow(() ->
                        new UnsupportedOperationException("Entity does not have primary key"));
    }

    private String getTableName(Class entity) {
        String tableName = "";

        tableName = ((Entity) entity.getAnnotation(Entity.class)).name();

        if (tableName.equals("")) {
            tableName = entity.getSimpleName();
        }
        return tableName;
    }

    private String getColumnName(Field field) {
        String columnName = field.getAnnotation(Column.class).name();

        if (columnName.isEmpty()) {
            columnName = field.getName();
        }

        return columnName;
    }

    private boolean doInsert(E entity, Field primary) throws IllegalAccessException, SQLException {
        if (!checkIfTableExists(entity.getClass())) {
            this.doCreate(entity.getClass());
        } else {
            if (!this.checkIfThereIsNewColumn(entity.getClass().getDeclaredFields())) {
                this.doAlter(entity.getClass());
            }
        }

        String tableName = this.getTableName(entity.getClass());
        String query = "INSERT INTO " + tableName + " (";
        String columns = "";
        String values = " ( ";
        Field[] fields = entity.getClass().getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {

                columns += "`" + this.getColumnName(field) + "`";

                Object value = field.get(entity);

                if (value instanceof Date) {
                    values += "'" + new SimpleDateFormat("yyyy-MM-dd").format(value) + "'";
                } else {
                    values += "'" + value + "'";
                }
                if (i < fields.length - 1) {
                    values += ",";
                    columns += ",";
                }
            }

        }

        query += columns + ") VALUES " + values + ")";
        return connection.prepareStatement(query).execute();
    }

    private boolean doUpdate(E entity, Field primary) throws IllegalAccessException, SQLException {
        String tableName = this.getTableName(entity.getClass());
        String query = "UPDATE " + tableName;
        String columns = " SET ";
        String where = " WHERE ";

        Field[] fields = entity.getClass().getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            Object value = field.get(entity);
            if (field.isAnnotationPresent(Id.class)) {
                where += this.getColumnName(field) + " = " + value;
            } else {
                if (value instanceof Date) {
                    columns += this.getColumnName(field) + " = '"
                            + new SimpleDateFormat("yyyy-MM-dd").format(value) + "'";
                } else if (value instanceof Integer) {
                    columns += this.getColumnName(field) + " = " + value;
                } else {
                    columns += this.getColumnName(field) + " = '" + value + "'";
                }

                if (i < fields.length - 1) {
                    columns += ",";
                }
            }
        }
        query += columns + where;

        return connection.prepareStatement(query).execute();
    }

    private void fillEntity(Class<E> table, ResultSet rs, E entity) throws SQLException, IllegalAccessException {
        Field[] fields = table.getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            String name = field.getName();
            this.fillField(field, entity, rs, field.getAnnotation(Column.class).name());
        }
    }

    private <E> void doCreate(Class entity) throws SQLException {
        String tableName = this.getTableName(entity);
        String query = "CREATE TABLE " + tableName + " ( ";
        String columns = "";

        Field[] fields = entity.getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            columns += this.getColumnName(field) + " " + getDBType(field);

            if (i < fields.length - 1) {
                columns += ", ";
            }
        }
        query += columns + ")";
        connection.prepareStatement(query).execute();
    }

    private <E> void doAlter(Class entity) throws SQLException {
        String tableName = this.getTableName(entity);
        String query = "ALTER TABLE " + tableName + " ADD ";
        String toAdd = "";


        Field[] fields = entity.getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            if (!this.checkIfColumnExists(field)) {
                toAdd += this.getColumnName(field) + " " + this.getDBType(field);
            }
        }

        query += String.join(",", toAdd);
        connection.prepareStatement(query).execute();
    }

    private String getDBType(Field field) {
        field.setAccessible(true);
        if (field.isAnnotationPresent(Id.class)) {
            return "INT AUTO_INCREMENT PRIMARY KEY";
        } else if (field.getType() == int.class || field.getType() == Integer.class) {
            return "INT";
        } else if (field.getType() == Date.class) {
            return "DATE";
        } else if (field.getType() == String.class) {
            return "VARCHAR (50)";
        }
        return "";
    }

    private void fillField(Field field, Object instance, ResultSet rs, String fieldName) throws SQLException, IllegalAccessException {
        field.setAccessible(true);
        if (field.getType() == int.class || field.getType() == Integer.class) {
            field.set(instance, rs.getInt(fieldName));
        } else if (field.getType() == Date.class) {
            field.set(instance, rs.getDate(fieldName));
        } else if (field.getType() == String.class) {
            field.set(instance, rs.getString(fieldName));
        }
    }

    private boolean checkIfTableExists(Class entity) throws SQLException {
        String query = "SELECT TABLE_NAME FROM information_schema.TABLES\n" +
                "WHERE TABLE_SCHEMA = 'orm_db'\n" +
                "AND TABLE_NAME = " + "'" + this.getTableName(entity) + "'";
        ResultSet rs = connection.prepareStatement(query).executeQuery();
        return rs.next();
    }

    private boolean checkIfColumnExists(Field field) throws SQLException {
        String query = "SELECT `COLUMN_NAME`\n" +
                "FROM `INFORMATION_SCHEMA`.`COLUMNS`\n" +
                "WHERE `TABLE_SCHEMA`='orm_db'\n" +
                "  AND `TABLE_NAME`='users';";

        ResultSet rs = connection.prepareStatement(query).executeQuery();
        while (rs.next()) {

            if (this.getColumnName(field).equals(rs.getString(1))) {
                return true;
            }
        }
        return false;
    }

    private boolean checkIfThereIsNewColumn(Field[] fields) throws SQLException {
        boolean result = true;

        for (Field field : fields) {

            result = this.checkIfColumnExists(field);

            if (!result) {
                return false;
            }
        }

        return result;
    }

    @Override
    public boolean delete(E entity) throws SQLException, IllegalAccessException {
        String query = "DELETE FROM " + this.getTableName(entity.getClass())
                + " WHERE `id` = ";


        for (Field field : entity.getClass().getDeclaredFields()) {
            field.setAccessible(true);

            if (field.isAnnotationPresent(Id.class)) {
                Object value = field.get(entity);
                query += value;
            }
        }

        return connection.prepareStatement(query).execute();
    }
}
