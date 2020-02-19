import entities.User;
import orm.Connector;
import orm.EntityManager;


import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Date;


public class Main {
    public static void main(String[] args) throws SQLException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        String username = "root";
        String password = "root";

        Connector.createConnection(username,password,"orm_db");
        EntityManager<User> entityManager = new EntityManager<>(Connector.getConnection());

        User user = entityManager.findFirst(User.class," username = 'Goshko'");
        entityManager.delete(user);

    }

}
