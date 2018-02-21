package com.autentia.rmi.test;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The RMI server.
 * <p>
 * Can be launched from the command line and takes care of creating the RMI registry and
 * binding remote objects to the given hostname and port.
 * <p>
 * It is assumed there is a MySQL database created like this:
 * <p>
 * CREATE DATABASE messages;
 * <br>
 * USE messages;
 * <br>
 * CREATE TABLE messages(id INTEGER PRIMARY KEY AUTO_INCREMENT, message VARCHAR(500) NOT NULL);
 * <br>
 * CREATE USER 'messages' IDENTIFIED BY 'messages';
 * <br>
 * GRANT ALL ON messages.* TO 'messages';
 * <br>
 * CREATE USER 'messages'@'localhost' IDENTIFIED BY 'messages';
 * <br>
 * GRANT ALL ON messages.* TO 'messages'@'localhost';
 */
public class MessageKeeperServer extends UnicastRemoteObject implements MessageKeeper {

    private final Connection connection;

    public MessageKeeperServer(int port) throws Exception {
        super(port);
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        connection = DriverManager.getConnection("jdbc:mysql://localhost/messages", "messages", "messages");
    }

    @Override
    public long saveMessage(String message) throws RemoteException {
        PreparedStatement stmt = null;
        ResultSet rset = null;
        try {
            stmt = connection.prepareStatement("insert into messages(message) values (?)");
            stmt.setString(1, message);
            stmt.execute();
            rset = stmt.getGeneratedKeys();
            return rset.next() ? rset.getInt(1) : 0;
        } catch (SQLException e) {
            throw new RemoteException("Could not insert message", e);
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException e) {
                    throw new RemoteException("Database error", e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    throw new RemoteException("Database error", e);
                }
            }
        }
    }

    @Override
    public List<String> findMessages(String substring) throws RemoteException {
        PreparedStatement stmt = null;
        ResultSet rset = null;
        try {
            stmt = connection.prepareStatement("select message from messages where message like ?");
            stmt.setString(1, "%" + substring + "%");
            rset = stmt.executeQuery();
            List<String> result = new ArrayList<>();
            while (rset.next()) {
                result.add(rset.getString(1));
            }
            return result;
        } catch (SQLException e) {
            throw new RemoteException("Could not find messages", e);
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException e) {
                    throw new RemoteException("Database error", e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    throw new RemoteException("Database error", e);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Params: registry-port server-host server-port");
            System.exit(-1);
        }

        int registryPort = Integer.parseInt(args[0]);
        String host = args[1];
        int serverPort = Integer.parseInt(args[2]);

        System.setProperty("java.rmi.server.hostname", host);

        System.out.println("Creating registry on port: " + registryPort);
        Registry registry = LocateRegistry.createRegistry(registryPort);
        System.out.println(String.format("Binding server object on: %s:%d", host, serverPort));
        registry.rebind("//MessageKeeper", new MessageKeeperServer(serverPort));
        for (String name: registry.list()) {
            System.out.println("Bound object: " + name);
        }
    }
}
