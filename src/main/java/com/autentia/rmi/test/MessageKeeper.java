package com.autentia.rmi.test;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface MessageKeeper extends Remote {

    long saveMessage(String message) throws RemoteException;
    List<String> findMessages(String substring) throws RemoteException;
}
