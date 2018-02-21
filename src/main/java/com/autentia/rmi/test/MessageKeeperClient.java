package com.autentia.rmi.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Random;

/**
 * The RMI client.
 * <p>
 * Takes a text file and sends each line to the RMI server as a new message.
 * <br>
 * Then searchs all messages including a random substring, just to generate more load on the server.
 */
public class MessageKeeperClient {

    private final MessageKeeper messageKeeper;
    private final Random random;

    public MessageKeeperClient(MessageKeeper messageKeeper) {
        this.messageKeeper = messageKeeper;
        random = new Random();
    }

    public void processFileLines(String path) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            while (true) {
                String line = reader.readLine();
                if (line == null || line.length() == 0) {
                    break;
                }
                processLine(line);
            }
        }
    }

    private void processLine(String line) throws Exception {
        System.out.println("Sending message: " + line);
        messageKeeper.saveMessage(line);
        int start = random.nextInt(line.length());
        int end = 1 + start + random.nextInt(line.length() - start);
        String substring = line.substring(start, end);
        System.out.println("Finding messages: " + substring);
        List<String> messages = messageKeeper.findMessages(substring);
        System.out.println(String.format("Got %d messages for substring: %s", messages.size(), substring));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Params: registry-host registry-port messages-file-path");
            System.exit(-1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String path = args[2];

        System.out.println(String.format("Binding to registry: %s:%d", host, port));
        Registry registry = LocateRegistry.getRegistry(host, port);
        MessageKeeper proxy = (MessageKeeper) registry.lookup("//MessageKeeper");
        MessageKeeperClient client = new MessageKeeperClient(proxy);

        System.out.println("Processing file: " + path);
        client.processFileLines(path);
    }
}
