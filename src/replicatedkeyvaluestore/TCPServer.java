package replicatedkeyvaluestore;

import java.net.*;
import java.util.*;

/**
 * A class which represents Server nodes.
 */
public class TCPServer {

    DataStore DS = new DataStore();
    public DataStore MD = new DataStore();
    ArrayList<String> CD = new ArrayList<String>();
    InetAddress myIP;

    public void runReplicatedServer(String port, String membershipIP, String membershipPort) throws Exception 
    {
        ServerSocket welcomeSocket = new ServerSocket(Integer.valueOf(port));
        System.out.println("Replicated server is listening to port " + port + "... ");
        TCPClient membershipConnection = new TCPClient();
        membershipConnection.membershipConnection(membershipIP, membershipPort, "logPortIP", port, MD);
        
        new Thread(new TCPDiscovery(membershipIP,membershipPort,MD)).start();


        while (true) {
            Socket connectionSocket = welcomeSocket.accept();
            new Thread(new CommandSwitchBoard(connectionSocket, DS,MD,CD, welcomeSocket)).start();
        }
    }

    public void runMembershipServer(String mport) throws Exception {
        ServerSocket welcomeSocket = new ServerSocket(Integer.valueOf(mport));
            System.out.println("Membership server is listening to port " + mport);
        while (true) {
            Socket connectionSocket = welcomeSocket.accept();

            new Thread(new CommandSwitchBoard(connectionSocket, DS,MD,CD, welcomeSocket)).start();
        }
    }
}
