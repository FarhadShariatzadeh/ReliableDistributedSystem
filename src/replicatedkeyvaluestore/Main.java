package replicatedkeyvaluestore;


/*
 * References:    https://systembash.com/a-simple-java-tcp-server-and-tcp-client/
                        https://docs.oracle.com/javase/tutorial/networking/sockets/index.html
                        https://docs.oracle.com/javase/tutorial/networking/datagrams/clientServer.html
                        https://stackoverflow.com/questions/8803674/declaring-variables-inside-or-outside-of-a-loop
                        stackoverflow.com
                        docs.oracle.com
 */

/**
 * Configured Main class which takes input arguments and acts accordingly.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        String serverOrClient = args[0];
        
         /** Instantiating Servers. First step is to run a membership server. To do so
         * we just pass a port number. Then, to run any other servers, we should pass a port number
         * an the IP address of the membership server which we have instantiated from the beginning.*/
        
        if (serverOrClient.equals("ts")) 
        {
            
                if (args.length == 4)
                {
                    String kvport = args[1];
                    String mip = args[2];
                    String mport = args[3];

                    System.out.println("Replicated Server is running");
                    new TCPServer().runReplicatedServer(kvport,mip,mport); //mip & mport are Membership Server's IP and port
                }
                
                else if (args.length == 2)
                {
                    String mport = args[1];
                    System.out.println("Membership Server is running");
                    new TCPServer().runMembershipServer(mport);
                }
                else
                {
                    System.out.println("TCP Server is not available");
                }
        } 
        
        /**Instantiating client node. Depending on what command we use, we pass needed parameters.*/
        
        else if (serverOrClient.equals("tc")) 
        {
            String ip = args[1];
            String port = args[2];
            String command = args[3];

            {
                if (command.equals("put")) 
                {
                    String key = args[4];
                    String value = args[5];
                    new TCPClient().put(ip, port, command, key, value); // ip & port = IP address and port of remote node 
                } 
                else if (command.equals("get")) 
                {
                    String key = args[4];
                    String value = null;
                    new TCPClient().get(ip, port, command, key);
                } 
                else if (command.equals("del")) 
                {
                    String key = args[4];
                    String value = null;
                    new TCPClient().del(ip, port, command, key, value); // ip & port = IP address and port of remote node 
                }
                else if (command.equals("store")) 
                {
                    new TCPClient().store(ip, port, command);
                }
                else if (command.equals("exit")) 
                {
                    new TCPClient().exit(ip, port, command);
                } 
                else 
                {
                    System.out.println("Command is invalid.");
                }
            }
        }
        else
        {
                    //Default
                    System.out.println("message is not valid");
        }
    }
}
