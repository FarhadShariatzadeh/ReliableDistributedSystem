package replicatedkeyvaluestore;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The heart of the application. Makes decisions based on user inputs.
 */
public class CommandSwitchBoard implements Runnable
{
    private Socket connectionSocket = null;
    private DataStore dataStore;
    private DataStore membershipDirectory;
    private ArrayList<String> commandDirectory;
    private ServerSocket welcomeSocket;

    /**
     *
     * @param clientSocket
     * @param dataStore
     * @param membershipDirectory
     * @param commandDirectory
     * @param srSocket
     */
    public CommandSwitchBoard(Socket clientSocket, DataStore dataStore, DataStore membershipDirectory, ArrayList<String> commandDirectory, ServerSocket srSocket)
    {
        this.connectionSocket = clientSocket;
        this.dataStore = dataStore;
        this.membershipDirectory = membershipDirectory;
        this.commandDirectory = commandDirectory;
        this.welcomeSocket = srSocket;
    }

    /**
     *
     */
    @Override
    public void run() 
    {
        try
        {
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            String command = inFromClient.readLine();
            switch (command) {
                
                case "put":
                    HandlePut(inFromClient, outToClient);
                    break;
                    
                //explanations for command "del" is pretty similar to command "put"
                case "del":
                    HandleDelete(inFromClient);
                    break;
                    
                case "dput1":
                    HandleDput1(inFromClient, outToClient);
                    break;
                    
                case "ddel1":
                    HandleDdel1(inFromClient, outToClient);
                    break;
                    
                case "dput2":
                    HandleDput2(inFromClient);
                    break;
                    
                case "ddel2":
                    HandleDdel2(inFromClient);
                    break;
                    
                case "get":
                    HandleGet(inFromClient, outToClient);
                    break;
                    
                case "delold":
                    HandleDelOld(inFromClient);
                    break;
                    
                case "store":
                    HandleStore(outToClient);
                    break;
                    
                case "logPortIP":
                    HandleLogPort(inFromClient, outToClient);
                    break;

                case "MBstore":
                    HandleMembershipStore(outToClient);
                    break;
                    
                case "exit":
                    HandleExit(inFromClient);
                    break;
                    
                case "quit":
                    HandleQuit(inFromClient);
                    break;
                    
                default:
                    outToClient.writeBytes("Command not understood!");
            }
            
            outToClient.close();
            inFromClient.close();

        }

        catch (IOException ex)
        {
            Logger.getLogger(CommandSwitchBoard.class.getName()).log(Level.SEVERE, null, ex);
        }

        catch (Exception ex)
        {
            Logger.getLogger(CommandSwitchBoard.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }

    private void HandlePut(BufferedReader inFromClient, DataOutputStream outToClient) throws IOException {
        try
        {
            String userKey = inFromClient.readLine();//user entry
            String userValue = inFromClient.readLine();//user entry
            String leaderPort = inFromClient.readLine();//user entry
            String membershipPort = inFromClient.readLine();//user entry

                    /*First, leader-node checks its Command Directory (CD) to see if it is working with the given key
                    already. If it's working, it'll break. Otherwise, it will lock the key by adding it to its Command Directory.
                    */
            if (!commandDirectory.contains(userKey) || commandDirectory.isEmpty()) {
                commandDirectory.add(userKey);
            } else {
                return;
            }

            //The, leader-node creates an ArrayList to temporarily save replies ("ASK" or "ABORT") it gets from all other nodes in the network
            ArrayList<String> check = new ArrayList<>();

            //Leader iterates through Membership Directory (MD) to retive Ports and IP addresses of available nodes in the network
            Map<String, String> map8 = membershipDirectory.map;
            LockUserKey(check, map8, leaderPort, userKey);

            //then, leader iterates over ArrayList to see if there is any "ABORT" message it has received back from a node in the network
            if (!check.contains("ABORT") && check != null) {
                SendingDput2(leaderPort, membershipPort, userKey, userValue);
            } else {
                AbortingFromDput2(leaderPort, membershipPort, userKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    private void AbortingFromDput2( String leaderPort, String membershipPort, String userKey) throws IOException {
        Map<String, String> map = membershipDirectory.map;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            if (mykey != null && myvalue != null) {
                if (mykey.equals(leaderPort)) {
                    continue;
                }
                if (mykey.equals(membershipPort)) {
                    continue;
                }

                Socket clientSocket = new Socket(myvalue, Integer.valueOf(mykey));
                DataOutputStream outToClient11 = new DataOutputStream(clientSocket.getOutputStream());

                commandDirectory.remove(userKey);
                outToClient11.writeBytes("quit" + '\n');
                outToClient11.writeBytes(userKey + '\n');
            }
        }

    }

    private void SendingDput2( String leaderPort, String membershipPort, String userKey, String userValue) throws IOException {
        //if no "ABORT" message has been received, Leader goes ahead and sends "dput2" command to all nodes
        Map<String, String> map = membershipDirectory.map;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            if (mykey != null && myvalue != null) {
                if (mykey.equals(leaderPort)) {
                    continue;
                }
                if (mykey.equals(membershipPort)) {
                    continue;
                }

                Socket clientSocket = new Socket(myvalue, Integer.valueOf(mykey));
                BufferedReader inFromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());

                outToClient.writeBytes("dput2" + '\n');
                outToClient.writeBytes(userKey + '\n');
                outToClient.writeBytes(userValue + '\n');

                outToClient.close();
                inFromClient.close();
            }
        }

        dataStore.put(userKey, userValue);
        commandDirectory.remove(userKey);
        System.out.println("Received: put key: " + userKey + " value: " + userValue);
    }

    private void LockUserKey( ArrayList<String> check, Map<String, String> map, String leaderPort, String userKey) throws IOException {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            if (mykey != null && myvalue != null) {
                if (mykey.equals(leaderPort)) {
                    continue;
                }

                Socket clientSocket = new Socket(myvalue, Integer.valueOf(mykey));
                BufferedReader inFromClient2 = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                DataOutputStream outToClient2 = new DataOutputStream(clientSocket.getOutputStream());

                //in each iteration, leader makes a connection with a node and sends command "dput1" and saves the reply it receives back from the node in the temporary ArrayList
                outToClient2.writeBytes("dput1" + '\n');
                outToClient2.writeBytes(userKey + '\n');
                String reply = inFromClient2.readLine();
                check.add(reply);
            }
        }
    }

    private void HandleDelete(BufferedReader inFromClient) throws IOException{
        //user entries
        String userKey = inFromClient.readLine();
        String leaderPort = inFromClient.readLine();
        String membershipPort = inFromClient.readLine();

        if(!commandDirectory.contains(userKey) || commandDirectory.isEmpty())
        {
            commandDirectory.add(userKey);
        }
        else
        {
            return;
        }

        ArrayList<String> check = new ArrayList<>();

        Map<String, String> newMap = membershipDirectory.map;

        TryRemoveNode(check, newMap, leaderPort, userKey);

        if(!check.contains("ABORT") && check != null)
        {
            HandleAbort(membershipPort, leaderPort, userKey);
        }

        else
        {
            UnlockUserKey(membershipPort, leaderPort, userKey);
        }
    }

    private void UnlockUserKey(String membershipPort, String leaderPort, String userKey) throws IOException {
        Map<String, String> map = membershipDirectory.map;
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            if (mykey != null && myvalue != null)
            {
                if (mykey.equals(leaderPort)){ continue;}
                if (mykey.equals(membershipPort)){ continue;}

                Socket clientSocket = new Socket(myvalue, Integer.valueOf(mykey));
                DataOutputStream newOutToClient = new DataOutputStream(clientSocket.getOutputStream());

                commandDirectory.remove(userKey);
                newOutToClient.writeBytes("quit"+ '\n');
                newOutToClient.writeBytes(userKey + '\n');
            }
        }
    }

    private void TryRemoveNode( ArrayList<String> check, Map<String, String> newMap, String leaderPort, String userKey) throws IOException {
        for (Map.Entry<String, String> entry : newMap.entrySet())
        {
            if(entry.getKey().equals(leaderPort)){ continue;}
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            if (mykey != null && myvalue != null)
            {
                Socket clientSocket = new Socket(myvalue, Integer.valueOf(mykey));
                BufferedReader inFromClient6 = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                DataOutputStream outToClient6 = new DataOutputStream(clientSocket.getOutputStream());

                outToClient6.writeBytes("ddel1"+ '\n');
                outToClient6.writeBytes(userKey+ '\n');
                String reply = inFromClient6.readLine();
                check.add(reply);
            }
        }
    }

    private void HandleAbort(String membershipPort, String leaderPort, String userKey) throws IOException {
        Map<String, String> map = membershipDirectory.map;
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            if (mykey != null && myvalue != null)
            {
                if (mykey.equals(leaderPort)) { continue;}
                if (mykey.equals(membershipPort)) { continue;}

                Socket clientSocket = new Socket(myvalue, Integer.valueOf(mykey));
                BufferedReader newInFromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                DataOutputStream neewOutToClient = new DataOutputStream(clientSocket.getOutputStream());

                neewOutToClient.writeBytes("ddel2"+ '\n');
                neewOutToClient.writeBytes(userKey+ '\n');

                neewOutToClient.close();
                newInFromClient.close();
            }
        }
        dataStore.del(userKey);
        commandDirectory.remove(userKey);
        System.out.println("Received: del key: " + userKey+ '\n' );
    }

    private void HandleDput1(BufferedReader inFromClient, DataOutputStream outToClient) throws IOException {
        String userKey = inFromClient.readLine();

        if(!commandDirectory.contains(userKey) || commandDirectory.isEmpty())
        {
            commandDirectory.add(userKey);
            outToClient.writeBytes("ACK" + '\n');
        }
        else
        {
            outToClient.writeBytes("ABORT" + '\n');
        }
    }

    private void HandleDdel1(BufferedReader inFromClient, DataOutputStream outToClient) throws IOException {
        String userKey = inFromClient.readLine();

        if(!commandDirectory.contains(userKey) || commandDirectory.isEmpty())
        {
            commandDirectory.add(userKey);
            outToClient.writeBytes("ACK" + '\n');
        }
        else
        {
            outToClient.writeBytes("ABORT" + '\n');
        }

    }

    private void HandleDput2(BufferedReader inFromClient) throws IOException{
        String key = inFromClient.readLine();
        String value = inFromClient.readLine();
        dataStore.put(key, value);
        commandDirectory.remove(key);
        System.out.println("Received: put key: " + key + " value: " + value);
    }

    private void HandleDdel2(BufferedReader inFromClient) throws IOException{
        String key = inFromClient.readLine();
        dataStore.del(key);
        commandDirectory.remove(key);
        System.out.println("Received: del key: " + key );
    }

    private void HandleGet(BufferedReader inFromClient, DataOutputStream outToClient) throws IOException {
        String key = inFromClient.readLine();
        String myVal = dataStore.get(key);
        if(myVal == null)
        {
            String reply = "Key/Value is not available.";
            outToClient.writeBytes(reply + '\n');
            System.out.println(reply);
        }
        else
        {
            String reply = "key:" + key + ":value:" + myVal;
            outToClient.writeBytes(reply + '\n');
            System.out.println("key:" + key + ":value:" + myVal);
        }
    }

    private void HandleDelOld(BufferedReader inFromClient) throws IOException {
        String key = inFromClient.readLine();
        dataStore.map.remove(key);
        System.out.println("delete key=" + key);
    }

    private void HandleStore(DataOutputStream outToClient) throws IOException {
        Map<String, String> map3 = dataStore.map;
        for (Map.Entry<String, String> entry : map3.entrySet()) {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            String reply = "key:" + mykey + ":value:" + myvalue + '\n';
            outToClient.writeBytes(reply);
            System.out.println("key:" + mykey + ":value:" + myvalue);
        }
        outToClient.writeBytes("Quit");
    }

    private void HandleLogPort(BufferedReader inFromClient, DataOutputStream outToClient) throws IOException {
        String keyIMDB = inFromClient.readLine();
        String valueIMDB = inFromClient.readLine();
        membershipDirectory.put(keyIMDB, valueIMDB);

        Map<String, String> map = membershipDirectory.map;
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            outToClient.writeBytes(mykey + '\n');
            outToClient.writeBytes(myvalue + '\n');
        }
    }

    private void HandleMembershipStore(DataOutputStream outToClient) throws IOException {
        Map<String, String> map = membershipDirectory.map;
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String mykey = entry.getKey();
            String myvalue = entry.getValue();
            if (mykey != null && myvalue != null)
            {
                outToClient.writeBytes(mykey + '\n');
                outToClient.writeBytes(myvalue + '\n');
            }
        }
    }

    private void HandleExit(BufferedReader inFromClient) throws IOException {
        String exitPort = inFromClient.readLine();
        membershipDirectory.del(exitPort);
        welcomeSocket.close();
        System.out.println("Server is shut down");
    }

    private void HandleQuit(BufferedReader inFromClient) throws IOException {
        String userKey = inFromClient.readLine();
        commandDirectory.remove(userKey);
    }

}       