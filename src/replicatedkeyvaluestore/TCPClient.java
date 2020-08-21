

package replicatedkeyvaluestore;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class which represents the Client node.
 */
public class TCPClient 
{
    public void put(String ip, String port, String command, String key, String value) throws Exception 
    {
        try (Socket clientSocket = new Socket(ip, Integer.valueOf(port))) //making connection to the purposed server
        {
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            
            outToServer.writeBytes(command + '\n');
            outToServer.writeBytes(key + '\n');
            outToServer.writeBytes(value + '\n');
            outToServer.writeBytes(port + '\n');
 
            outToServer.close();
            inFromServer.close();
        }
    }
    
    public void del(String ip, String port, String command, String key, String value) throws Exception 
    {
        try (Socket clientSocket = new Socket(ip, Integer.valueOf(port))) //making connection to the purposed server
        {
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            outToServer.writeBytes(command + '\n');
            outToServer.writeBytes(key + '\n');
            outToServer.writeBytes(port + '\n');

            outToServer.close();
            inFromServer.close();
        }
    }

    public void get(String ip, String port, String command, String key) throws Exception
    {
        try (Socket clientSocket = new Socket(ip, Integer.valueOf(port))) //making connection to the purposed server
        {
            clientSocket.setSoTimeout(500);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            
            outToServer.writeBytes(command + '\n');
            outToServer.writeBytes(key + '\n');
            
            do
            {
                String replyResult = inFromServer.readLine();
                System.out.println(replyResult);
            }
            while(inFromServer.ready());
            
            outToServer.close();
            inFromServer.close();
        }
    }
    
    public void membershipConnection(String ip, String port, String command, String key, DataStore MD) throws Exception 
    {
        try (Socket clientSocket = new Socket(ip, Integer.valueOf(port)))
        {
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            outToServer.writeBytes(command + '\n');
            outToServer.writeBytes(key + '\n');
            outToServer.writeBytes((String.valueOf(clientSocket.getLocalAddress()).substring(1)) + '\n');
            
            do
            {
                String keyFromMembership = inFromServer.readLine();
                String valueFromMembership = inFromServer.readLine();
                MD.put(keyFromMembership, valueFromMembership);
            }
            while (inFromServer.ready());
            
            outToServer.close();
            inFromServer.close();
        }
    }
    
    public void store(String ip, String port, String command) throws Exception
    {
        try (Socket clientSocket = new Socket(ip, Integer.valueOf(port)))
        {
            clientSocket.setSoTimeout(500);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            outToServer.writeBytes(command + '\n');
            
            do
            {
                String replyResult = inFromServer.readLine();
                System.out.println(replyResult + '\n');
            }
            while(inFromServer.ready());
            
            outToServer.close();
            inFromServer.close();
        }
    }
    
    public void exit(String ip, String port, String command) throws Exception
    {
        try (Socket clientSocket = new Socket(ip, Integer.valueOf(port)))
        {
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(command + '\n');
            outToServer.writeBytes(port + '\n');
        }
    }
    
    public synchronized void membershipQuery(String ip, String port, DataStore MD)
    {
        try 
        {   Socket clientSocket = new Socket(ip, Integer.valueOf(port));
            {
                DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                outToServer.writeBytes("MBstore" + '\n');
                
                do
                {
                    String keyFromMembership = inFromServer.readLine();
                    String valueFromMembership = inFromServer.readLine();
                    MD.put(keyFromMembership, valueFromMembership);
                    Map<String, String> map3 = MD.map;
                }
                while(inFromServer.ready());
            }
        }
        
        catch (IOException ex) 
        {
            Logger.getLogger(TCPClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
    

