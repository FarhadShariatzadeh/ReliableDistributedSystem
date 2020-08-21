/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package replicatedkeyvaluestore;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class which handles communications between the membership node and server nodes.
 */
public class TCPDiscovery implements Runnable{
    
            String IP;
        String port;
        DataStore MD;
        
        public TCPDiscovery (String ip,String port,DataStore MD)
        {
            this.IP=ip;
            this.port=port;
            this.MD=MD;
        }
        
        @Override
        public void run() {   
            while (true){
                try {
                    TCPClient tc = new TCPClient();
                    tc.membershipQuery(IP, port, MD);
                    
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(TCPDiscovery.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

}
