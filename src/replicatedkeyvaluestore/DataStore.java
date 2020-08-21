
package replicatedkeyvaluestore;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A class which provides collecitons to store data.
 */
class DataStore {

    Map<String, String> map = new HashMap<>();
   

    public synchronized void put(String key, String value) {
        map.put(key, value);
    }

    public synchronized void del(String key) {
        map.remove(key);
    }

    public String get(String key) {
        return map.get(key);
    }

}
