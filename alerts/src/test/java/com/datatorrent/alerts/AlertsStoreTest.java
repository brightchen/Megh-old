package com.datatorrent.alerts;

import com.datatorrent.alerts.Store.AlertsStore;
import java.util.*;

import com.datatorrent.alerts.Store.DoublyLinkedList;
import com.datatorrent.alerts.Store.Node;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class AlertsStoreTest {

    private TestStore store ;
    private ArrayList<AlertMessage> messageList ;
    private int len = 500 ;

    public class LevelChange implements LevelChangeNotifier {

        @Override
        public void OnChange( AlertMessage message ) {

            // System.out.println("Change Notifier " + message.getId());
        }
    }

    public static class TestStore extends AlertsStore {

        public TestStore(LevelChangeNotifier levelChangeNotifier, Config config) {

            super(levelChangeNotifier, config) ;
        }

        public synchronized boolean allSorted() {

            Iterator<Map.Entry<Long,DoublyLinkedList>> it = alertsWithSameTimeout.entrySet().iterator() ;

            while ( it.hasNext() ) {

                Map.Entry<Long,DoublyLinkedList> entry = it.next();
                DoublyLinkedList list = entry.getValue();

                Node curr = list.head.next;

                while ( curr != list.tail ) {

                    if ( curr.next != list.tail ) {

                        if ( curr.lastNotified.getTime() - curr.next.lastNotified.getTime() > 0) return false ;
                    }

                    curr = curr.next ;
                }
            }

            return true ;
        }
    }

    private AlertMessage generator(Integer id, Integer level) {
        AlertMessage message = new AlertMessage() ;
        message.setFlag(true);
        message.setEventId(id);
        message.setLevel(1) ;
        message.setAppId(id.toString());

        return message ;
    }

    @Before
    public void Init() {

        store = new TestStore(new LevelChange(), new ConfigImpl());
        messageList = new ArrayList<>() ;

        for ( int i = 0 ; i < len ; ++i ) {

            AlertMessage message = generator(i, 1) ;

            Random rnd = new Random() ;
            Long n =(long) rnd.nextInt(100) + 1 ;
            store.put(n, 1, message);
            messageList.add(message) ;
        }
    }

    @Test
    public void testMessageStore_addRemove() throws Exception {


        for ( int i = 0 ; i < len ; ++i ) {

            assertTrue ( " Message present ", store.isPresent(messageList.get(i)) ) ;
        }

        for ( AlertMessage message: messageList) {

            store.remove(message);
        }

        for ( int i = 0 ; i < len ; ++i ) {

            assertFalse(" Message not present ", store.isPresent(messageList.get(i)));
        }

        for ( AlertMessage message: messageList) {

            store.remove(message);
        }
    }

    @Test
    public void testMessageStore_sortedlist() throws Exception {

        for ( int i = 0 ; i < 10 ; ++i ) {
            assertTrue("All the queues are sorted", store.allSorted());
            Thread.sleep(10000);
        }
    }
}
