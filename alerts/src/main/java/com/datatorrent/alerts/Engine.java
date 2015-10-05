package com.datatorrent.alerts;

 import com.datatorrent.alerts.Store.Store;
import com.datatorrent.alerts.action.email.EmailNotificationTuple;
import com.datatorrent.alerts.conf.Config;
import com.datatorrent.api.DefaultOutputPort;
 import com.datatorrent.common.util.BaseOperator;
 import com.datatorrent.api.Context;
 import com.datatorrent.api.DefaultInputPort;

 import java.util.List;

/**
 * @since 2.1.0
 */
public class Engine extends BaseOperator
{
    Store store;
    Integer DefaultWaitTime = 30000 ;
    Config config = new Config();

    public final transient DefaultOutputPort<EmailNotificationTuple> messageOutput = new DefaultOutputPort<EmailNotificationTuple>();

    public class LevelChange implements LevelChangeNotifier {

        @Override
        public void OnChange( Message message ) {
            sendMessage(message);
        }
    }

    public transient final DefaultInputPort<Message> messageInput = new DefaultInputPort<Message>()
    {
        @Override
        public void process( Message message )
        {
            if ( message.isFlag() ) {
                Integer timeout = message.timeOutForCurrLevel() ;

                enrich(message) ;
                store.put( timeout, message.getCurrentLevel(), message);
                sendMessage(message);
            }
            else {
                store.remove(message) ;
            }
        }

        private void enrich(Message message) {

            //TODO : Get the escalation Policy from the XML.
            //     : Decide on the override
        }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
        store = new Store(new LevelChange(), DefaultWaitTime);
    }

    @Override
    public void teardown()
    {
    }

    private void sendMessage( Message message ) {

        List<Action> actions = message.getCurrentActions();

        for ( Action action : actions ) {
            if ( action instanceof EmailAction ) {

                // TODO : fill the tuple
                EmailNotificationTuple emailNotificationTuple = new EmailNotificationTuple() ;
                messageOutput.emit(emailNotificationTuple);
            }
        }
    }
}