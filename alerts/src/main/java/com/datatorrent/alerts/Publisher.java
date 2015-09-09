package com.datatorrent.alerts;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.WebsocketAppDataPusher;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;

public class Publisher implements PublisherInterface
{
  private SharedPubSubWebSocketClient wsClient;
  private WebsocketAppDataPusher appDataPusher;
  private volatile static Publisher instance;

  public static Publisher getInstance()
  {
    if (instance == null) {
      synchronized (Publisher.class) {
        if (instance == null) {
          instance = new Publisher();
          instance.configureAlertPublishing();
        }
      }
    }
    return instance;
  }

  public boolean configureAlertPublishing()
  {
    try {
      // Establish web socket connection
      String gatewayAddress = "node0.morado.com:9292";
      wsClient = new SharedPubSubWebSocketClient("ws://" + gatewayAddress + "/pubsub", 1500);
      wsClient.setLoginUrl("http://" + gatewayAddress + StreamingContainerManager.GATEWAY_LOGIN_URL_PATH);
      wsClient.setUserName("isha");
      wsClient.setPassword("isha");
      wsClient.setup();

      appDataPusher = new WebsocketAppDataPusher(wsClient, "alerts");

    } catch (Exception e) {
      DTThrowable.wrapIfChecked(e);
    }

    return true;
  }

  @Override
  public boolean publishAlert(Message alert)
  {
    ObjectMapper mapper = new ObjectMapper();

    try {
      // Send message to alert gateway API
      String alertMessage = mapper.writeValueAsString(alert);
      JSONObject json = new JSONObject(alertMessage);
      appDataPusher.push(json);
      return true;
    } catch (IOException e) {
      DTThrowable.wrapIfChecked(e);
    } catch (JSONException e) {
      DTThrowable.wrapIfChecked(e);
    }

    return false;
  }

  public void teardown()
  {
    try {
      // Close all the connections
    } catch (Exception e) {
      DTThrowable.wrapIfChecked(e);
    }
  }

}