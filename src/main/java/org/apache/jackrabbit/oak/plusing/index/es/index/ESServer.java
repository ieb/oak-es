package org.apache.jackrabbit.oak.plusing.index.es.index;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ieb on 20/05/2016.
 * Creates a client connection to a ES server, either embedded or as a remote client.
 */
public class ESServer {

    private static final String PROP_URL = "es-server-url";
    private Client client;

    private Node node;


    public void init(Map<String, Object> properties) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put(cleanSettings(properties)).build();
        if (!properties.containsKey(PROP_URL)) {
            node = NodeBuilder.nodeBuilder().settings(settings).build();
            node.start();
            client = node.client();
        } else {
            TransportClient tclient = TransportClient.builder().settings(settings).build();
            String[] hosts = String.valueOf(properties.get(PROP_URL)).split(",");
            for (String host : hosts) {
                String[] hostport = host.split(":");
                tclient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostport[0]),
                        Integer.parseInt(hostport[1])));
            }
            client = tclient;
        }

    }

    public void destroy() {
        if (client != null) {
            client.close();
            client = null;
        }
        if (node != null) {
            node.close();
            node = null;
        }
    }

    private Map<String, Object> cleanSettings(Map<String, Object> dirty) {
        Map<String, Object> clean = new HashMap<String, Object>();
        for( Map.Entry<String, Object> e : dirty.entrySet()) {
            clean.put(e.getKey(), String.valueOf(e.getValue()));
        }
        return clean;
    }

    public Client getClient() {
        return client;
    }
}
