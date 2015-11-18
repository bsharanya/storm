package backtype.storm.scheduler.advancedstela;

import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.Map;

public class StelaNimbusClient {
    private NimbusClient nimbusClient;

    public StelaNimbusClient(Map _conf) {
        try {
            nimbusClient = new NimbusClient(_conf, (String) _conf.get(Config.NIMBUS_HOST),
                    (Integer) _conf.get(Config.NIMBUS_THRIFT_PORT));
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    NimbusClient getNimbusClient() {
        return nimbusClient;
    }

    String getTopologyStatus(String topologyId) {
        ClusterSummary clusterSummary = null;
        try {
            clusterSummary = nimbusClient.getClient().getClusterInfo();
            List<TopologySummary> topologies = clusterSummary.get_topologies();

            for (TopologySummary topo : topologies) {
                if (topo.get_id().equals(topologyId)) {
                    return topo.get_status();
                }
            }
        } catch (TException e) {
            e.printStackTrace();
        }

        return null;
    }
}

