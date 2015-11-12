package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StelaTopologies {
//    private static final Logger LOG = LoggerFactory.getLogger(StelaTopologies.class);

    private NimbusClient nimbusClient;
    private HashMap<String, StelaTopology> stelaTopologies;

    public StelaTopologies(Map conf) {
        if (conf != null) {
            try {
                nimbusClient = new NimbusClient(conf, (String) conf.get(Config.NIMBUS_HOST),
                        (Integer) conf.get(Config.NIMBUS_THRIFT_PORT));

                constructTopologyGraphs();
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }

    public HashMap<String, StelaTopology> getStelaTopologies() {
        return stelaTopologies;
    }

    public void constructTopologyGraphs() {
        stelaTopologies = new HashMap<>();

        try {
            List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();

            for(TopologySummary topologySummary: topologies) {
                String id = topologySummary.get_id();
                StelaTopology stelaTopology = new StelaTopology(id);
                StormTopology stormTopology = nimbusClient.getClient().getTopology(id);

                addSpoutsAndBolts(stormTopology, stelaTopology);
                constructTopologyGraph(stormTopology, stelaTopology);

                stelaTopologies.put(id, stelaTopology);
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void addSpoutsAndBolts(StormTopology stormTopology, StelaTopology stelaTopology) throws TException {
        for (Map.Entry<String, SpoutSpec> spout: stormTopology.get_spouts().entrySet()) {
            if (!spout.getKey().matches("(__).*")) {
                stelaTopology.addSpout(spout.getKey(), new StelaComponent(spout.getKey(),
                        spout.getValue().get_common().get_parallelism_hint()));
            }
        }

        for (Map.Entry<String, Bolt> bolt: stormTopology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                stelaTopology.addBolt(bolt.getKey(), new StelaComponent(bolt.getKey(),
                        bolt.getValue().get_common().get_parallelism_hint()));
            }
        }
    }

    private void constructTopologyGraph(StormTopology topology, StelaTopology stelaTopology) {
        for (Map.Entry<String, Bolt> bolt: topology.get_bolts().entrySet()) {
            StelaComponent stelaComponent = stelaTopology.getBolts().get(bolt.getKey());

            for (Map.Entry<GlobalStreamId, Grouping> parent : bolt.getValue().get_common().get_inputs().entrySet()) {
                String parentId = parent.getKey().get_componentId();

                if (stelaTopology.getBolts().get(parentId) == null) {
                    stelaTopology.getSpouts().get(parentId).addChild(stelaComponent.getId());
                } else {
                    stelaTopology.getBolts().get(parentId).addChild(stelaComponent.getId());
                }

                stelaComponent.addParent(parentId);
            }
        }
    }
}
