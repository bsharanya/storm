package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StelaTopologies {
    private static final Logger LOG = LoggerFactory.getLogger(StelaTopologies.class);

    private Map config;
    private NimbusClient nimbusClient;
    private HashMap<String, StelaTopology> stelaTopologies;

    public StelaTopologies(Map conf) {
        config = conf;
        stelaTopologies = new HashMap<>();
    }

    public HashMap<String, StelaTopology> getStelaTopologies() {
        return stelaTopologies;
    }

    public void constructTopologyGraphs() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST),
                        (Integer) config.get(Config.NIMBUS_THRIFT_PORT));

                List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();

                for (TopologySummary topologySummary : topologies) {
                    String id = topologySummary.get_id();

                    if (!stelaTopologies.containsKey(id)) {
                        Double userSpecifiedSlo = getUserSpecifiedSLOFromConfig(id);

                        StelaTopology stelaTopology = new StelaTopology(id, userSpecifiedSlo);
                        StormTopology stormTopology = nimbusClient.getClient().getTopology(id);

                        addSpoutsAndBolts(stormTopology, stelaTopology);
                        constructTopologyGraph(stormTopology, stelaTopology);

                        stelaTopologies.put(id, stelaTopology);
                    }
                }
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    private Double getUserSpecifiedSLOFromConfig(String id) throws TException {
        Double topologySLO = 1.0;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));
            topologySLO = (Double) conf.get(Config.TOPOLOGY_SLO);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return topologySLO;
    }

    private void addSpoutsAndBolts(StormTopology stormTopology, StelaTopology stelaTopology) throws TException {
        for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
            if (!spout.getKey().matches("(__).*")) {
                stelaTopology.addSpout(spout.getKey(), new StelaComponent(spout.getKey(),
                        spout.getValue().get_common().get_parallelism_hint()));
            }
        }

        for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                stelaTopology.addBolt(bolt.getKey(), new StelaComponent(bolt.getKey(),
                        bolt.getValue().get_common().get_parallelism_hint()));
            }
        }
    }

    private void constructTopologyGraph(StormTopology topology, StelaTopology stelaTopology) {
        for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                StelaComponent stelaComponent = stelaTopology.getBolts().get(bolt.getKey());

                for (Map.Entry<GlobalStreamId, Grouping> parent : bolt.getValue().get_common().get_inputs().entrySet())
                {
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
}
