package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StelaTopologyInformation {
    private static final Logger LOG = LoggerFactory.getLogger(StelaTopologyInformation.class);

    private Map config;

    public int numWorkers = 0;
    public HashMap<String, StelaComponent> allComponents;

    public StelaTopologyInformation(Map conf) {
        config = conf;
        allComponents = new HashMap<String, StelaComponent>();
    }

    public void collect(String topologyId) {
        LOG.info("Getting information for topology {}.", topologyId);
        try {
            NimbusClient nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST),
                    (Integer) config.get(Config.NIMBUS_THRIFT_PORT));

            List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();
            for (TopologySummary topologySummary : topologies) {
                if (topologySummary.get_id().equals(topologyId)) {
                    numWorkers = topologySummary.get_num_workers();

                    StormTopology topology = nimbusClient.getClient().getTopology(topologyId);

                    for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                        if (!spout.getKey().matches("(__).*")) {
                            StelaComponent component;

                            if (allComponents.containsKey(spout.getKey())) {
                                component = allComponents.get(spout.getKey());
                            } else {
                                component = new StelaComponent(spout.getKey());
                                allComponents.put(spout.getKey(), component);
                            }

                            for (Map.Entry<GlobalStreamId, Grouping> entry : spout.getValue().get_common()
                                    .get_inputs().entrySet()) {
                                if (!entry.getKey().get_componentId().matches("(__).*")) {
                                    component.parents.add(entry.getKey().get_componentId());
                                    if (!allComponents.containsKey(entry.getKey().get_componentId())) {
                                        allComponents.put(entry.getKey().get_componentId(),
                                                new StelaComponent(entry.getKey().get_componentId()));
                                    }
                                    allComponents.get(entry.getKey().get_componentId()).children.add(spout.getKey());
                                }
                            }
                        }
                    }

                    for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                        if (!bolt.getKey().matches("(__).*")) {
                            StelaComponent component;

                            if (allComponents.containsKey(bolt.getKey())) {
                                component = allComponents.get(bolt.getKey());
                            } else {
                                component = new StelaComponent(bolt.getKey());
                                allComponents.put(bolt.getKey(), component);
                            }

                            for (Map.Entry<GlobalStreamId, Grouping> entry : bolt.getValue().get_common().get_inputs()
                                    .entrySet()) {
                                if (!entry.getKey().get_componentId().matches("(__).*")) {
                                    component.parents.add(entry.getKey().get_componentId());
                                    if (!allComponents.containsKey(entry.getKey().get_componentId())) {
                                        allComponents.put(entry.getKey().get_componentId(),
                                                new StelaComponent(entry.getKey().get_componentId()));
                                    }
                                    allComponents.get(entry.getKey().get_componentId()).children.add(bolt.getKey());
                                }
                            }
                        }
                    }

                }
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
