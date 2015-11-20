package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Observer {
    private static final Logger LOG = LoggerFactory.getLogger(Observer.class);
    private static final String ALL_TIME = ":all-time";
    private static final String METRICS = "__metrics";
    private static final String SYSTEM = "__system";

    private Map config;
    private Topologies topologies;
    private NimbusClient nimbusClient;

    public Observer(Map conf) {
        config = conf;
        topologies = new Topologies(config);
    }

    public synchronized TopologyPairs getTopologiesToBeRescaled() {
        return topologies.getTopologyPairScaling();
    }

    public synchronized void run() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST),
                        (Integer) config.get(Config.NIMBUS_THRIFT_PORT));
                topologies.constructTopologyGraphs();
                HashMap<String, Topology> allTopologies = topologies.getStelaTopologies();

                collectStatistics(allTopologies);
                calculateSloPerSource(allTopologies);
                logFinalSourceSLOsPer(allTopologies);

            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    private void collectStatistics(HashMap<String, Topology> allTopologies) throws TException {
        for (String topologyId : allTopologies.keySet()) {
            Topology topology = allTopologies.get(topologyId);
            TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(topologyId);
            List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();

            for (ExecutorSummary executor : executorSummaries) {
                String componentId = executor.get_component_id();
                Component component = topology.getAllComponents().get(componentId);

                ExecutorStats stats = executor.get_stats();
                if (stats == null) {
                    continue;
                }

                ExecutorSpecificStats specific = stats.get_specific();

                Map<String, Map<String, Long>> transferred = stats.get_transferred();

                if (specific.is_set_spout()) {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (!(key.equals(METRICS) || key.equals(SYSTEM))) {
                            component.setCurrentTransferred(statValues.get(key).intValue());
                            component.setTotalTransferred(statValues.get(key).intValue());
                        }
                    }
                } else {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (!(key.equals(METRICS) || key.equals(SYSTEM))) {
                            component.setCurrentTransferred(statValues.get(key).intValue());
                            component.setTotalTransferred(statValues.get(key).intValue());
                        }
                    }

                    Map<String, Map<GlobalStreamId, Long>> executed = specific.get_bolt().get_executed();
                    Map<GlobalStreamId, Long> executedStatValues = executed.get(ALL_TIME);
                    for (GlobalStreamId streamId : executedStatValues.keySet()) {
                        component.addCurrentExecuted(streamId.get_componentId(),
                                executedStatValues.get(streamId).intValue());
                        component.addTotalExecuted(streamId.get_componentId(),
                                executedStatValues.get(streamId).intValue());
                    }
                }
            }
        }
    }

    private void calculateSloPerSource(HashMap<String, Topology> allTopologies) {
        for (String topologyId : allTopologies.keySet()) {
            Topology topology = allTopologies.get(topologyId);
            HashMap<String, Component> spouts = topology.getSpouts();

            HashMap<String, Component> parents = new HashMap<>();
            for (Component spout : spouts.values()) {
                HashSet<String> children = spout.getChildren();
                for (String child : children) {
                    Component component = topology.getAllComponents().get(child);
                    Integer currentTransferred = spout.getCurrentTransferred();
                    Integer executed = component.getCurrentExecuted().get(spout.getId());

                    if (executed == null) {
                        continue;
                    }

                    Double value = ((double) executed) / (double) currentTransferred;
                    component.addSpoutTransfer(spout.getId(), value);
                    parents.put(child, component);
                }
            }

            while (!parents.isEmpty()) {
                HashMap<String, Component> children = new HashMap<>();
                for (Component bolt : parents.values()) {
                    HashSet<String> boltChildren = bolt.getChildren();

                    for (String child : boltChildren) {
                        Component stelaComponent = topology.getAllComponents().get(child);
                        Integer currentTransferred = bolt.getCurrentTransferred();
                        Integer executed = stelaComponent.getCurrentExecuted().get(bolt.getId());

                        if (executed == null) {
                            continue;
                        }

                        Double value = ((double) executed) / (double) currentTransferred;
                        for (String component : bolt.getSpoutTransfer().keySet()) {
                            stelaComponent.addSpoutTransfer(component,
                                    value * bolt.getSpoutTransfer().get(component));
                        }
                        children.put(stelaComponent.getId(), stelaComponent);
                    }
                }

                parents = children;
            }
        }
    }

    private void logFinalSourceSLOsPer(HashMap<String, Topology> allTopologies) {
        LOG.info("*************************************************");

        for (String topologyId : allTopologies.keySet()) {
            Double calculatedSLO = 0.0;
            Topology topology = allTopologies.get(topologyId);

            LOG.info("Output SLO for topology {} is {}", topologyId, topology.getUserSpecifiedSLO());

            for (Component bolt : topology.getBolts().values()) {
                if (bolt.getChildren().isEmpty()) {
                    for (Double sourceProportion : bolt.getSpoutTransfer().values()) {
                        calculatedSLO += sourceProportion;
                    }
                }
            }

            calculatedSLO = calculatedSLO / topology.getSpouts().size();
            topology.setMeasuredSLOs(calculatedSLO);
            LOG.info("Measure SLO for topology {} is {}", topologyId, calculatedSLO);
            LOG.info("*************************************************");
        }

    }
}
