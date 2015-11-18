package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SLOObserver {
    private static final Logger LOG = LoggerFactory.getLogger(SLOObserver.class);
    private static final String ALL_TIME = ":all-time";
    private static final String METRICS = "__metrics";
    private static final String SYSTEM = "__system";

    private Map config;
    private StelaTopologies stelaTopologies;
    private NimbusClient nimbusClient;

    public SLOObserver(Map conf) {
        config = conf;
        stelaTopologies = new StelaTopologies(config);
    }

    public synchronized TopologyPair getTopologiesToBeRescaled() {
        return stelaTopologies.getTopologyPairScaling();
    }

    public synchronized void run() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST),
                        (Integer) config.get(Config.NIMBUS_THRIFT_PORT));
                stelaTopologies.constructTopologyGraphs();
                HashMap<String, StelaTopology> allTopologies = stelaTopologies.getStelaTopologies();

                collectStatistics(allTopologies);
                calculateSloPerSource(allTopologies);
                logFinalSourceSLOsPer(allTopologies);

            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    private void collectStatistics(HashMap<String, StelaTopology> allTopologies) throws TException {
        for (String topologyId : allTopologies.keySet()) {
            StelaTopology stelaTopology = allTopologies.get(topologyId);
            TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(topologyId);
            List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();

            for (ExecutorSummary executor : executorSummaries) {
                String componentId = executor.get_component_id();
                StelaComponent stelaComponent = stelaTopology.getAllComponents().get(componentId);

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
                            stelaComponent.setCurrentTransferred(statValues.get(key).intValue());
                            stelaComponent.setTotalTransferred(statValues.get(key).intValue());
                        }
                    }
                } else {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (!(key.equals(METRICS) || key.equals(SYSTEM))) {
                            stelaComponent.setCurrentTransferred(statValues.get(key).intValue());
                            stelaComponent.setTotalTransferred(statValues.get(key).intValue());
                        }
                    }

                    Map<String, Map<GlobalStreamId, Long>> executed = specific.get_bolt().get_executed();
                    Map<GlobalStreamId, Long> executedStatValues = executed.get(ALL_TIME);
                    for (GlobalStreamId streamId : executedStatValues.keySet()) {
                        stelaComponent.addCurrentExecuted(streamId.get_componentId(),
                                executedStatValues.get(streamId).intValue());
                        stelaComponent.addTotalExecuted(streamId.get_componentId(),
                                executedStatValues.get(streamId).intValue());
                    }
                }
            }
        }
    }

    private void calculateSloPerSource(HashMap<String, StelaTopology> allTopologies) {
        for (String topologyId : allTopologies.keySet()) {
            StelaTopology stelaTopology = allTopologies.get(topologyId);
            HashMap<String, StelaComponent> spouts = stelaTopology.getSpouts();

            HashMap<String, StelaComponent> parents = new HashMap<>();
            for (StelaComponent spout : spouts.values()) {
                HashSet<String> children = spout.getChildren();
                for (String child : children) {
                    StelaComponent stelaComponent = stelaTopology.getAllComponents().get(child);
                    Integer currentTransferred = spout.getCurrentTransferred();
                    Integer executed = stelaComponent.getCurrentExecuted().get(spout.getId());

                    if (executed == null) {
                        continue;
                    }

                    Double value = ((double) executed) / (double) currentTransferred;
                    stelaComponent.addSpoutTransfer(spout.getId(), value);
                    parents.put(child, stelaComponent);
                }
            }

            while (!parents.isEmpty()) {
                HashMap<String, StelaComponent> children = new HashMap<>();
                for (StelaComponent bolt : parents.values()) {
                    HashSet<String> boltChildren = bolt.getChildren();

                    for (String child : boltChildren) {
                        StelaComponent stelaComponent = stelaTopology.getAllComponents().get(child);
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

    private void logFinalSourceSLOsPer(HashMap<String, StelaTopology> allTopologies) {
        LOG.info("*************************************************");

        for (String topologyId : allTopologies.keySet()) {
            Double calculatedSLO = 0.0;
            StelaTopology stelaTopology = allTopologies.get(topologyId);

            LOG.info("Output SLO for topology {} is {}", topologyId, stelaTopology.getUserSpecifiedSLO());

            for (StelaComponent bolt : stelaTopology.getBolts().values()) {
                if (bolt.getChildren().isEmpty()) {
                    for (Double sourceProportion : bolt.getSpoutTransfer().values()) {
                        calculatedSLO += sourceProportion;
                    }
                }
            }

            calculatedSLO = calculatedSLO / stelaTopology.getSpouts().size();
            stelaTopology.setMeasuredSLOs(calculatedSLO);
            LOG.info("Measure SLO for topology {} is {}", topologyId, calculatedSLO);
            LOG.info("*************************************************");
        }

    }
}
