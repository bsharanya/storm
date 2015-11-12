package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StelaSLOObserver {
    private static final Logger LOG = LoggerFactory.getLogger(StelaSLOObserver.class);
    private static final String ALL_TIME = ":all-time";
    private static final String METRICS = "__metrics";
    private static final String SYSTEM = "__system";

    private Map config;
    private StelaTopologies stelaTopologies;
    private NimbusClient nimbusClient;

    public StelaSLOObserver(Map conf) {
        config = conf;
        if (config != null) {
            stelaTopologies = new StelaTopologies(config);
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST),
                        (Integer) config.get(Config.NIMBUS_THRIFT_PORT));
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        HashMap<String, StelaTopology> allTopologies = stelaTopologies.getStelaTopologies();

        for (String topologyId : allTopologies.keySet()) {
            StelaTopology stelaTopology = allTopologies.get(topologyId);
            try {
                TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(topologyId);
                List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();

                for (ExecutorSummary executor: executorSummaries) {
                    String componentId = executor.get_component_id();
                    StelaComponent stelaComponent = stelaTopology.getAllComponents().get(componentId);

                    ExecutorStats stats = executor.get_stats();
                    ExecutorSpecificStats specific = stats.get_specific();

                    Map<String, Map<String, Long>> transferred = stats.get_transferred();

                    if (specific.is_set_spout()) {
                        Map<String, Long> statValues = transferred.get(ALL_TIME);
                        for (String key: statValues.keySet()) {
                            if (!(key.equals(METRICS) || key.equals(SYSTEM))) {
                                stelaComponent.setTotalTransferred(statValues.get(key).intValue());
                                stelaComponent.setCurrentTransferred(statValues.get(key).intValue());
                            }
                        }
                    } else {
                        Map<String, Long> statValues = transferred.get(ALL_TIME);
                        for (String key: statValues.keySet()) {
                            if (!(key.equals(METRICS) || key.equals(SYSTEM))) {
                                stelaComponent.setTotalTransferred(statValues.get(key).intValue());
                                stelaComponent.setCurrentTransferred(statValues.get(key).intValue());
                            }
                        }

                        Map<String, Map<GlobalStreamId, Long>> executed = specific.get_bolt().get_executed();
                        Map<GlobalStreamId, Long> executedStatValues = executed.get(ALL_TIME);
                        for (GlobalStreamId streamId: executedStatValues.keySet()) {
                            stelaComponent.addTotalExecuted(streamId.get_componentId(),
                                    executedStatValues.get(streamId).intValue());
                            stelaComponent.addCurrentExecuted(streamId.get_componentId(),
                                    executedStatValues.get(streamId).intValue());
                        }

                    }
                }
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        LOG.info("********* Stela SLO Observer Begin *********");
        for (StelaTopology stelaTopology: allTopologies.values()) {
            HashMap<String, StelaComponent> allComponents = stelaTopology.getAllComponents();
            for (StelaComponent stelaComponent: allComponents.values()) {
                LOG.info(stelaComponent.toString());
            }
        }
        LOG.info("********* Stela SLO Observer End *********");
    }
}
