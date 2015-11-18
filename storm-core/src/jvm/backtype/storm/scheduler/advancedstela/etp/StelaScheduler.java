package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StelaScheduler implements IScheduler{
    private static final Logger LOG = LoggerFactory.getLogger(StelaScheduler.class);
    private static final String SCHEDULER = "StelaScheduler";

    private Map config;
    private StelaNimbusClient client;
    private StelaGlobalState globalState;

    @Override
    public void prepare(Map conf) {
        config = conf;
        if (config != null) {
            client = new StelaNimbusClient(config);
            globalState = new StelaGlobalState(config, SCHEDULER);
        }
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        if (globalState != null) {
            globalState.updateGlobalState(cluster, topologies);
        } else {
            return;
        }

        for (TopologyDetails topologyDetails: topologies.getTopologies()) {
            globalState.logTopologyInformation(topologyDetails);
            String topologyStatus = client.getTopologyStatus(topologyDetails.getId());

            if (globalState.rebalancingState == StelaSignal.ScaleOut && ("REBALANCING").equals(topologyStatus)) {

            }

        }
    }
}
