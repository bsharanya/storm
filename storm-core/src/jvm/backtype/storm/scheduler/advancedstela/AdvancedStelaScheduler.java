package backtype.storm.scheduler.advancedstela;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.advancedstela.slo.SLOObserver;
import backtype.storm.scheduler.advancedstela.slo.SLORunner;
import backtype.storm.scheduler.advancedstela.slo.TopologyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AdvancedStelaScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler.class);
    private static final Integer OBSERVER_RUN_INTERVAL = 30;

    @SuppressWarnings("rawtypes")
    private Map config;
    private SLOObserver sloObserver;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        config = conf;
        sloObserver = new SLOObserver(conf);

        Integer observerRunDelay = (Integer) config.get(Config.STELA_SLO_OBSERVER_INTERVAL);
        if (observerRunDelay == null) {
            observerRunDelay = OBSERVER_RUN_INTERVAL;
        }
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new SLORunner(sloObserver), 0, observerRunDelay, TimeUnit.SECONDS);
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        TopologyPair topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();
    }
}
