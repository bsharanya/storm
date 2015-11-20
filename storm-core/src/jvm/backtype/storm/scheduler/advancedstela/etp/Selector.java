package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Selector {

    public ArrayList<ExecutorSummary> selectPair(Cluster cluster, String targetID, String victimID,
                                                        GlobalState globalState, GlobalStatistics globalStatistics) {

        TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetID);
        TopologySchedule victimSchedule = globalState.getTopologySchedules().get(victimID);
        TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetID);
        TopologyStatistics victimStatistics = globalStatistics.getTopologyStatistics().get(victimID);

        Strategy targetStrategy = new Strategy(targetSchedule, targetStatistics);
        Strategy victimStrategy = new Strategy(victimSchedule, victimStatistics);
        TreeMap<Component, Double> rankTarget = targetStrategy.topologyETPRankDescending();
        TreeMap<Component, Double> rankVictim = victimStrategy.topologyETPRankAscending();

        for (Map.Entry<Component, Double> victimComponent : rankVictim.entrySet()) {
            List<ExecutorSummary> victimExecutorDetails = victimComponent.getKey().getExecutorSummaries();

            for (Map.Entry<Component, Double> targetComponent : rankTarget.entrySet()) {
                List<ExecutorSummary> targetExecutorDetails = targetComponent.getKey().getExecutorSummaries();

                for (ExecutorSummary victimSummary : victimExecutorDetails) {
                    for (ExecutorSummary targetSummary : targetExecutorDetails) {

                        if (victimSummary.get_host().equals(targetSummary.get_host())) {
                            ArrayList<ExecutorSummary> ret = new ArrayList<>();
                            ret.add(victimSummary);
                            ret.add(targetSummary);
                            return ret;
                        }

                    }
                }
            }
        }
        return null;
    }
}
