package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Selector {

    public ArrayList<ExecutorDetails> selectPair(Cluster cluster, String targetID, String victimID,
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
            List<ExecutorDetails> victimExecutorDetails = victimComponent.getKey().getExecutorDetails();

            for (Map.Entry<Component, Double> targetComponent : rankTarget.entrySet()) {
                List<ExecutorDetails> targetExecutorDetails = targetComponent.getKey().getExecutorDetails();

                for (ExecutorDetails victimExecutor : victimExecutorDetails) {
                    for (ExecutorDetails targetExecutor : targetExecutorDetails) {

                        if (executorToNode(cluster, targetID, targetExecutor).equals(executorToNode(cluster, victimID, victimExecutor))) {
                            ArrayList<ExecutorDetails> ret = new ArrayList<ExecutorDetails>();
                            ret.add(victimExecutor);
                            ret.add(targetExecutor);
                            return ret;
                        }

                    }
                }
            }
        }
        return null;
    }

    private String executorToNode(Cluster cluster, String id, ExecutorDetails executorDetails) {
        WorkerSlot w = cluster.getAssignmentById(id).getExecutorToSlot().get(executorDetails);
        return w.getNodeId();
    }
}
