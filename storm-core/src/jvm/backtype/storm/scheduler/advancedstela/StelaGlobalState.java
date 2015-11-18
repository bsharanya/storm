package backtype.storm.scheduler.advancedstela;

import backtype.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StelaGlobalState {
    private static final Logger LOG = LoggerFactory.getLogger(StelaGlobalState.class);

    private Map config;
    private Map<String, Map<WorkerSlot, List<ExecutorDetails>>> scheduleState;
    private Map<String, StelaNode> nodes;
    private Map<String, Map<String, StelaComponent>> components;
    private HashMap<String, Integer> workersForTopology;
    private Map<String, Boolean> scheduleLog = new HashMap<String, Boolean>();
    public StelaSignal rebalancingState;

    private File schedulingLog;
    private File nodesToBeRemoved;

    public StelaGlobalState(Map conf, String filename) {
        config = conf;
        scheduleState = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
        schedulingLog = new File(StelaConfig.LOG_PATH + filename + "_SchedulingInfo");
        nodesToBeRemoved = new File(StelaConfig.LOG_PATH + filename + "_Nodelist");

        try {
            schedulingLog.delete();
            nodesToBeRemoved.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void updateGlobalState(Cluster cluster, Topologies topologies) {
        nodes = getNodes(cluster);
        components = getComponents(topologies);
    }

    void logTopologyInformation(TopologyDetails topologyDetails) {
        if (components.size() > 0) {
            File file = schedulingLog;
            if (!scheduleLog.containsKey(topologyDetails.getId())) {
                scheduleLog.put(topologyDetails.getId(), false);
            }

            if (!scheduleLog.get(topologyDetails.getId())) {
                String data = "\n\n<!---Topology Info---!>\n";
                data += componentsToString();

                StelaHelper.writeToFile(file, data);
                scheduleLog.put(topologyDetails.getId(), true);
            }
        }
    }

    private Map<String, StelaNode> getNodes(Cluster cluster) {
        Map<String, StelaNode> nodeDetails = new HashMap<>();
        for (Map.Entry<String, SupervisorDetails> entry : cluster.getSupervisors().entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorDetails supervisorDetails = cluster.getSupervisors().get(supervisorId);
            StelaNode stelaNode = new StelaNode(supervisorDetails, cluster.getAssignableSlots(supervisorDetails));
            nodeDetails.put(supervisorId, stelaNode);
        }

        for (Map.Entry<String, SchedulerAssignment> entry : cluster.getAssignments().entrySet()) {
            for (Map.Entry<ExecutorDetails, WorkerSlot> executor : entry.getValue().getExecutorToSlot().entrySet()) {
                String nodeId = executor.getValue().getNodeId();
                if (nodeDetails.containsKey(nodeId)) {
                    StelaNode stelaNode = nodeDetails.get(nodeId);
                    if (stelaNode.slotsToExecutors.containsKey(executor.getValue())) {
                        stelaNode.slotsToExecutors.get(executor.getValue()).add(executor.getKey());
                        stelaNode.executors.add(executor.getKey());
                    } else {
                        LOG.error("ERROR: should have node {} should have worker: {}", executor.getValue().getNodeId(),
                                executor.getValue());
                    }
                } else {
                    LOG.error("ERROR: should have node {}", executor.getValue().getNodeId());
                }
            }
        }

        return nodeDetails;
    }

    private Map<String, Map<String, StelaComponent>> getComponents(Topologies topologies) {
        workersForTopology = new HashMap<String, Integer>();
        Map<String, Map<String, StelaComponent>> componentDetails = new HashMap<>();
        StelaTopologyInformation topologyInformation = new StelaTopologyInformation(config);

        for (TopologyDetails topology : topologies.getTopologies()) {
            topologyInformation.collect(topology.getId());
            for (StelaComponent component : topologyInformation.allComponents.values()) {
                component.executorDetails = mapComponentsToExecutors(topology, component.id);
            }
            componentDetails.put(topology.getId(), topologyInformation.allComponents);
            workersForTopology.put(topology.getId(), topologyInformation.numWorkers);
        }

        return componentDetails;
    }

    private List<ExecutorDetails> mapComponentsToExecutors(TopologyDetails topology, String component) {
        List<ExecutorDetails> executorDetails = new ArrayList<ExecutorDetails>();
        for (Map.Entry<ExecutorDetails, String> entry : topology.getExecutorToComponent().entrySet()) {
            if (entry.getValue().equals(component)) {
                executorDetails.add(entry.getKey());
            }
        }

        return executorDetails;
    }

    private String componentsToString() {
        String data = "";
        data += "\n!--Components--!\n";
        for (Map.Entry<String, Map<String, StelaComponent>> entry : components.entrySet()) {
            data += "->Topology: " + entry.getKey() + "\n";
            for (Map.Entry<String, StelaComponent> component : entry.getValue().entrySet()) {
                data += "-->Component: " + component.getValue().id + "==" + entry.getKey() + "\n";
                data += "--->Parents: " + component.getValue().parents + "\n";
                data += "--->Children: " + component.getValue().children + "\n";
                data += "--->execs: " + component.getValue().executorDetails + "\n";
            }
        }
        return data;
    }
}
