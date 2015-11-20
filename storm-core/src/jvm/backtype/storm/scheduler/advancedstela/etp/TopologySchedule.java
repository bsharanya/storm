package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

import java.util.ArrayList;
import java.util.HashMap;

public class TopologySchedule {
    private String id;
    private Integer numberOFWorkers;
    private HashMap<ExecutorDetails, Component> executorToComponent;
    private HashMap<WorkerSlot, ArrayList<ExecutorDetails>> assignment;
    private HashMap<String, Component> components;

    public TopologySchedule(String identifier, int wookerCount) {
        id = identifier;
        numberOFWorkers = wookerCount;
        components = new HashMap<>();
        executorToComponent = new HashMap<>();
        assignment = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public void setNumberOFWorkers(Integer numberOFWorkers) {
        numberOFWorkers = numberOFWorkers;
    }

    public void addExecutorToComponent(ExecutorDetails details, String componentId) {
        executorToComponent.put(details, components.get(componentId));
    }

    public void addAssignment(WorkerSlot slot, ExecutorDetails details) {
        if (!assignment.containsKey(slot)) {
            assignment.put(slot, new ArrayList<ExecutorDetails>());
        }
        ArrayList<ExecutorDetails> executorDetails = assignment.get(slot);
        executorDetails.add(details);
    }

    public void addComponents(String id, Component component) {
        components.put(id, component);
    }

    public Integer getNumberOFWorkers() {
        return numberOFWorkers;
    }

    public HashMap<ExecutorDetails, Component> getExecutorToComponent() {
        return executorToComponent;
    }

    public HashMap<WorkerSlot, ArrayList<ExecutorDetails>> getAssignment() {
        return assignment;
    }

    public HashMap<String, Component> getComponents() {
        return components;
    }
}
