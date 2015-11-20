package backtype.storm.scheduler.advancedstela.slo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Topology implements Comparable<Topology> {
    private String id;
    private Double userSpecifiedSLO;
    private Queue<Double> measuredSLOs;
    private HashMap<String, Component> spouts;
    private HashMap<String, Component> bolts;

    public Topology(String topologyId, Double slo) {
        id = topologyId;
        userSpecifiedSLO = slo;
        measuredSLOs = new LinkedList<>();
        spouts = new HashMap<>();
        bolts = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public Double getUserSpecifiedSLO() {
        return userSpecifiedSLO;
    }

    public void setMeasuredSLOs(Double value) {
        if (measuredSLOs.size() == 3) {
            measuredSLOs.remove();
        }
        measuredSLOs.add(value);
    }

    public Double getMeasuredSLO() {
        int count = 0;
        double result = 0.0;
        for (Double value : measuredSLOs) {
            result += value;
            count++;
        }
        return (result / count);
    }

    public void addSpout(String id, Component component) {
        spouts.put(id, component);
    }

    public HashMap<String, Component> getSpouts() {
        return spouts;
    }

    public void addBolt(String id, Component component) {
        bolts.put(id, component);
    }

    public HashMap<String, Component> getBolts() {
        return bolts;
    }

    public HashMap<String, Component> getAllComponents() {
        HashMap<String, Component> components = new HashMap<>();
        components.putAll(spouts);
        components.putAll(bolts);
        return components;
    }

    @Override
    public int compareTo(Topology other) {
        return getMeasuredSLO().compareTo(other.getMeasuredSLO());
    }

    public boolean sloViolated() {
        return getMeasuredSLO() < userSpecifiedSLO;
    }
}
