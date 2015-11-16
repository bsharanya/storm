package backtype.storm.scheduler.advancedstela.slo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class StelaTopology {
    private String id;
    private Double userSpecifiedSLO;
    private Queue<Double> measuredSLOs;
    private HashMap<String, StelaComponent> spouts;
    private HashMap<String, StelaComponent> bolts;

    public StelaTopology(String topologyId, Double slo) {
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

    public Double getMeasuredSLOs() {
        int count = 0;
        double result = 0.0;
        for (Double value : measuredSLOs) {
            result += value;
            count++;
        }
        return (result / count);
    }

    public void addSpout(String id, StelaComponent component) {
        spouts.put(id, component);
    }

    public HashMap<String, StelaComponent> getSpouts() {
        return spouts;
    }

    public void addBolt(String id, StelaComponent component) {
        bolts.put(id, component);
    }

    public HashMap<String, StelaComponent> getBolts() {
        return bolts;
    }

    public HashMap<String, StelaComponent> getAllComponents() {
        HashMap<String, StelaComponent> components = new HashMap<>();
        components.putAll(spouts);
        components.putAll(bolts);
        return components;
    }
}
