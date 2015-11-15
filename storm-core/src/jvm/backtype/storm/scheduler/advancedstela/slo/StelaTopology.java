package backtype.storm.scheduler.advancedstela.slo;

import java.util.HashMap;

public class StelaTopology {
    private String id;
    private HashMap<String, StelaComponent> spouts;
    private HashMap<String, StelaComponent> bolts;
    private Double userSpecifiedSlo;

    public StelaTopology(String topologyId, Double slo) {
        id = topologyId;
        userSpecifiedSlo = slo;
        spouts = new HashMap<>();
        bolts = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public Double getUserSpecifiedSlo() {
        return userSpecifiedSlo;
    }

    public void addSpout(String id, StelaComponent component) {
        spouts.put(id, component);
    }

    public void addBolt(String id, StelaComponent component) {
        bolts.put(id, component);
    }

    public HashMap<String, StelaComponent> getSpouts() {
        return spouts;
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
