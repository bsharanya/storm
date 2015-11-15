package backtype.storm.scheduler.advancedstela.slo;

import java.util.HashMap;
import java.util.HashSet;

public class StelaComponent {
    private String id;
    private int parallelism;
    private HashSet<String> parents;
    private HashSet<String> children;
    private Integer totalTransferred;
    private Integer currentTransferred;
    private HashMap<String, Integer> totalExecuted;
    private HashMap<String, Integer> currentExecuted;
    private HashMap<String, Float> spoutTransfer;

    public StelaComponent(String key, int parallelismHint) {
        id = key;
        parallelism = parallelismHint;
        parents = new HashSet<>();
        children = new HashSet<>();
        totalTransferred = 0;
        currentTransferred = 0;
        totalExecuted = new HashMap<>();
        currentExecuted = new HashMap<>();
        spoutTransfer = new HashMap<>();
    }

    public HashSet<String> getParents() {
        return parents;
    }

    public HashSet<String> getChildren() {
        return children;
    }

    public String getId() {
        return id;
    }

    public void addParent(String parentId) {
        parents.add(parentId);
    }

    public void addChild(String childId) {
        children.add(childId);
    }

    public void setTotalTransferred(Integer totalTransferred) {
        this.totalTransferred = totalTransferred;
    }

    public Integer getCurrentTransferred() {
        return currentTransferred;
    }

    public void setCurrentTransferred(Integer currentTransferred) {
        this.currentTransferred = currentTransferred - totalTransferred;
    }

    public HashMap<String, Integer> getCurrentExecuted() {
        return currentExecuted;
    }

    public HashMap<String, Float> getSpoutTransfer() {
        return spoutTransfer;
    }

    public void addSpoutTransfer(String key, Float value) {
        spoutTransfer.put(key, value);
    }

    public void addTotalExecuted(String key, Integer value) {
        totalExecuted.put(key, value);
    }

    public void addCurrentExecuted(String key, Integer value) {
        if (totalExecuted.get(key) == null) {
            currentExecuted.put(key, value);
        } else {
            currentExecuted.put(key, value - totalExecuted.get(key));
        }
    }

    public String printSLOValue() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (String component : spoutTransfer.keySet()) {
            sb.append(component).append(":").append(spoutTransfer.get(component));
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder p = new StringBuilder();
        p.append("[ ");
        for (String parent : parents) {
            p.append(parent).append(" ");
        }
        p.append("]");

        StringBuilder c = new StringBuilder();
        c.append("[ ");
        for (String child : children) {
            c.append(child).append(" ");
        }
        c.append("]");

        StringBuilder t = new StringBuilder();
        t.append("[ ");
        for (String component : totalExecuted.keySet()) {
            t.append(component).append(":").append(totalExecuted.get(component)).append(" ");
        }
        t.append("]");

        StringBuilder cE = new StringBuilder();
        cE.append("[ ");
        for (String component : currentExecuted.keySet()) {
            cE.append(component).append(":").append(currentExecuted.get(component)).append(" ");
        }
        cE.append("]");

        return "StelaComponent{" +
                "id='" + id + '\'' +
                ", parallelism=" + parallelism +
                ", parents=" + p.toString() +
                ", children=" + c.toString() +
                ", totalTransferred=" + totalTransferred +
                ", currentTransferred=" + currentTransferred +
                ", totalExecuted=" + t.toString() +
                ", currentExecuted=" + cE.toString() +
                ", sloValue=" + printSLOValue() +
                '}';
    }
}
