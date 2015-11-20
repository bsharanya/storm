package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.ExecutorDetails;

import java.util.ArrayList;
import java.util.List;

public class Component {
    private String id;
    private Integer parallelism;
    private List<String> parents;
    private List<String> children;
    private List<ExecutorDetails> executorDetails;

    public Component(String identifier, int parallelismHint) {
        id = identifier;
        parallelism = parallelismHint;
        parents = new ArrayList<String>();
        children = new ArrayList<String>();
        executorDetails = new ArrayList<ExecutorDetails>();
    }

    public String getId() {
        return id;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public List<String> getParents() {
        return parents;
    }

    public List<String> getChildren() {
        return children;
    }

    public List<ExecutorDetails> getExecutorDetails() {
        return executorDetails;
    }

    public void addParent(String parentId) {
        parents.add(parentId);
    }

    public void addChild(String childId) {
        children.add(childId);
    }

    public void addExecutor(ExecutorDetails executor) {
        executorDetails.add(executor);
    }
}
