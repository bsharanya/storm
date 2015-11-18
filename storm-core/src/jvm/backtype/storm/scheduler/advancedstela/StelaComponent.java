package backtype.storm.scheduler.advancedstela;

import backtype.storm.scheduler.ExecutorDetails;

import java.util.ArrayList;
import java.util.List;

public class StelaComponent {
    public String id;
    public List<String> parents = null;
    public List<String> children = null;
    public List<ExecutorDetails> executorDetails = null;

    public StelaComponent(String id) {
        this.parents = new ArrayList<String>();
        this.children = new ArrayList<String>();
        this.executorDetails = new ArrayList<ExecutorDetails>();
        this.id = id;
    }
}

