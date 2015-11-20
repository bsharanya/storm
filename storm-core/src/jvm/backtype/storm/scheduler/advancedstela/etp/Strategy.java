package backtype.storm.scheduler.advancedstela.etp;

import java.util.*;

public class Strategy {
    private String id;
    private TopologySchedule topologySchedule;
    private TopologyStatistics topologyStatistics;
    private HashMap<String, Double> componentEmitRates;
    private HashMap<String, Double> componentExecuteRates;
    private TreeMap<String, Double> expectedEmitRates;
    private TreeMap<String, Double> expectedExecutedRates;
    private HashMap<String, Integer> parallelism;
    private ArrayList<Component> sourceList;
    private HashMap<Component, Double> congestionMap;
    private HashMap<Component, Double> topologyETPMap;
    private TreeMap<Component, Double> topologyETPRankDesc;
    private TreeMap<Component, Double> topologyETPRankAsc;

    public Strategy(TopologySchedule tS, TopologyStatistics tStats) {
        id = tS.getId();
        topologySchedule = tS;
        topologyStatistics = tStats;
        componentEmitRates = new HashMap<>();
        componentExecuteRates = new HashMap<>();
        parallelism = new HashMap<String, Integer>();
        congestionMap = new HashMap<>();
        expectedEmitRates = new TreeMap<>();
        expectedExecutedRates = new TreeMap<>();
        sourceList = new ArrayList<Component>();
        topologyETPMap = new HashMap<>();
        topologyETPRankDesc = new TreeMap<>();
        topologyETPRankAsc = new TreeMap<>();
    }

    public TreeMap<Component, Double> topologyETPRankDescending() {
        collectRates();
        congestionDetection();

        //calculate ETP for each component
        for (Component component : topologySchedule.getComponents().values()) {
            Double score = etpCalculation(component);
            topologyETPMap.put(component, score);
        }
        topologyETPRankDesc.putAll(topologyETPMap);
        return topologyETPRankDesc;
    }

    public TreeMap<Component, Double> topologyETPRankAscending() {
        collectRates();
        congestionDetection();

        //calculate ETP for each component
        for (Component component : topologySchedule.getComponents().values()) {
            Double score = etpCalculation(component);
            topologyETPMap.put(component, score);
        }
        topologyETPRankAsc.putAll(topologyETPMap);
        return topologyETPRankAsc;
    }

    private void congestionDetection() {
        HashMap<String, Component> components = topologySchedule.getComponents();
        for (Map.Entry<String, Double> componentRate : componentExecuteRates.entrySet()) {
            Double out = componentRate.getValue();
            Double in = 0.0;

            Component self = components.get(componentRate.getKey());

            if (self.getParents().size() != 0) {
                for (String parent : self.getParents()) {
                    in += componentEmitRates.get(parent);
                }
            }

            if (in > 1.2 * out) {
                Double io = in - out;
                congestionMap.put(self, io);
            }
        }
    }

    private Double etpCalculation(Component component) {
        Double ret = 0.0;
        if (component.getChildren().size() == 0) {
            return componentEmitRates.get(component.getId());
        }

        HashMap<String, Component> components = topologySchedule.getComponents();
        for (String c : component.getChildren()) {
            Component child = components.get(c);
            if (congestionMap.get(child) != null) {
                return 0.0;
            } else {
                ret = ret + etpCalculation(child);
            }
        }

        return ret;
    }

    private void collectRates() {
        for (Map.Entry<String, List<Integer>> emitThroughput : topologyStatistics.getEmitThroughputHistory().entrySet()) {
            componentEmitRates.put(emitThroughput.getKey(), computeMovingAverage(emitThroughput.getValue()));
        }
        expectedEmitRates.putAll(componentEmitRates);

        for (Map.Entry<String, List<Integer>> executeThroughput : topologyStatistics.getExecuteThroughputHistory().entrySet()) {
            componentExecuteRates.put(executeThroughput.getKey(), computeMovingAverage(executeThroughput.getValue()));
        }
        expectedExecutedRates.putAll(componentExecuteRates);


        for (Map.Entry<String, Component> component : topologySchedule.getComponents().entrySet()) {
            parallelism.put(component.getKey(), component.getValue().getParallelism());
        }

        for (Component component : topologySchedule.getComponents().values()) {
            if (component.getParents().size() == 0) {
                sourceList.add(component);
            }
        }
    }

    private Double computeMovingAverage(List<Integer> rates) {
        Double sum = 0.0;
        for (Integer val : rates) {
            sum += val;
        }
        return sum / (rates.size() * 1.0);
    }
}
