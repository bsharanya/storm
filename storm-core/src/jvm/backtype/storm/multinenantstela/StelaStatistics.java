package backtype.storm.multinenantstela;

import backtype.storm.generated.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class StelaStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(StelaStatistics.class);
    private static final String LOG_PATH = "/tmp/";
    private static final String SCHEDULER_TYPE = "MultiTenantScheduler";
    private static final Integer MOVING_AVG_WINDOW = 30;

    public class NodeStats {
        public String hostname;
        public ArrayList<ExecutorSummary> bolts_on_node;
        public ArrayList<ExecutorSummary> spouts_on_node;
        public Integer emit_throughput;
        public Integer transfer_throughput;
        public HashMap<String, Integer> bolts_on_node_throughput;
        public HashMap<String, Integer> spouts_on_node_throughput;

        public NodeStats(String hostname) {
            this.hostname = hostname;
            this.bolts_on_node = new ArrayList<ExecutorSummary>();
            this.spouts_on_node = new ArrayList<ExecutorSummary>();
            this.bolts_on_node_throughput = new HashMap<String, Integer>();
            this.bolts_on_node_throughput.put("transfer", 0);
            this.bolts_on_node_throughput.put("emit", 0);
            this.spouts_on_node_throughput = new HashMap<String, Integer>();
            this.spouts_on_node_throughput.put("transfer", 0);
            this.spouts_on_node_throughput.put("emit", 0);
            this.emit_throughput = 0;
            this.transfer_throughput = 0;
        }
    }

    public class ComponentStats {
        public String componentId;
        public Integer total_emit_throughput;
        public Integer total_transfer_throughput;
        public Integer total_execute_throughput;
        public Integer parallelism_hint;

        public ComponentStats(String id) {
            this.componentId = id;
            this.total_emit_throughput = 0;
            this.total_transfer_throughput = 0;
            this.total_execute_throughput = 0;
        }
    }

    public HashMap<String, Integer> transferStatsTable;

    public HashMap<String, Integer> emitStatsTable;

    public HashMap<String, Integer> executeStatsTable;

    public HashMap<String, Long> startTimes;

    public HashMap<String, NodeStats> nodeStats;

    public HashMap<String, HashMap<String, ComponentStats>> componentStats;

    public HashMap<String, HashMap<String, List<Integer>>> transferThroughputHistory;

    public HashMap<String, HashMap<String, List<Integer>>> emitThroughputHistory;

    public HashMap<String, HashMap<String, List<Integer>>> executeThroughputHistory;

    public HashMap<String, Integer> executeRatesTable;

    public HashMap<String, Integer> emitRatesTable;

    /* Files to log output values to collect data and plot graphs*/
    private File stelaStatsCompleteLog;
    private File sinksLog;
    private File componentsLog;

    private String host;
    private Integer port;

    public StelaStatistics(String host, int port) {
        this.host = host;
        this.port = port;
        this.transferStatsTable = new HashMap<String, Integer>();
        this.emitStatsTable = new HashMap<String, Integer>();
        this.executeStatsTable = new HashMap<String, Integer>();
        this.executeRatesTable = new HashMap<String, Integer>();
        this.emitRatesTable = new HashMap<String, Integer>();
        this.emitThroughputHistory = new HashMap<String, HashMap<String, List<Integer>>>();
        this.transferThroughputHistory = new HashMap<String, HashMap<String, List<Integer>>>();
        this.executeThroughputHistory = new HashMap<String, HashMap<String, List<Integer>>>();
        this.startTimes = new HashMap<String, Long>();
        this.nodeStats = new HashMap<String, NodeStats>();
        this.componentStats = new HashMap<String, HashMap<String, ComponentStats>>();

        /* Delete all logs */
        stelaStatsCompleteLog = new File(LOG_PATH + SCHEDULER_TYPE + "_complete");
        sinksLog = new File(LOG_PATH + SCHEDULER_TYPE + "_sinks");
        componentsLog = new File(LOG_PATH + SCHEDULER_TYPE + "_components");

        stelaStatsCompleteLog.delete();
        sinksLog.delete();
        componentsLog.delete();
    }

    public void collect() {
        LOG.info("Getting stats...");

        // reseting values
        this.nodeStats.clear();
        this.componentStats.clear();

        TSocket tsocket = new TSocket(host, port);
        TFramedTransport tTransport = new TFramedTransport(tsocket);
        TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
        Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);

        try {
            tTransport.open();
            ClusterSummary clusterSummary = client.getClusterInfo();
            List<TopologySummary> topologies = clusterSummary.get_topologies();

            for (TopologySummary topology : topologies) {
                try {
                    String topologyId = topology.get_id();
                    LOG.info(" *** Stela Statistics *** ");
                    LOG.info("Topology ID: " + topologyId);
                    TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
                    StormTopology stormTopology = client.getTopology(topologyId);

                    List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();
                    for (ExecutorSummary executorSummary : executorSummaries) {
                        String host = executorSummary.get_host();
                        String port = String.valueOf(executorSummary.get_port());
                        String componentId = executorSummary.get_component_id();
                        String taskId = Integer.toString(executorSummary
                                .get_executor_info().get_task_start());

                        initDataStructs(componentId, host, executorSummary, stormTopology, topology);

                        ExecutorStats executorStats = executorSummary.get_stats();
                        if (executorStats == null) {
                            continue;
                        }

                        ExecutorSpecificStats execSpecStats = executorStats.get_specific();
                        BoltStats boltStats = null;

                        if (execSpecStats.is_set_bolt()) {
                            boltStats = execSpecStats.get_bolt();
                        }

                        Map<String, Map<String, Long>> transfer = executorStats.get_transferred();
                        Map<String, Map<String, Long>> emit = executorStats.get_emitted();

                        for (String s : emit.keySet()) {
                            LOG.info("Emit Stats: " + s + ": " + emit.get(s));
                            Map<String, Long> stringLongMap = emit.get(s);
                            for (String s1 : stringLongMap.keySet()) {
                                LOG.info("   Emit Stats: " + s1 + ": " + stringLongMap.get(s1));
                            }
                        }
                        for (String s : transfer.keySet()) {
                            LOG.info("Transfer Stats: " + s + ": " + transfer.get(s));
                            Map<String, Long> stringLongMap = transfer.get(s);
                            for (String s1 : stringLongMap.keySet()) {
                                LOG.info("   Transfer Stats: " + s1 + ": " + stringLongMap.get(s1));
                            }
                        }

                        if (transfer.get(":all-time").get("default") != null
                                && emit.get(":all-time").get("default") != null) {

                            String taskHashId = getTaskHashId(host, port, componentId, topology, taskId);
                            Integer totalTransferOutput = transfer.get(":all-time").get("default").intValue();
                            Integer totalEmitOutput = emit.get(":all-time").get("default").intValue();
                            Integer totalExecuted = 0;

                            LOG.info("Total transfer output: " + totalTransferOutput);
                            LOG.info("Total emit output: " + totalEmitOutput);

                            if (boltStats != null) {
                                totalExecuted = getBoltStatLongValueFromMap(boltStats.get_executed(), ":all-time").intValue();
                                LOG.info("Total executed: " + totalExecuted);

                                if (execSpecStats.is_set_bolt()) {

                                    // Integer
                                    // executed_count=boltStats.get_executed();

                                    if (!this.executeStatsTable.containsKey(taskHashId)) {
                                        this.executeStatsTable.put(taskHashId, totalExecuted);
                                    }

                                    if (!this.executeRatesTable.containsKey(taskHashId)) {
                                        this.executeRatesTable.put(taskHashId, 0);
                                    }

                                    // LOG.info("Executor {}: GLOBAL STREAM ID: {}",taskId,
                                    // boltStats.get_executed());
                                }
                            }

                            if (!this.transferStatsTable.containsKey(taskHashId)) {
                                this.transferStatsTable.put(taskHashId, totalTransferOutput);
                            }
                            if (!this.emitStatsTable.containsKey(taskHashId)) {
                                this.emitStatsTable.put(taskHashId, totalEmitOutput);
                                this.emitRatesTable.put(taskHashId, 0);
                            }

                            // get throughput
                            Integer transfer_throughput = totalTransferOutput - this.transferStatsTable.get(taskHashId);
                            if (transfer_throughput < 0) {
                                transfer_throughput = 0;
                            }
                            Integer emit_throughput = totalEmitOutput - this.emitStatsTable.get(taskHashId);
                            if (emit_throughput < 0) {
                                emit_throughput = 0;
                            }

                            this.emitRatesTable.put(taskHashId, emit_throughput);
                            Integer execute_throughput = 0;
                            if (this.executeStatsTable.containsKey(taskHashId)) {
                                execute_throughput = totalExecuted - this.executeStatsTable.get(taskHashId);
                                this.executeRatesTable.put(taskHashId, execute_throughput);
                            }

                            LOG.info((host + ':' + port + ':' + componentId + ":" + topologyId + ":" + taskId + ","
                                    + transfer.get(":all-time").get("default") + "," + this.transferStatsTable.get(taskHashId)
                                    + "," + transfer_throughput + "," + emit.get(":all-time").get("default") + ","
                                    + this.emitStatsTable.get(taskHashId) + "," + emit_throughput + "," + totalExecuted + ","
                                    + this.executeStatsTable.get(taskHashId) + "," + execute_throughput));

                            // LOG.info("-->transfered: {}\n -->emmitted: {}",
                            // executorStats.get_transferred(),
                            // executorStats.get_emitted());

                            this.transferStatsTable.put(taskHashId, totalTransferOutput);
                            this.emitStatsTable.put(taskHashId, totalEmitOutput);
                            this.executeStatsTable.put(taskHashId, totalExecuted);

                            // get node stats
                            this.nodeStats.get(host).transfer_throughput += transfer_throughput;
                            this.nodeStats.get(host).emit_throughput += emit_throughput;

                            // get node component stats
                            if (stormTopology.get_bolts().containsKey(componentId)) {
                                this.nodeStats.get(host).bolts_on_node_throughput.put("transfer",
                                        this.nodeStats.get(host).bolts_on_node_throughput.get("transfer") + transfer_throughput);
                                this.nodeStats.get(host).bolts_on_node_throughput.put("emit",
                                        this.nodeStats.get(host).bolts_on_node_throughput.get("emit") + emit_throughput);
                            } else if (stormTopology.get_spouts().containsKey(componentId)) {
                                this.nodeStats.get(host).spouts_on_node_throughput.put("transfer",
                                        this.nodeStats.get(host).spouts_on_node_throughput.get("transfer") + transfer_throughput);
                                this.nodeStats.get(host).spouts_on_node_throughput.put("emit",
                                        this.nodeStats.get(host).spouts_on_node_throughput.get("emit") + emit_throughput);
                            }

                            this.componentStats.get(topologyId).get(componentId).total_transfer_throughput += transfer_throughput;
                            this.componentStats.get(topologyId).get(componentId).total_emit_throughput += emit_throughput;
                            this.componentStats.get(topologyId).get(componentId).total_execute_throughput += execute_throughput;

                            long unixTime = (System.currentTimeMillis() / 1000) - this.startTimes.get(topologyId);
                            String data = String.valueOf(unixTime) + ':' + SCHEDULER_TYPE + ":" + host + ':' + port
                                    + ':' + componentId + ":" + topologyId + ":" + taskId + ":" + transfer_throughput
                                    + "," + emit_throughput + "," + execute_throughput + "\n";
                            StelaHelpers.writeToFile(stelaStatsCompleteLog, data);
                        }

                        if (executorSummaries.size() > 0) {
                            updateThroughputHistory(topology);
//                            logOverallStats();
//                            logNodeStats();
//                            logComponentStats(topology);
                        }
                    }
                } catch (Exception e) {
                    LOG.error(Arrays.toString(e.getStackTrace()));
                }

                LOG.info(" *** Stela Statistics *** ");
            }

        } catch (TException e) {
            e.printStackTrace();
        } finally {
            tTransport.close();
        }
    }

    private void initDataStructs(String componentId, String host, ExecutorSummary executorSummary,
                                 StormTopology stormTopology, TopologySummary topology) {

        String topologyId = topology.get_id();

        if (this.transferThroughputHistory.containsKey(topologyId)) {
            this.transferThroughputHistory.put(topologyId, new HashMap<String, List<Integer>>());
        }

        if (!this.emitThroughputHistory.containsKey(topologyId)) {
            this.emitThroughputHistory.put(topologyId, new HashMap<String, List<Integer>>());
        }

        if (!this.executeThroughputHistory.containsKey(topologyId)) {
            this.executeThroughputHistory.put(topologyId, new HashMap<String, List<Integer>>());
        }

        if (!this.componentStats.containsKey(topologyId)) {
            this.componentStats.put(topologyId, new HashMap<String, ComponentStats>());

        }
        if (!componentId.matches("(__).*")) {
            if (!this.nodeStats.containsKey(host)) {
                this.nodeStats.put(host, new NodeStats(host));
            }

            if (!this.componentStats.get(topologyId).containsKey(componentId)) {
                this.componentStats.get(topologyId).put(
                        componentId,
                        new ComponentStats(componentId));
            }
            if (!this.transferThroughputHistory.get(topologyId).containsKey(componentId)) {
                this.transferThroughputHistory.get(topologyId).put(componentId, new ArrayList<Integer>());
            }
            if (!this.emitThroughputHistory.get(topologyId).containsKey(componentId)) {
                this.emitThroughputHistory.get(topologyId).put(componentId, new ArrayList<Integer>());
            }
            if (!this.executeThroughputHistory.get(topologyId).containsKey(componentId)) {
                this.executeThroughputHistory.get(topologyId).put(componentId, new ArrayList<Integer>());
            }

            if (stormTopology.get_bolts().containsKey(componentId)) {
                this.nodeStats.get(host).bolts_on_node.add(executorSummary);
                this.componentStats.get(topologyId).get(componentId).parallelism_hint =
                        stormTopology.get_bolts().get(componentId).get_common().get_parallelism_hint();
            } else if (stormTopology.get_spouts().containsKey(componentId)) {
                this.nodeStats.get(host).spouts_on_node.add(executorSummary);
                this.componentStats.get(topologyId).get(componentId).parallelism_hint =
                        stormTopology.get_spouts().get(componentId).get_common().get_parallelism_hint();
            } else {
                LOG.error("ERROR: " + componentId + " type of component cannot be determined");
            }
        }
    }

    private String getTaskHashId(String host, String port, String componentId, TopologySummary topology, String taskId) {
        return host + ':' + port + ':' + componentId + ":" + topology.get_id() + ":" + taskId;
    }

    private Long getBoltStatLongValueFromMap(Map<String, Map<GlobalStreamId, Long>> map, String statName) {
        Long statValue = (long) 0;
        Map<GlobalStreamId, Long> intermediateMap = map.get(statName);

        for (Long val : intermediateMap.values()) {
            statValue += val;
        }
        return statValue;
    }

    private void updateThroughputHistory(TopologySummary topology) {

        String topologyId = topology.get_id();
        HashMap<String, List<Integer>> compTransferHistory = this.transferThroughputHistory.get(topologyId);
        HashMap<String, List<Integer>> compEmitHistory = this.emitThroughputHistory.get(topologyId);
        HashMap<String, List<Integer>> compExecuteHistory = this.executeThroughputHistory.get(topologyId);

        //LOG.info("compTransferHistory: {}", compTransferHistory);
        //LOG.info("componentStats: {}", this.componentStats);

        for (Map.Entry<String, ComponentStats> entry : this.componentStats.get(topologyId).entrySet()) {
            if (compTransferHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
                compTransferHistory.get(entry.getKey()).remove(0);
            }
            if (compEmitHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
                compEmitHistory.get(entry.getKey()).remove(0);
            }
            if (compExecuteHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
                compExecuteHistory.get(entry.getKey()).remove(0);
            }

            compTransferHistory.get(entry.getKey()).add(entry.getValue().total_transfer_throughput);
            compEmitHistory.get(entry.getKey()).add(entry.getValue().total_emit_throughput);
            compExecuteHistory.get(entry.getKey()).add(entry.getValue().total_execute_throughput);
        }
    }

    private void logOverallStats() {
        LOG.info("**** STATISTICS OVERVIEW ****");
        LOG.info("OVERALL THROUGHPUT:");
        for (Map.Entry<String, NodeStats> ns : this.nodeStats.entrySet()) {
            LOG.info("{} -> transfer: {}    emit: {}", ns.getKey(), ns.getValue().transfer_throughput, ns.getValue().emit_throughput);
        }
    }

    private void logNodeStats() {
        LOG.info("NODE STATS:");
        for (Map.Entry<String, NodeStats> ns : this.nodeStats.entrySet()) {
            LOG.info("{}:", ns.getKey());
            LOG.info("# of Spouts: {}    # of Bolts: {}", ns.getValue().spouts_on_node.size(), ns.getValue().bolts_on_node.size());
            LOG.info("total spout throughput (transfer):{} (emit):{}", ns.getValue().spouts_on_node_throughput.get("transfer"),
                    ns.getValue().spouts_on_node_throughput.get("emit"));
            LOG.info("total bolt throughput (transfer):{} (emit):{}", ns.getValue().bolts_on_node_throughput.get("transfer"),
                    ns.getValue().bolts_on_node_throughput.get("emit"));
        }
    }

    private void logComponentStats(TopologySummary topology) {
        LOG.info("COMPONENT STATS:");
        int num_output_bolt = 0, total_output_bolt_emit = 0;
        String topologyId = topology.get_id(), output_bolts = "";
        long unixTime = (System.currentTimeMillis() / 1000) - this.startTimes.get(topologyId);

        for (Map.Entry<String, ComponentStats> cs : this.componentStats.get(topologyId).entrySet()) {
            int avg_transfer_throughput = cs.getValue().total_transfer_throughput / cs.getValue().parallelism_hint;
            int avg_emit_throughput = cs.getValue().total_emit_throughput / cs.getValue().parallelism_hint;

            String data = "";
            if (cs.getKey().matches(".*_output_.*")) {
                LOG.info("Component: {}(output) total throughput (transfer): {} (emit): {} avg throughput (transfer): {} (emit): {}",
                        cs.getKey(), cs.getValue().total_transfer_throughput, cs.getValue().total_emit_throughput,
                        avg_transfer_throughput, avg_emit_throughput);

                num_output_bolt++;
                total_output_bolt_emit += cs.getValue().total_emit_throughput;
                output_bolts += cs.getKey() + ",";

                data = String.valueOf(unixTime) + ":" + SCHEDULER_TYPE + ":" + cs.getValue().componentId
                        + ":" + cs.getValue().parallelism_hint + ":" + topologyId + ":"
                        + cs.getValue().total_emit_throughput + "\n";
            } else {
                LOG.info("Component: {} total throughput (transfer): {} (emit): {} (execute): {} avg throughput (transfer): {} (emit): {} (execute): {}",
                        cs.getKey(), cs.getValue().total_transfer_throughput, cs.getValue().total_emit_throughput, cs.getValue().total_execute_throughput,
                        avg_transfer_throughput, avg_emit_throughput, avg_emit_throughput);

                data = String.valueOf(unixTime) + ":" + SCHEDULER_TYPE + ":" + cs.getValue().componentId
                        + ":" + cs.getValue().parallelism_hint + ":" + topologyId + ":"
                        + cs.getValue().total_transfer_throughput + "\n";
            }

            StelaHelpers.writeToFile(componentsLog, data);
        }

        if (num_output_bolt > 0) {
            LOG.info("Output Bolts stats: ");
            String data = String.valueOf(unixTime) + ':' + SCHEDULER_TYPE + ":" + output_bolts + ":" + topologyId + ":"
                    + total_output_bolt_emit / num_output_bolt + "\n";
            LOG.info(data);
            StelaHelpers.writeToFile(sinksLog, data);
        }
    }
}
