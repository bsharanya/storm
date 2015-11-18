package backtype.storm.scheduler.advancedstela.slo;

public class TopologyPair {
    private StelaTopology giver;
    private StelaTopology receiver;

    public StelaTopology getGiver() {
        return giver;
    }

    public void setGiver(StelaTopology giver) {
        this.giver = giver;
    }

    public StelaTopology getReceiver() {
        return receiver;
    }

    public void setReceiver(StelaTopology receiver) {
        this.receiver = receiver;
    }
}
