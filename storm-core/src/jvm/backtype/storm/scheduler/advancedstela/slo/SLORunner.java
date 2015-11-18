package backtype.storm.scheduler.advancedstela.slo;

public class SLORunner implements Runnable {
    private SLOObserver sloObserver;

    public SLORunner(SLOObserver observer) {
        sloObserver = observer;
    }

    @Override
    public void run() {
        sloObserver.run();
    }
}
