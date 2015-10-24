package backtype.storm.multinenantstela;

import backtype.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/* MultiTenant Stela */
public class StelaMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(StelaMonitor.class);

    @SuppressWarnings("rawtypes")
    private Map _conf;

    private StelaStatistics stelaStatistics;

    public StelaMonitor(@SuppressWarnings("rawtypes") Map _conf) {
        this._conf = _conf;
        if (this._conf != null) {
            LOG.error("****************** " + this._conf.toString());
            LOG.error("****************** " + (String)this._conf.get(Config.NIMBUS_HOST));
            LOG.error("****************** " + (Integer)this._conf.get(Config.NIMBUS_THRIFT_PORT));
        }
        stelaStatistics = new StelaStatistics("localhost", 6627);
    }

    public StelaStatistics collect() {
        stelaStatistics.collect();
        return stelaStatistics;
    }
}
