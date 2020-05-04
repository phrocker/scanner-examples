package org.poma.scanning.mr.split;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public abstract class LocationStrategy implements Funnel<RangeSplit> {
    
    /**
     * 
     */
    private static final long serialVersionUID = 5311164371626270099L;
    
    private static final Logger log = Logger.getLogger(LocationStrategy.class);
    
    protected LocationStrategy() {
        
    }
    
    public void funnel(RangeSplit rangeSplit, PrimitiveSink sink) {
        try {
            for (String location : rangeSplit.getLocations()) {
                sink.putString(location.trim().toLowerCase());
            }
        } catch (IOException e) {
            log.error(e);
        }
    }
    
}
