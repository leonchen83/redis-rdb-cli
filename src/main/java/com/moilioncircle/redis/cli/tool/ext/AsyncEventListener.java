package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.util.Concurrents;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Baoyi Chen
 */
public class AsyncEventListener implements EventListener {
    
    private final EventListener listener;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    
    public AsyncEventListener(EventListener listener, Replicator r, Configure c) {
        this.listener = listener;
        r.addCloseListener(rep -> {
            Concurrents.terminateQuietly(executor, c.getTimeout(), TimeUnit.MILLISECONDS);
        });
    }
    
    @Override
    public void onEvent(Replicator replicator, Event event) {
        executor.submit(() -> listener.onEvent(replicator, event));
    }
}
