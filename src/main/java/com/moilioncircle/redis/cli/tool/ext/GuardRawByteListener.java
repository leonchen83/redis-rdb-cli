package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.glossary.Guard;
import com.moilioncircle.redis.cli.tool.io.GuardOutputStream;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.io.RawByteListener;

import java.io.OutputStream;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class GuardRawByteListener implements RawByteListener {
    private GuardOutputStream out;
    private OutputStream internal;

    public GuardRawByteListener(int cap, OutputStream internal) {
        this.internal = internal;
        this.out = new GuardOutputStream(cap, internal);
    }

    public <T extends OutputStream> T getOutputStream() {
        return (T) this.internal;
    }

    public void reset(OutputStream out) {
        this.internal = out;
        this.out.reset(out);
    }

    public void setGuard(Guard guard) {
        this.out.setGuard(guard);
    }

    @Override
    public void handle(byte... raw) {
        OutputStreams.write(raw, out);
    }
}