package com.moilioncircle.redis.cli.tool.ext.rct;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class KeyRdbVisitor extends AbstractRdbVisitor {

    public KeyRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
    }

    @Override
    public Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyString(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyList(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplySet(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyZSet(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyZSet2(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyHash(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyHashZipMap(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyListZipList(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplySetIntSet(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyZSetZipList(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyHashZipList(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyListQuickList(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyModule(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyModule2(in, version, key, contains, type, context);
    }

    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyStreamListPacks(in, version, key, contains, type, context);
    }
}