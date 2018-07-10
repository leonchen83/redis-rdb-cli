package com.moilioncircle.redis.cli.tool.ext.rct;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

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
    public Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyString(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyList(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplySet(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyZSet(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyZSet2(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyHash(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyHashZipMap(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyListZipList(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplySetIntSet(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyZSetZipList(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyHashZipList(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyListQuickList(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyModule(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyModule2(in, db, version, key, contains, type);
    }

    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        quote(key, out);
        out.write('\n');
        return super.doApplyStreamListPacks(in, db, version, key, contains, type);
    }
}