package com.moilioncircle.redis.cli.tool.ext.rct;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.DumpRawByteListener;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;

/**
 * @author Baoyi Chen
 */
public class KeyValRdbVisitor extends AbstractRdbVisitor {
    public KeyValRdbVisitor(Replicator replicator,
                            Configure configure,
                            File out,
                            List<Long> db,
                            List<String> regexs,
                            List<DataType> types,
                            Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
    }
    
    private void emit(byte[] str) throws IOException {
        escape.encode(str, out);
    }
    
    @Override
    public Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        escape.encode(val, out);
        out.write('\n');
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            escape.encode(element, out);
            if (len - 1 > 0) out.write(' ');
            else out.write('\n');
            len--;
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            escape.encode(element, out);
            if (len - 1 > 0) out.write(' ');
            else out.write('\n');
            len--;
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            escape.encode(element, out);
            out.write(' ');
            escape.encode(score, out);
            if (len - 1 > 0) out.write(' ');
            else out.write('\n');
            len--;
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            escape.encode(element, out);
            out.write(' ');
            escape.encode(score, out);
            if (len - 1 > 0) out.write(' ');
            else out.write('\n');
            len--;
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            escape.encode(field, out);
            out.write(' ');
            escape.encode(value, out);
            if (len - 1 > 0) out.write(' ');
            else out.write('\n');
            len--;
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        boolean flag = true;
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                out.write('\n');
                return new DummyKeyValuePair();
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                if (!flag) out.write(' ');
                escape.encode(field, out);
                out.write(' ');
                escape.encode(null, out);
                out.write('\n');
                return new DummyKeyValuePair();
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            if (flag) flag = false;
            else out.write(' ');
            escape.encode(field, out);
            out.write(' ');
            escape.encode(value, out);
        }
    }
    
    @Override
    public Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        for (int i = 0; i < zllen; i++) {
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            escape.encode(e, out);
            if (i + 1 < zllen) out.write(' ');
            else out.write('\n');
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        
        int encoding = BaseRdbParser.LenHelper.encoding(stream);
        long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
        for (long i = 0; i < lenOfContent; i++) {
            String element;
            switch (encoding) {
                case 2:
                    element = String.valueOf(stream.readInt(2));
                    break;
                case 4:
                    element = String.valueOf(stream.readInt(4));
                    break;
                case 8:
                    element = String.valueOf(stream.readLong(8));
                    break;
                default:
                    throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
            }
            escape.encode(element.getBytes(), out);
            if (i + 1 < lenOfContent) out.write(' ');
            else out.write('\n');
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
            zllen--;
            escape.encode(element, out);
            out.write(' ');
            escape.encode(score, out);
            if (zllen - 1 > 0) out.write(' ');
            else out.write('\n');
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            escape.encode(field, out);
            out.write(' ');
            escape.encode(value, out);
            if (zllen - 1 > 0) out.write(' ');
            else out.write('\n');
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
    
            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                escape.encode(e, out);
                if (i + 1 < len) out.write(' ');
                else if (j + 1 < zllen) out.write(' ');
                else out.write('\n');
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule(in, db, version, key, contains, type);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }
    
    @Override
    public Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule2(in, db, version, key, contains, type);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }
    
    protected ModuleParser<? extends Module> lookupModuleParser(String moduleName, int moduleVersion) {
        return replicator.getModuleParser(moduleName, moduleVersion);
    }
    
    @Override
    @SuppressWarnings("resource")
    public Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        emit(key);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            super.doApplyStreamListPacks(in, db, version, key, contains, type);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }
}