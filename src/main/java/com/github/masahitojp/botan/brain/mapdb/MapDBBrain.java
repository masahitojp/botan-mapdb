package com.github.masahitojp.botan.brain.mapdb;

import com.github.masahitojp.botan.brain.BotanBrain;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;


@SuppressWarnings("unused")
public class MapDBBrain implements BotanBrain {
    private final String path;
    private final String tableName;
    private DB db;
    private ConcurrentMap<ByteArrayWrapper, byte[]> data;

    @SuppressWarnings("unused")
    public MapDBBrain() {
        this.path = Optional.ofNullable(System.getProperty("MAPDB_PATH")).orElse("./botan_map_db");
        this.tableName = Optional.ofNullable(System.getProperty("MAPDB_TABLE_NAME")).orElse("botan");
    }

    @SuppressWarnings("unused")
    public MapDBBrain(String path, String tableName) {
        this.path = path;
        this.tableName = tableName;
    }

    @Override
    public final Optional<byte[]> get(final byte[] key) {
        return Optional.ofNullable(data.get(new ByteArrayWrapper(key)));
    }

    @Override
    public final Optional<byte[]> put(byte[] key, byte[] value) {
        final Optional<byte[]> result = Optional.ofNullable(data.put(new ByteArrayWrapper(key), value));
        db.commit();
        return result;
    }

    @Override
    public final Optional<byte[]> delete(byte[] key) {
        final Optional<byte[]> result = Optional.ofNullable(data.remove(new ByteArrayWrapper(key)));
        db.commit();
        return result;
    }

    public final void deleteAll() {
        for(final ByteArrayWrapper bw:data.keySet()) {
            data.remove(bw);
        }
        db.commit();
    }


    @Override
    public Set<byte[]> keys(byte[] startsWith) {
        return this.data.keySet()
                .stream()
                .filter(key -> indexOf(key.getData(), startsWith) == 0)
                .map(ByteArrayWrapper::getData)
                .collect(Collectors.toSet());
    }

    private int indexOf(byte[] outerArray, byte[] smallerArray) {
        for(int i = 0; i < outerArray.length - smallerArray.length+1; i++) {
            boolean found = true;
            for(int j = 0; j < smallerArray.length; j++) {
                if (outerArray[i+j] != smallerArray[j]) {
                    found = false;
                    break;
                }
            }
            if (found) return i;
        }
        return -1;
    }

    @Override
    public void initialize() {
        db = DBMaker.newFileDB(new File(path))
                .transactionDisable()
                .closeOnJvmShutdown()
                .make();
        data = db.getHashMap(tableName);
    }

    @Override
    public void beforeShutdown() {
        db.close();
    }
}
