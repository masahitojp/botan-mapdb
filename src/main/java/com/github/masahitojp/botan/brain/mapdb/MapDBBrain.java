package com.github.masahitojp.botan.brain.mapdb;

import com.github.masahitojp.botan.brain.BotanBrain;
import com.github.masahitojp.botan.utils.BotanUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.*;

public class MapDBBrain implements BotanBrain {
    private static Logger log = LoggerFactory.getLogger(MapDBBrain.class);
    private static String KEY = "botan:brain";
    private final ConcurrentHashMap<String, String> data;
    private final DB db;
    private final ConcurrentMap<String, byte[]> inner;
    private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    @SuppressWarnings("unused")
    public MapDBBrain() throws IOException {
        this(
                BotanUtils.envToOpt("MAPDB_PATH").orElse("./botan_map_db"),
                BotanUtils.envToOpt("MAPDB_TABLE_NAME").orElse("botan")
        );
    }

    public MapDBBrain(final String mapdbPath, final String tableName) throws IOException {
        if (mapdbPath == null || mapdbPath.equals("")) throw new NullPointerException("MAPDB_PATH is not set");
        if (tableName == null || tableName.equals("")) throw new NullPointerException("MAPDB_TABLE_NAME is not set");
        final Path path = Paths.get(mapdbPath);

        Files.createDirectories(path.getParent());

        data = new ConcurrentHashMap<>();
        db = DBMaker.newFileDB(path.toFile()).closeOnJvmShutdown().make();
        inner = db.getHashMap(tableName);
    }

    @Override
    public final ConcurrentHashMap<String, String> getData() {
        return data;
    }

    @Override
    public void beforeShutdown() {
        service.shutdown();
        db.close();
    }

    @Override
    public void initialize() {

        final byte[] a = inner.getOrDefault(KEY, new byte[]{});
        if (a != null && a.length > 0) {
            deserialize(a);
        }

        service.scheduleAtFixedRate(() -> {
            final byte[] result = serialize();
            if (result != null && result.length > 0) {
                if (!Arrays.equals(inner.getOrDefault(KEY, new byte[]{}), result)) {
                    inner.put(KEY, result);
                    db.commit();
                }
            }
        }, 5, 30, TimeUnit.SECONDS);
    }

    private byte[] serialize() {
        try (
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(baos)
        ) {
            oos.writeObject(data);
            oos.flush();
            return baos.toByteArray();
        } catch (final Exception e) {
            log.warn("{}", e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void deserialize(final byte[] storedData) {
        try (
                final ByteArrayInputStream bi = new ByteArrayInputStream(storedData);
                final ObjectInputStream si = new ObjectInputStream(bi)
        ) {
            final ConcurrentHashMap<String, String> d = (ConcurrentHashMap<String, String>) si.readObject();
            d.forEach(data::put);
        } catch (Exception e) {
            log.warn("{}", e);
        }
    }

}