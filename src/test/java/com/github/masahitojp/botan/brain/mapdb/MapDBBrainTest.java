package com.github.masahitojp.botan.brain.mapdb;

import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MapDBBrainTest {

    private MapDBBrain data;

    @Before
    public void startUp() throws IOException {
        data = new MapDBBrain();
        data.initialize();
    }

    @After
    public void tearDown() {
        data.beforeShutdown();
    }

    @Test
    public void testSet() {
        val key = "test";
        val value = "test_abc";

        assertThat(data.getData().get(key), is(nullValue()));
        assertThat(data.getData().put(key, value), is(nullValue()));
        assertThat(data.getData().get("test"), is(value));
        assertThat(data.getData().remove("test"), is(value));
        assertThat(data.getData().get("test"), is(nullValue()));
    }

    @Test
    public void search() throws UnsupportedEncodingException {
        val key = "test";
        val value = "test_abc";

        data.getData().put(key, value);
        data.getData().put("test2", "test2");
        data.getData().put("key", "value");


        final List<String> list = data.getData().keySet().stream().filter(x -> x.startsWith("test")).collect(Collectors.toList());
        assertThat(list.size(), is(2));
        assertThat(list.contains(key), is(true));

    }
}