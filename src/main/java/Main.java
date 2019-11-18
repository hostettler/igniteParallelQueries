import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

public class Main {

    public static void main(String[] args) {
        IgniteConfiguration c = new IgniteConfiguration();
        try (Ignite ignite = Ignition.start(c)) {

            QueryEntity foo = new QueryEntity(String.class, Foo.class);
            foo.addQueryField("s", String.class.getName(), null);
            foo.addQueryField("i", int.class.getName(), null);
            QueryEntity bar = new QueryEntity(AffinityKey.class, Bar.class);
            bar.addQueryField("fooRef", String.class.getName(), null);
            bar.addQueryField("key", String.class.getName(), null);
            bar.addQueryField("s", String.class.getName(), null);
            bar.addQueryField("i", int.class.getName(), null);
            bar.setKeyFields(new TreeSet<>(Arrays.asList("key", "fooRef")));

            CacheConfiguration<Object, Object> regular = new CacheConfiguration<Object, Object>();
            regular.setCacheMode(PARTITIONED);
            regular.setName("regularCache");
            regular.setQueryEntities(Arrays.asList(foo, bar));
            IgniteCache<Object, Object> regularCache = ignite.createCache(regular);
 
            CacheConfiguration<Object, Object> parallel = new CacheConfiguration<Object, Object>();
            parallel.setCacheMode(PARTITIONED);
            parallel.setName("parallelCache");
            parallel.setQueryEntities(Arrays.asList(foo, bar));
            //This is the only difference between the 2 caches
            parallel.setQueryParallelism(16);
            IgniteCache<Object, Object> parallelCache = ignite.createCache(parallel);

            for (int i = 0; i < 100; i++) {
                String key = String.valueOf(i);
                
                regularCache.put("foo" + key, new Foo("foo" + key, i));
                regularCache.put(new AffinityKey("bar" + key, "foo" + (i % 10)), new Bar("bar" + key, i));
                
                parallelCache.put("foo" + key, new Foo("foo" + key, i));
                parallelCache.put(new AffinityKey("bar" + key, "foo" + (i % 10)), new Bar("bar" + key, i));
            }

            SqlFieldsQuery query = new SqlFieldsQuery("select f.s, sum(b.i) from Bar b join Foo f on b.fooRef = f.s group by f.s");
            FieldsQueryCursor<List<?>> cursor;

            cursor = regularCache.query(query);
            System.out.println("Result for regular cache : " + cursor.getAll());
            //Because of parallel.setQueryParallelism(16) the result are wrong.
            cursor = parallelCache.query(query);
            System.out.println("Result for cache with parallel.setQueryParallelism(16): "  + cursor.getAll());
        }
    }

    public static final class Foo {

        String s;
        int i;

        public Foo() {
        }

        public Foo(String s, int i) {
            this.s = s;
            this.i = i;
        }

    }

    public static final class Bar {

        String s;
        int i;

        public Bar() {

        }

        public Bar(String s, int i) {
            this.s = s;
            this.i = i;
        }

    }

    static class AffinityKey {
        /** Key. */
        String key;

        /** Affinity key. */
        @AffinityKeyMapped
        String fooRef;

        /**
         * @param key Key.
         * @param fooRef Affinity key.
         */
        public AffinityKey(String key, String fooRef) {
            this.key = key;
            this.fooRef = fooRef;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AffinityKey key1 = (AffinityKey)o;
            return Objects.equals(key, key1.key) &&
                Objects.equals(fooRef, key1.fooRef);
        }

        @Override public int hashCode() {
            return Objects.hash(key, fooRef);
        }
    }
}
