import java.util.Arrays;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteProperties;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheMode.LOCAL;

public class Main {

    public static void main(String[] args) {
        IgniteConfiguration c = new IgniteConfiguration();
        try (Ignite ignite = Ignition.start(c)) {

            QueryEntity foo = new QueryEntity(String.class, Foo.class);
            foo.addQueryField("s", String.class.getName(), null);
            foo.addQueryField("i", int.class.getName(), null);
            QueryEntity bar = new QueryEntity(String.class, Bar.class);
            bar.addQueryField("s", String.class.getName(), null);
            bar.addQueryField("i", int.class.getName(), null);
            bar.addQueryField("fooRef", String.class.getName(), null);

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
                regularCache.put("bar" + key, new Bar("bar" + key, i, "foo" + (i % 10)));
                
                parallelCache.put("foo" + key, new Foo("foo" + key, i));
                parallelCache.put("bar" + key, new Bar("bar" + key, i, "foo" + (i % 10)));
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

        String fooRef;

        public Bar() {

        }

        public Bar(String s, int i, String fooRef) {
            this.s = s;
            this.i = i;
            this.fooRef = fooRef;
        }

    }
}
