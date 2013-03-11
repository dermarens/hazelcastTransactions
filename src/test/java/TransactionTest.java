import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Transaction;

public class TransactionTest {

    private HazelcastInstance hazelcast;

    private static String MAP1 = "MAP1";

    private static String MAP2 = "MAP2";

    MultiMap<Integer, String> map1;

    IMap<Integer, String> map2;

    ExecutorService executorService;

    Random random;

    @Before
    public void setUp() throws Exception {
        Config config = new Config();
        config.getGroupConfig().setName("marens");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(10);
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("192.168.32.50");
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getInterfaces().setEnabled(true).addInterface("192.168.32.50");

        hazelcast = Hazelcast.newHazelcastInstance(config);

        map1 = hazelcast.getMultiMap(MAP1);
        map2 = hazelcast.getMap(MAP2);
        executorService = Executors.newFixedThreadPool(50);
        random = new Random();
    }

    @Test
    public void test() throws Exception {
        List<Callable<Integer>> callables = new ArrayList<Callable<Integer>>();

        for (int i = 0; i < 8; i++) {
            final int j = i;
            final HazelcastInstance hz = hazelcast;
            callables.add(new Callable<Integer>() {

                public Integer call() throws Exception {
                    System.out.println("Running: " + j);
                    Transaction tx = hazelcast.getTransaction();
                    System.out.println("Starting transaction: " + tx);
                    tx.begin();
                    try {
                        Thread.sleep(random.nextInt(5000));
                        map1.put(1, String.valueOf(j));
                        Thread.sleep(random.nextInt(5000));
                        map2.put(1, String.valueOf(j));
                        System.out.println("Commiting transaction: " + tx);
                        long lockedEntryCount1 = map1.getLocalMultiMapStats().getLockedEntryCount();
                        long lockWaitCount1 = map1.getLocalMultiMapStats().getLockWaitCount();
                        long lockedEntryCount2 = map2.getLocalMapStats().getLockedEntryCount();
                        long lockWaitCount2 = map2.getLocalMapStats().getLockWaitCount();
                        System.out.println("try: lockedEntryCount1,lockWaitCount1:"+lockedEntryCount1+","+lockWaitCount1 + ", Thread: " + Thread.currentThread().getId());
                        System.out.println("try: lockedEntryCount2,lockWaitCount2:"+lockedEntryCount2+","+lockWaitCount2 + ", Thread: " + Thread.currentThread().getId());
                        tx.commit();
                    } catch (Throwable t) {
                        System.out.println("Roll back transaction: " + tx);
                        tx.rollback();
                        t.printStackTrace();
                        throw new Exception(t);
                    } finally {
                        long lockedEntryCount1 = map1.getLocalMultiMapStats().getLockedEntryCount();
                        long lockWaitCount1 = map1.getLocalMultiMapStats().getLockWaitCount();
                        long lockedEntryCount2 = map2.getLocalMapStats().getLockedEntryCount();
                        long lockWaitCount2 = map2.getLocalMapStats().getLockWaitCount();
                        System.out.println("finally: lockedEntryCount1,lockWaitCount1:"+lockedEntryCount1+","+lockWaitCount1 + ", Thread: " + Thread.currentThread().getId());
                        System.out.println("finally: lockedEntryCount2,lockWaitCount2:"+lockedEntryCount2+","+lockWaitCount2 + ", Thread: " + Thread.currentThread().getId());
                    }
                    return j;
                }
            });

        }
        List<Future<Integer>> invokeAll = executorService.invokeAll(callables);
        for (Future<Integer> future : invokeAll) {
            future.get();
        }

    }
}
