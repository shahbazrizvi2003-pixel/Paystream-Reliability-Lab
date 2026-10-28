package com.paystream.txnproducer.generator;

import bank.events.Channel;
import bank.events.Transaction;
import bank.events.TxnType;
import com.paystream.common.AccountPool;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Emits synthetic banking transactions at a configurable, constant rate
 * (FR-04: 1 to 5,000 msg/s). Messages are keyed by account_id so per-account
 * ordering is preserved - the velocity rule in the fraud detector depends on it.
 *
 * Roughly 10% of generated transactions exceed the default fraud threshold so
 * the end-to-end flow (transaction -> alert -> notification) carries traffic
 * continuously without a separate fault injector.
 */
@Component
public class SyntheticTransactionGenerator {

    public static final int MIN_RATE = 1;
    public static final int MAX_RATE = 5_000;
    /** share of transactions generated above the typical fraud threshold */
    static final double HIGH_VALUE_SHARE = 0.10;

    private static final Logger log = LoggerFactory.getLogger(SyntheticTransactionGenerator.class);

    private final KafkaTemplate<String, Transaction> template;
    private final AccountPool accounts;
    private final String topic;
    private final SplittableRandom random;
    private final Counter sentCounter;
    private final Counter failedCounter;

    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong sequence = new AtomicLong();

    private ScheduledExecutorService executor;
    private volatile boolean running;
    private volatile int ratePerSecond;
    /** remainder accumulator; only touched by the single scheduler thread */
    private double pendingMessages;

    @Autowired
    public SyntheticTransactionGenerator(KafkaTemplate<String, Transaction> template,
                                         AccountPool accounts,
                                         @Value("${paystream.topics.transactions}") String topic,
                                         MeterRegistry meters) {
        this(template, accounts, topic, meters, new SplittableRandom());
    }

    /** Visible for tests: deterministic generation via a fixed seed. */
    SyntheticTransactionGenerator(KafkaTemplate<String, Transaction> template,
                                  AccountPool accounts,
                                  String topic,
                                  MeterRegistry meters,
                                  SplittableRandom random) {
        this.template = template;
        this.accounts = accounts;
        this.topic = topic;
        this.sentCounter = meters.counter("paystream_transactions_sent_total");
        this.failedCounter = meters.counter("paystream_transactions_failed_total");
        this.random = random;
    }

    /** Starts (or restarts) generation at the requested rate. */
    public synchronized void start(int requestedRate) {
        int rate = Math.clamp((long) requestedRate, MIN_RATE, MAX_RATE);
        stop();
        pendingMessages = 0;
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "txn-generator");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(this::tick, 0, 100, TimeUnit.MILLISECONDS);
        ratePerSecond = rate;
        running = true;
        log.info("Transaction generation started: {} msg/s on topic {}", rate, topic);
    }

    public synchronized void stop() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

    public int getRatePerSecond() {
        return ratePerSecond;
    }

    public long getSentCount() {
        return sent.get();
    }

    public long getFailedCount() {
        return failed.get();
    }

    /** Sends rate/10 messages every 100 ms, accumulating the remainder. */
    private void tick() {
        try {
            pendingMessages += ratePerSecond / 10.0;
            int toSend = (int) pendingMessages;
            pendingMessages -= toSend;
            for (int i = 0; i < toSend; i++) {
                sendOne();
            }
        } catch (RuntimeException e) {
            failed.incrementAndGet();
            log.error("Generation tick failed", e);
        }
    }

    void sendOne() {
        Transaction txn = nextTransaction();
        template.send(topic, txn.getAccountId().toString(), txn)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        sent.incrementAndGet();
                        sentCounter.increment();
                    } else {
                        failed.incrementAndGet();
                        failedCounter.increment();
                        log.warn("Produce failed: {}", ex.getMessage());
                    }
                });
    }

    Transaction nextTransaction() {
        AccountPool.Account account = accounts.get(random.nextInt(accounts.size()));
        boolean highValue = random.nextDouble() < HIGH_VALUE_SHARE;
        BigDecimal amount = highValue
                ? BigDecimal.valueOf(random.nextLong(1_000_000, 5_000_001), 2)
                : BigDecimal.valueOf(random.nextLong(500, 200_001), 2);
        return Transaction.newBuilder()
                .setTxnId(UUID.randomUUID())
                .setAccountId(account.accountId())
                .setAmount(amount.setScale(2, RoundingMode.HALF_UP))
                .setCurrency("USD")
                .setType(random.nextInt(10) < 7 ? TxnType.DEBIT
                        : (random.nextInt(10) < 5 ? TxnType.CREDIT : TxnType.TRANSFER))
                .setChannel(switch (random.nextInt(4)) {
                    case 0 -> Channel.ATM;
                    case 1 -> Channel.POS;
                    case 2 -> Channel.ONLINE;
                    default -> Channel.BRANCH;
                })
                .setMerchantId(random.nextBoolean() ? "MERCH-" + random.nextInt(1_000) : null)
                .setEventTime(Instant.now())
                .setMetadata(Map.of("source", "synthetic",
                        "sequence", Long.toString(sequence.incrementAndGet())))
                .build();
    }
}
