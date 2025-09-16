from time import sleep

from osprey.worker.lib.osprey_shared.logging import DynamicLogSampler, RollingCounter


def test_rolling_counter_clears():
    """
    A rolling counter should clear after the buffer_capacity * milliseconds_per_index has passed (⸝⸝⸝╸w╺⸝⸝⸝)
    """
    counter = RollingCounter(buffer_capacity=3, milliseconds_per_index=100)

    for _ in range(500):
        counter.increment()
    sleep(0.11)
    for _ in range(500):
        counter.increment()
    sleep(0.11)
    for _ in range(500):
        counter.increment()

    assert counter.get_count() == 1500
    sleep(0.5)
    assert counter.get_count() == 0


def test_rolling_counter_rolls():
    """
    A rolling counter should roll to the next index after each milliseconds_per_index has passed (⸝⸝⸝• ω •⸝⸝⸝)
    """
    counter = RollingCounter(buffer_capacity=3, milliseconds_per_index=100)

    for _ in range(500):
        counter.increment()
    sleep(0.11)
    for _ in range(500):
        counter.increment()
    sleep(0.11)
    for _ in range(500):
        counter.increment()

    assert counter.get_count() == 1500

    sleep(0.11)
    for _ in range(200):
        counter.increment()

    assert counter.get_count() == 1200

    sleep(0.11)
    for _ in range(200):
        counter.increment()

    assert counter.get_count() == 900

    sleep(0.11)
    for _ in range(200):
        counter.increment()

    assert counter.get_count() == 600

    sleep(0.11)

    assert counter.get_count() == 400


def test_dynamic_log_sampler_recalculates_probability():
    """
    The dynamic log sampler should recalculate the probability of logging after a configured
    number of logs have been processed ( ⸝⸝´꒳`⸝⸝)
    """
    sampler = DynamicLogSampler(target_max_logs_per_minute=2, target_max_errors_per_minute=0)

    for _ in range(sampler.num_increments_per_recalculation):
        assert sampler.probability == 1.0
        assert sampler._should_log() is True
        sampler._process_log_record()

    assert sampler.probability == max(
        0, 1 - int(sampler.num_increments_per_recalculation / sampler.target_max_logs_per_minute)
    )


def test_dynamic_log_sampler_stays_below_target_max():
    """
    The dynamic log sampler should roughly stay below the target maximum logs per minute (. ❛ ᴗ ❛.)
    """
    target_max = 1000

    def run_test(i: int) -> bool:
        sampler = DynamicLogSampler(target_max_logs_per_minute=target_max, target_max_errors_per_minute=i)

        assert sampler.probability == 1.0
        assert sampler._should_log() is True

        num_successful_logs = 0
        total_logs_processed = target_max * 50
        for _ in range(total_logs_processed):
            sampler._process_log_record()
            if sampler._should_log():
                num_successful_logs += 1

        # If we stay within 0.5% of the target maximum, we consider this a success
        if abs(target_max - num_successful_logs) / target_max < 0.05:
            return 1

        print(
            f'Failed test with {num_successful_logs} successful logs out of {total_logs_processed} processed {abs(target_max - num_successful_logs) / target_max}'  # noqa: E501
        )
        return 0

    num_test_iterations = 100
    num_test_successes = 0
    # Run this test num_test_iterations times to ensure that the sampler is not just getting lucky
    for i in range(num_test_iterations):
        num_test_successes += run_test(i)

    assert num_test_successes / num_test_iterations >= 0.95
