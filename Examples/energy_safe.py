import time

def run_loop(duration_sec=1.0):
    """
    duration_sec - сколько секунд длится каждый замер
    sleep_values - задержки между итерациями
    """
    sleep_values = [0, 0.000001, 0.0001, 0.001, 0.01, 0.05, 0.1, 0.5]
    results = []

    for sleep_delay in sleep_values:
        start_time  = time.time()
        iterations  = 0            # = сколько раз отработал while
        # Эмуляция. Каждая итерация ≈ один вызов data.next()

        while time.time() - start_time < duration_sec:
            iterations += 1
            if sleep_delay:
                time.sleep(sleep_delay)

        results.append((sleep_delay, iterations))

    # вывод результатов
    print(f"{'sleep (сек)':<12} {'итераций/сек':<12}")
    print("-" * 28)
    for sleep, loops in results:
        print(f"{sleep:<12} {loops:<12}")

if __name__ == "__main__":
    run_loop()

