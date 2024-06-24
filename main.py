import concurrent.futures
import psycopg2


MAX_WORKERS = 50
PAGE = 500000


def get_db_connection():
    conn = psycopg2.connect("dbname=myapp")
    return conn


def create_table():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS mydata")
            cur.execute("CREATE TABLE IF NOT EXISTS mydata (id UUID NOT NULL DEFAULT gen_random_uuid(), data BIGINT NOT NULL)")


def insert_data(seed: int):
    start = PAGE * seed + 1
    stop = PAGE * (seed + 1)
    print(f"({start}, {stop})")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO mydata (data) SELECT generate_series(%s, %s)", (start, stop))
    conn.close()


def insert_data2(seed: int, conn):
    start = PAGE * seed + 1
    stop = PAGE * (seed + 1)
    print(f"({start}, {stop})")
    with conn.cursor() as cur:
            cur.execute("INSERT INTO mydata (data) SELECT generate_series(%s, %s)", (start, stop))


def sequential_write():
    for seed in range(0, 100):
        insert_data(seed)


def threading_write():
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(insert_data, seed): seed for seed in range(0, 100)}


def threading_write2():
    with get_db_connection() as conn:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(insert_data2, seed, conn): seed for seed in range(0, 100)}
    conn.close()


def multiprocessing_write():
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(insert_data, seed): seed for seed in range(0, 100)}


def main():
    create_table()
    multiprocessing_write()


if __name__ == "__main__":
    main()

# s = 4m15.957s // 4m5.596s
# t = 3m45.115s // 3m42.409s
# m = 3m51.740s // 3m52.648s