import os
import sqlite3

class SQLInterface:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("create table if not exists jobits(job_id integer, dataset, input_file, status, num_attempts, host, exit_code, run_time, startup_time)")
        self.db.execute("create index if not exists file_index on jobits(dataset, input_file asc)")
        self.db.commit()
        self.job_id_counter = 0

        try:
            cur = self.db.execute("select max(job_id) from jobits")
            count = int(cur.fetchone()[0])
            if count:
                print "Restarting with job counter", count
                self.job_id_counter = count
        except:
            pass

    def disconnect(self):
        self.db.close()

    def register_jobits(self):
        dataset_lists = [x['dataset list'] for x in self.config['tasks']]
        datasets = [x['dataset'] for x in self.config['tasks']]
        for list, dataset in zip(dataset_lists, datasets):
            if os.path.isdir(list):
                files = ['file:'+f.strip() for f in os.popen('ls -d %s/*' % list).readlines()]
            elif os.path.isfile(list):
                files = ['file:'+f.strip() for f in open(list).readlines()]
            columns = [(dataset, file) for file in files]
            self.db.executemany("insert into jobits(dataset, input_file, status, num_attempts) values (?, ?, 'i', 0)", columns)

        self.db.commit()

    def pop_jobits(self, size=1):
        self.job_id_counter += 1
        id = str(self.job_id_counter)

        input_files = []
        update = []

        last_dataset = None

        for dataset, input_file in self.db.execute("""
                select dataset, input_file
                from jobits
                where (status='i' or status='f')
                order by dataset, input_file
                limit ?""", (size,)):
            if last_dataset is None:
                last_dataset = dataset
            elif last_dataset != dataset:
                break

            input_files.append(input_file)
            update.append((int(id), dataset, input_file))

        self.db.executemany("update jobits set status='r', job_id=? where dataset=? and input_file=?", update)
        self.db.commit()
        return [id, dataset, set(input_files), -1]

    def reset_jobits(self):
        with self.db as db:
            db.execute("update jobits set status='f' where status='r'")

    def update_jobits(self, id, failed=False):
        with self.db as db:
            db.execute("update jobits set status=? where job_id=?",
                    ('f' if failed else 's', int(id)))

    def unfinished_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status!='s'")
        return cur.fetchone()[0]

    def running_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status=='r'")
        return cur.fetchone()[0]
