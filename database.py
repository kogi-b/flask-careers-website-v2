import os
from sqlalchemy import create_engine, text

db_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_string, connect_args={
  "ssl": {
    "ssl_ca": "/etc/ssl/cert.pem"
  }
})


def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("select * from jobs"))
    jobs = []
    column_names = result.keys()
    for row in result.all():
      jobs.append(dict(zip(column_names, row)))
    return jobs

def load_job_from_db(id):
  with engine.connect() as conn:
    result = conn.execute(
      text("select * from jobs where id = :val"), {'val':id})
    rows = result.all()
    column_names = result.keys()
    if len(rows) == 0:
      return None
    else:
      # print(column_names)
      # print(rows)
      jobs = None
      column_names = result.keys()
      for row in rows:
        jobs=dict(zip(column_names, row))
        # jobs.append(dict(zip(column_names, row)))
      return(jobs)
      

load_job_from_db(3)
# print(load_jobs_from_db())